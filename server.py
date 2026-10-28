"""Rahul TradeX - standalone live chart website (FastAPI + Yahoo Finance)."""
import hashlib
import hmac
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

HERE = Path(__file__).parent
IST = ZoneInfo("Asia/Kolkata")
IST_OFFSET = 19800

WATCHLIST = [
    "NIFTY 50", "RELIANCE", "TCS", "INFY", "HDFCBANK", "SBIN", "ICICIBANK", "LTIMINDTREE",
    "ITC", "AXISBANK", "KOTAKBANK", "BHARTIARTL", "ASIANPAINT", "MARUTI", "SUNPHARMA",
    "INDUSINDBK", "WIPRO", "TITAN", "POWERGRID", "NTPC", "BAJFINANCE", "TECHM",
    "ULTRACEMCO", "JSWSTEEL", "HINDUNILVR", "ONGC", "GRASIM", "HEROMOTOCO",
    "SHRIRAMFIN", "BAJAJ-AUTO",
]
# label -> (yahoo interval, yahoo period, intraday)
TIMEFRAMES = {
    "1m": ("1m", "5d", True), "5m": ("5m", "1mo", True), "15m": ("15m", "1mo", True),
    "1H": ("60m", "3mo", True), "1D": ("1d", "2y", False), "1W": ("1wk", "5y", False),
}
ALIASES = {
    "NIFTY 50": "^NSEI", "NIFTY50": "^NSEI", "NIFTY": "^NSEI",
    "NIFTY BANK": "^NSEBANK", "BANKNIFTY": "^NSEBANK", "LTIMINDTREE": "LTIM.NS",
}
SYMBOL_RE = re.compile(r"^[A-Z0-9&\-\.\^ ]{1,20}$")

app = FastAPI(title="Rahul TradeX", docs_url=None, redoc_url=None)
_cache, _lock = {}, threading.Lock()


def cached(key, ttl, fn):
    """All visitors share one Yahoo call per ttl seconds. Failures are not cached."""
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    value = fn()
    if value:
        with _lock:
            if len(_cache) > 500:
                _cache.clear()
            _cache[key] = (now, value)
    return value


def clean(raw):
    sym = (raw or "").strip().upper()
    if sym.startswith("NSE:"):
        sym = sym[4:]
    if not SYMBOL_RE.match(sym):
        raise HTTPException(400, "Invalid symbol")
    return sym


def yahoo_symbol(sym):
    if sym in ALIASES:
        return ALIASES[sym]
    if sym.startswith("^") or sym.endswith(".NS"):
        return sym
    return sym.replace(" ", "") + ".NS"


def positive(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if x != x or x <= 0 else x


def is_market_open():
    now = datetime.now(IST)
    if now.weekday() >= 5:
        return False
    return (9, 15) <= (now.hour, now.minute) <= (15, 30)


def get_candles(sym, interval, period):
    try:
        df = yf.Ticker(yahoo_symbol(sym)).history(period=period, interval=interval, auto_adjust=False)
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        if df.empty:
            return None
        idx = df.index.tz_localize("UTC") if df.index.tz is None else df.index
        out = []
        for ts, r in zip(idx, df.itertuples()):
            out.append({
                "time": int(ts.timestamp()) + IST_OFFSET,
                "open": float(r.Open), "high": float(r.High), "low": float(r.Low),
                "close": float(r.Close), "volume": int(r.Volume) if r.Volume == r.Volume else 0,
            })
        return out
    except Exception:
        return None


def get_quote(sym):
    try:
        t = yf.Ticker(yahoo_symbol(sym))
        price = prev = None
        try:
            fi = t.fast_info
            price = positive(fi["last_price"])
            prev = positive(fi["previous_close"])
        except Exception:
            pass
        if price is None or prev is None:
            daily = t.history(period="5d", interval="1d").dropna(subset=["Close"])
            if price is None and not daily.empty:
                price = positive(daily["Close"].iloc[-1])
            if prev is None and len(daily) > 1:
                prev = positive(daily["Close"].iloc[-2])
        return {"price": price, "prev": prev} if price else None
    except Exception:
        return None


def get_batch():
    try:
        names = {s: yahoo_symbol(s) for s in WATCHLIST}
        df = yf.download(sorted(set(names.values())), period="5d", interval="1d",
                         auto_adjust=False, progress=False, threads=True, group_by="ticker")
        out = {}
        for s, y in names.items():
            try:
                c = df[y]["Close"].dropna()
                out[s] = {"last": float(c.iloc[-1]), "prev": float(c.iloc[-2]) if len(c) > 1 else None}
            except Exception:
                continue
        return out
    except Exception:
        return None


# ---------------- Private access + Zerodha Kite Connect (optional) ----------------
# Set these in Render > Environment (never in GitHub):
#   APP_PASSWORD (makes the site private), COOKIE_SECRET (long random text),
#   KITE_API_KEY, KITE_API_SECRET (from your Kite Connect app)
APP_PASSWORD = os.getenv("APP_PASSWORD", "")
COOKIE_SECRET = os.getenv("COOKIE_SECRET") or APP_PASSWORD
KITE_KEY = os.getenv("KITE_API_KEY", "").strip()
KITE_SECRET = os.getenv("KITE_API_SECRET", "").strip()
PRIVATE = bool(APP_PASSWORD)
KITE_ON = PRIVATE and bool(KITE_KEY and KITE_SECRET)  # live broker data only on a private site
KITE = {"token": None, "at": 0.0}
TOK = {}
_fails = {}
KITE_TF = {"1m": ("minute", 5), "5m": ("5minute", 30), "15m": ("15minute", 30),
           "1H": ("60minute", 90), "1D": ("day", 730), "1W": ("day", 1500)}
KITE_NAMES = {"NIFTY 50": "NSE:NIFTY 50", "NIFTY50": "NSE:NIFTY 50", "NIFTY": "NSE:NIFTY 50",
              "NIFTY BANK": "NSE:NIFTY BANK", "BANKNIFTY": "NSE:NIFTY BANK", "LTIMINDTREE": "NSE:LTIM"}


def cookie_value():
    return hmac.new(COOKIE_SECRET.encode(), b"tradex-ok", hashlib.sha256).hexdigest()


LOGIN_PAGE = """<!doctype html><meta name=viewport content="width=device-width,initial-scale=1"><title>Login</title>
<body style="background:#131722;color:#d1d4dc;font-family:system-ui;display:grid;place-items:center;height:100vh;margin:0">
<div style="width:280px"><h2>Rahul TradeX</h2><input id=p type=password placeholder=Password
style="width:100%;padding:10px;margin:8px 0;background:#1e222d;color:#d1d4dc;border:1px solid #2a2e39;border-radius:6px;box-sizing:border-box">
<button onclick=go() style="width:100%;padding:10px;background:#2962ff;color:#fff;border:0;border-radius:6px">Login</button>
<p id=m style="color:#ef5350"></p></div>
<script>async function go(){const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},
body:JSON.stringify({password:document.getElementById('p').value})});
if(r.ok)location='/';else document.getElementById('m').textContent=(await r.json()).detail||'Failed'}
document.getElementById('p').onkeydown=e=>{if(e.key==='Enter')go()}</script>"""


@app.middleware("http")
async def gate(request: Request, call_next):
    path = request.url.path
    if not PRIVATE or path in ("/login", "/api/health"):
        return await call_next(request)
    if hmac.compare_digest(request.cookies.get("tx", ""), cookie_value()):
        return await call_next(request)
    if path.startswith("/api/"):
        return JSONResponse({"detail": "Login required"}, status_code=401)
    return RedirectResponse("/login")


@app.get("/login")
def login_page():
    return HTMLResponse(LOGIN_PAGE)


@app.post("/login")
async def login(request: Request):
    ip = request.client.host if request.client else "x"
    n, until = _fails.get(ip, (0, 0))
    if time.time() < until:
        raise HTTPException(429, "Too many tries. Wait 15 minutes.")
    try:
        pw = str((await request.json()).get("password", ""))
    except Exception:
        pw = ""
    if not APP_PASSWORD or not hmac.compare_digest(pw.encode(), APP_PASSWORD.encode()):
        n += 1
        _fails[ip] = (n, time.time() + 900 if n >= 5 else 0)
        raise HTTPException(401, "Wrong password")
    _fails.pop(ip, None)
    resp = JSONResponse({"ok": True})
    resp.set_cookie("tx", cookie_value(), httponly=True, secure=True, samesite="lax", max_age=2592000)
    return resp


def kite_ok():
    return KITE_ON and KITE["token"] is not None and time.time() - KITE["at"] < 20 * 3600


def kite_get(path, params=None):
    if not kite_ok():
        return None
    try:
        r = requests.get("https://api.kite.trade" + path, params=params, timeout=8,
                         headers={"X-Kite-Version": "3", "Authorization": f"token {KITE_KEY}:{KITE['token']}"})
        if r.status_code == 403:
            KITE["token"] = None  # token expired: log in again
            return None
        return r.json().get("data") if r.status_code == 200 else None
    except Exception:
        return None


@app.get("/zerodha/login")
def zerodha_login():
    if not KITE_ON:
        raise HTTPException(400, "Zerodha is not configured (need APP_PASSWORD, KITE_API_KEY, KITE_API_SECRET)")
    return RedirectResponse(f"https://kite.zerodha.com/connect/login?v=3&api_key={KITE_KEY}")


@app.get("/zerodha/callback")
def zerodha_callback(request_token: str = "", status: str = ""):
    if not KITE_ON or status != "success" or not request_token:
        raise HTTPException(400, "Zerodha login was not completed")
    checksum = hashlib.sha256((KITE_KEY + request_token + KITE_SECRET).encode()).hexdigest()
    try:
        r = requests.post("https://api.kite.trade/session/token", timeout=10, headers={"X-Kite-Version": "3"},
                          data={"api_key": KITE_KEY, "request_token": request_token, "checksum": checksum})
        tok = r.json()["data"]["access_token"]
    except Exception:
        raise HTTPException(502, "Zerodha did not accept the login. Check API key and secret.")
    KITE["token"], KITE["at"] = tok, time.time()
    return RedirectResponse("/")


def kite_symbol(sym):
    if sym.startswith("^"):
        return None
    return KITE_NAMES.get(sym) or "NSE:" + sym.replace(".NS", "")


def kite_quote(keys):
    data = kite_get("/quote", [("i", k) for k in keys])
    if data:
        for k, d in data.items():
            if d.get("instrument_token"):
                TOK[k] = d["instrument_token"]
    return data


def kite_candles(sym, tf, days=None):
    ks = kite_symbol(sym)
    if not ks:
        return None
    if ks not in TOK:
        kite_quote([ks])
    inst = TOK.get(ks)
    if not inst:
        return None
    interval, d0 = KITE_TF[tf]
    days = days or d0
    to = datetime.now(IST)
    fmt = "%Y-%m-%d %H:%M:%S"
    data = kite_get(f"/instruments/historical/{inst}/{interval}",
                    {"from": (to - timedelta(days=days)).strftime(fmt), "to": to.strftime(fmt)})
    if not data or not data.get("candles"):
        return None
    rows = [{"time": int(datetime.strptime(c[0], "%Y-%m-%dT%H:%M:%S%z").timestamp()) + IST_OFFSET,
             "open": float(c[1]), "high": float(c[2]), "low": float(c[3]), "close": float(c[4]),
             "volume": int(c[5])} for c in data["candles"]]
    if tf == "1W":
        df = pd.DataFrame(rows)
        g = df.groupby(pd.to_datetime(df["time"], unit="s").dt.to_period("W-FRI"))
        rows = [{"time": int(x.time.iloc[0]), "open": float(x.open.iloc[0]), "high": float(x.high.max()),
                 "low": float(x.low.min()), "close": float(x.close.iloc[-1]), "volume": int(x.volume.sum())}
                for _, x in g]
    return rows


def candles_src(sym, tf):
    if kite_ok():
        rows = kite_candles(sym, tf)
        if rows:
            return rows, "zerodha"
    interval, period, _ = TIMEFRAMES[tf]
    rows = get_candles(sym, interval, period)
    return (rows, "yahoo") if rows else None


def quote_src(sym):
    if kite_ok():
        ks = kite_symbol(sym)
        d = (kite_quote([ks]) or {}).get(ks) if ks else None
        price = positive((d or {}).get("last_price"))
        if price:
            return {"price": price, "prev": positive((d.get("ohlc") or {}).get("close")), "source": "zerodha"}
    q = get_quote(sym)
    return {**q, "source": "yahoo"} if q else None


def batch_src():
    if kite_ok():
        keys = {s: kite_symbol(s) for s in WATCHLIST if kite_symbol(s)}
        data = kite_quote(list(keys.values()))
        if data:
            out = {}
            for s, k in keys.items():
                d = data.get(k)
                if d and positive(d.get("last_price")):
                    out[s] = {"last": float(d["last_price"]), "prev": positive((d.get("ohlc") or {}).get("close"))}
            if out:
                return out
    return get_batch()


# ---------------- Analysis: rule-based decision + news ----------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def decide(rows, capital, risk_pct):
    """Deterministic rules only. Returns a decision with plain-language reasons."""
    if not rows or len(rows) < 60:
        return {"decision": "NO DECISION", "score": 0, "confidence": 0,
                "reasons": ["Not enough recent price data to judge safely."]}
    df = pd.DataFrame(rows)
    c, h, l, v = df["close"], df["high"], df["low"], df["volume"]
    last = float(c.iloc[-1])
    e20, e50 = ema(c, 20), ema(c, 50)
    e200 = ema(c, 200) if len(c) >= 200 else None
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / 14, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / 14, adjust=False).mean()
    rsi = float(100 - 100 / (1 + up.iloc[-1] / dn.iloc[-1])) if dn.iloc[-1] else 100.0
    macd = ema(c, 12) - ema(c, 26)
    hist = macd - ema(macd, 9)
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = float(tr.ewm(alpha=1 / 14, adjust=False).mean().iloc[-1])
    hi20, lo20 = float(h.iloc[-21:-1].max()), float(l.iloc[-21:-1].min())
    vol_ratio = float(v.iloc[-1] / v.iloc[-21:-1].mean()) if v.iloc[-21:-1].mean() else 1.0

    score, why = 0, []
    if e200 is not None and last > e50.iloc[-1] > e200.iloc[-1]:
        score += 2; why.append("Strong uptrend: price is above its 50-day and 200-day averages.")
    elif e200 is not None and last < e50.iloc[-1] < e200.iloc[-1]:
        score -= 2; why.append("Strong downtrend: price is below its 50-day and 200-day averages.")
    elif last > e50.iloc[-1]:
        score += 1; why.append("Price is above its 50-day average (mild uptrend).")
    else:
        score -= 1; why.append("Price is below its 50-day average (mild downtrend).")
    if rsi > 70:
        score -= 1; why.append(f"RSI {rsi:.0f}: overbought, a pullback is more likely.")
    elif rsi < 30:
        score += 1; why.append(f"RSI {rsi:.0f}: oversold, a bounce is possible.")
    elif rsi >= 55:
        score += 1; why.append(f"RSI {rsi:.0f}: momentum is positive.")
    elif rsi <= 45:
        score -= 1; why.append(f"RSI {rsi:.0f}: momentum is weak.")
    else:
        why.append(f"RSI {rsi:.0f}: neutral.")
    if hist.iloc[-1] > 0 and hist.iloc[-1] > hist.iloc[-2]:
        score += 1; why.append("MACD is positive and rising.")
    elif hist.iloc[-1] < 0 and hist.iloc[-1] < hist.iloc[-2]:
        score -= 1; why.append("MACD is negative and falling.")
    if last > hi20 and vol_ratio > 1.2:
        score += 1; why.append("Breakout above the 20-day high on higher volume.")
    elif last < lo20 and vol_ratio > 1.2:
        score -= 1; why.append("Breakdown below the 20-day low on higher volume.")
    if atr / last > 0.05:
        why.append("Caution: this stock is very volatile right now. Use smaller size.")

    decision = "BUY" if score >= 3 else "SELL / AVOID" if score <= -3 else "WAIT (do nothing)"
    out = {"decision": decision, "score": score, "confidence": min(100, round(abs(score) / 5 * 100)),
           "reasons": why, "price": last, "rsi": round(rsi, 1), "atr": round(atr, 2)}
    if decision == "BUY" or decision == "SELL / AVOID":
        sign = 1 if decision == "BUY" else -1
        stop, target = last - sign * 2 * atr, last + sign * 3 * atr
        risk_amt = capital * risk_pct / 100
        qty = math.floor(risk_amt / (2 * atr)) if atr > 0 else 0
        out.update({"stop_loss": round(stop, 2), "target": round(target, 2),
                    "shares_for_your_risk": qty, "risk_amount": round(risk_amt, 2),
                    "note": "Levels are for a BUY idea." if sign == 1 else
                            "If you hold it, consider reducing. Mirror levels shown for short-side ideas only."})
    return out


def get_news(sym):
    try:
        items = yf.Ticker(yahoo_symbol(sym)).news or []
        out = []
        for it in items[:8]:
            c = it.get("content") or it
            url = ((c.get("canonicalUrl") or {}).get("url")) or c.get("link")
            title = c.get("title")
            if not title or not url:
                continue
            src = (c.get("provider") or {}).get("displayName") or c.get("publisher") or ""
            out.append({"title": title, "source": src, "url": url,
                        "time": c.get("pubDate") or c.get("providerPublishTime")})
        return out or None
    except Exception:
        return None


@app.get("/api/analysis")
def analysis(symbol: str, capital: float = 100000, risk: float = 1.0):
    sym = clean(symbol)
    capital = min(max(capital, 1000), 1e9)
    risk = min(max(risk, 0.1), 5)
    got = cached(("c", sym, "1D"), 60, lambda: candles_src(sym, "1D"))
    if not got:
        raise HTTPException(503, "No live data for this symbol right now")
    res = decide(got[0], capital, risk)
    res["symbol"] = sym
    res["news"] = cached(("n", sym), 300, lambda: get_news(sym)) or []
    res["disclaimer"] = "Educational decision support only. Not investment advice. Markets are risky; you can lose money."
    return res


# ---------------- Scanner + backtest ----------------
COST = 0.001  # assumed 0.1% per side for fees + slippage
ORDER = {"BUY": 0, "SELL / AVOID": 1, "WAIT (do nothing)": 2, "NO DECISION": 3, "NO DATA": 4}


def scan_one(s):
    try:
        got = cached(("c", s, "1D"), 60, lambda: candles_src(s, "1D"))
        if not got:
            return {"symbol": s, "decision": "NO DATA", "confidence": 0}
        d = decide(got[0], 100000, 1)
        return {"symbol": s, "decision": d["decision"], "confidence": d["confidence"], "price": d.get("price")}
    except Exception:
        return {"symbol": s, "decision": "NO DATA", "confidence": 0}


@app.get("/api/scan")
def scan():
    def run():
        with ThreadPoolExecutor(max_workers=4) as ex:
            out = list(ex.map(scan_one, WATCHLIST))
        return out if any(o["decision"] != "NO DATA" for o in out) else None
    out = cached(("scan",), 600, run)
    if not out:
        raise HTTPException(503, "No data right now. Try again in a minute.")
    return sorted(out, key=lambda o: (ORDER.get(o["decision"], 9), -o["confidence"]))


def backtest_stats(rows):
    n = len(rows)
    if n < 260:
        return {"error": "Not enough history to test this stock."}
    trades, i = [], 200
    while i < n - 1:
        sig = decide(rows[:i + 1], 100000, 1)
        if sig["decision"] != "BUY":
            i += 1
            continue
        stop, target = sig["stop_loss"], sig["target"]
        entry = rows[i + 1]["open"] * (1 + COST)
        for j in range(i + 1, min(i + 21, n)):  # max 20 trading days
            r = rows[j]
            if r["low"] <= stop:
                px, why = min(stop, r["open"]), "stop"
                break
            if r["high"] >= target:
                px, why = max(target, r["open"]), "target"
                break
        else:
            px, why = rows[j]["close"], "time"
        ret = px * (1 - COST) / entry - 1
        trades.append({"date": datetime.fromtimestamp(rows[i + 1]["time"], timezone.utc).strftime("%d %b %Y"),
                       "ret": round(ret * 100, 2), "exit": why})
        i = j
    eq, peak, dd = 1.0, 1.0, 0.0
    for t in trades:
        eq *= 1 + t["ret"] / 100
        peak = max(peak, eq)
        dd = max(dd, 1 - eq / peak)
    wins = [t["ret"] for t in trades if t["ret"] > 0]
    return {"trades": len(trades), "win_rate": round(len(wins) / len(trades) * 100) if trades else 0,
            "avg_trade": round(sum(t["ret"] for t in trades) / len(trades), 2) if trades else 0,
            "total_return": round((eq - 1) * 100, 1), "max_drawdown": round(dd * 100, 1),
            "buy_hold": round((rows[-1]["close"] / rows[200]["close"] - 1) * 100, 1),
            "years": round((n - 200) / 250, 1), "last_trades": trades[-5:]}


@app.get("/api/backtest")
def backtest_api(symbol: str):
    sym = clean(symbol)

    def run():
        rows = kite_candles(sym, "1D", 1800) if kite_ok() else None
        rows = rows or get_candles(sym, "1d", "5y")
        return backtest_stats(rows) if rows else None
    res = cached(("bt", sym), 3600, run)
    if not res:
        raise HTTPException(503, "No data for this symbol right now")
    return res


# ---------------- Sector view ----------------
SECTORS = {
    "Banks": ["HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK", "INDUSINDBK"],
    "IT": ["TCS", "INFY", "WIPRO", "TECHM", "HCLTECH", "LTIMINDTREE"],
    "Auto": ["MARUTI", "M&M", "HEROMOTOCO", "BAJAJ-AUTO", "EICHERMOT"],
    "FMCG": ["HINDUNILVR", "ITC", "NESTLEIND", "BRITANNIA", "TATACONSUM"],
    "Pharma": ["SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "LUPIN"],
    "Metals": ["TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "COALINDIA"],
    "Oil & Gas": ["RELIANCE", "ONGC", "BPCL", "IOC", "GAIL"],
    "Power": ["NTPC", "POWERGRID", "TATAPOWER"],
    "Infra & Cement": ["LT", "ULTRACEMCO", "GRASIM", "ADANIPORTS", "DLF"],
    "Finance (non-bank)": ["BAJFINANCE", "SHRIRAMFIN", "BAJAJFINSV"],
}


def stock_view(s):
    try:
        got = cached(("c", s, "1D"), 60, lambda: candles_src(s, "1D"))
        if not got or len(got[0]) < 60:
            return None
        rows = got[0]
        c = pd.Series([r["close"] for r in rows])
        d = decide(rows, 100000, 1)
        return {"symbol": s, "decision": d["decision"], "score": d["score"],
                "price": round(float(c.iloc[-1]), 2),
                "w": round((c.iloc[-1] / c.iloc[-6] - 1) * 100, 1),
                "m": round((c.iloc[-1] / c.iloc[-22] - 1) * 100, 1),
                "up": bool(c.iloc[-1] > ema(c, 50).iloc[-1])}
    except Exception:
        return None


@app.get("/api/sectors")
def sectors():
    def run():
        names = sorted({x for v in SECTORS.values() for x in v})
        with ThreadPoolExecutor(max_workers=4) as ex:
            views = dict(zip(names, ex.map(stock_view, names)))
        out = []
        for sec, syms in SECTORS.items():
            st = [views[x] for x in syms if views.get(x)]
            if len(st) < 2:
                continue
            sc = sum(x["score"] for x in st) / len(st)
            breadth = sum(x["up"] for x in st) / len(st) * 100
            label = "STRONG" if sc >= 1.5 and breadth >= 60 else "WEAK" if sc <= -1.5 and breadth <= 40 else "MIXED"
            out.append({"sector": sec, "label": label, "score": round(sc, 1), "breadth": round(breadth),
                        "w": round(sum(x["w"] for x in st) / len(st), 1),
                        "m": round(sum(x["m"] for x in st) / len(st), 1),
                        "stocks": sorted(st, key=lambda x: -x["score"])})
        return sorted(out, key=lambda o: -o["score"]) or None
    out = cached(("sectors",), 600, run)
    if not out:
        raise HTTPException(503, "No data right now. Try again in a minute.")
    return out


# ---------------- Strategy lab: several strategies, tested in two halves ----------------
def rsi_n(c, n):
    d = c.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    dn = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / dn.where(dn != 0))


def lab_strategies(df):
    c, h, l = df["close"], df["high"], df["low"]
    e20, e50, e100, e200 = ema(c, 20), ema(c, 50), ema(c, 100), ema(c, 200)
    r14, r2 = rsi_n(c, 14), rsi_n(c, 2)
    return {
        "Trend breakout (55-day high)": (
            (c > h.shift(1).rolling(55).max()) & (c > e100), c < l.shift(1).rolling(20).min(), 120, 3.0),
        "Pullback in uptrend": ((c > e100) & (r14 < 40), (c > e20) | (r14 > 60), 30, 2.5),
        "Oversold bounce (RSI2)": ((c > e200) & (r2 < 10), c > c.rolling(5).mean(), 10, 3.0),
        "EMA 20/50 cross": ((e20 > e50) & (e20.shift(1) <= e50.shift(1)) & (c > e100), e20 < e50, 150, 3.0),
    }


def simulate(df, atr, entry, exit_, hold, stop_atr, lo, hi):
    o, h, l, c = (df[k].to_numpy() for k in ("open", "high", "low", "close"))
    a, en, ex = atr.to_numpy(), entry.fillna(False).to_numpy(), exit_.fillna(False).to_numpy()
    out, i = [], lo
    while i < hi - 1:
        if not en[i] or a[i] != a[i]:
            i += 1
            continue
        buy, stop, j = o[i + 1] * (1 + COST), c[i] - stop_atr * a[i], i + 1
        while True:
            if l[j] <= stop:
                px = min(stop, o[j])
                break
            if ex[j] or j - i >= hold or j >= hi - 1:
                px = c[j]
                break
            j += 1
        out.append(px * (1 - COST) / buy - 1)
        i = j
    return out


def trade_stats(tr, hold_pct):
    eq = peak = 1.0
    dd = 0.0
    for r in tr:
        eq *= 1 + r
        peak = max(peak, eq)
        dd = max(dd, 1 - eq / peak)
    gain, loss = sum(r for r in tr if r > 0), -sum(r for r in tr if r < 0)
    return {"trades": len(tr), "win": round(len([r for r in tr if r > 0]) / len(tr) * 100) if tr else 0,
            "total": round((eq - 1) * 100, 1), "pf": round(gain / loss, 2) if loss else (9.99 if gain else 0),
            "dd": round(dd * 100, 1), "hold": round(hold_pct, 1)}


@app.get("/api/lab")
def lab(symbol: str):
    sym = clean(symbol)

    def run():
        rows = kite_candles(sym, "1D", 1800) if kite_ok() else None
        rows = rows or get_candles(sym, "1d", "5y")
        if not rows or len(rows) < 500:
            return {"error": "Not enough history to test this stock."}
        df = pd.DataFrame(rows)
        c, h, l = df["close"], df["high"], df["low"]
        tr_ = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
        atr = tr_.ewm(alpha=1 / 14, adjust=False).mean()
        n = len(df)
        split, start = int(n * 0.6), 200
        hold_a = (c.iloc[split - 1] / c.iloc[start] - 1) * 100
        hold_b = (c.iloc[n - 1] / c.iloc[split] - 1) * 100
        res = []
        for name, (en, ex, hold, sa) in lab_strategies(df).items():
            a = trade_stats(simulate(df, atr, en, ex, hold, sa, start, split), hold_a)
            b = trade_stats(simulate(df, atr, en, ex, hold, sa, split, n), hold_b)
            ok = a["total"] > 0 and b["total"] > 0 and a["trades"] + b["trades"] >= 10 and b["pf"] >= 1.1
            res.append({"name": name, "first": a, "second": b, "passed": ok, "signal_now": bool(en.iloc[-1]),
                        "stop": round(float(c.iloc[-1] - sa * atr.iloc[-1]), 2)})
        passed = sorted([r for r in res if r["passed"]], key=lambda r: -r["first"]["total"])
        best = passed[0] if passed else None
        if best and best["signal_now"]:
            verdict = f"BUY idea from '{best['name']}' (passed both test periods). Stop-loss near {best['stop']}."
        elif best:
            verdict = f"'{best['name']}' passed testing but gives no buy signal today. WAIT."
        else:
            verdict = "No strategy was profitable in both test periods for this stock. Do nothing."
        return {"symbol": sym, "verdict": verdict, "best": best["name"] if best else None, "results": res,
                "years": round((n - start) / 250, 1)}
    res = cached(("lab", sym), 3600, run)
    if not res:
        raise HTTPException(503, "No data for this symbol right now")
    return res


# ---------------- Intraday + long-term decisions ----------------
COST_I = 0.0005  # assumed 0.05% per side for intraday fees + slippage


def intraday_frame(rows):
    df = pd.DataFrame(rows)
    df["day"] = df["time"] // 86400          # time is IST-shifted, so this is the IST date
    df["mod"] = (df["time"] % 86400) // 60   # minute of the IST day (9:15 = 555)
    tp = (df["high"] + df["low"] + df["close"]) / 3
    g = df.groupby("day")
    cv = g["volume"].cumsum()
    df["vwap"] = (tp * df["volume"]).groupby(df["day"]).cumsum() / cv.where(cv > 0)
    c = df["close"]
    df["e9"], df["e21"], df["rsi"] = ema(c, 9), ema(c, 21), rsi_n(c, 14)
    tr = pd.concat([df["high"] - df["low"], (df["high"] - c.shift()).abs(), (df["low"] - c.shift()).abs()], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / 14, adjust=False).mean()
    df["bar"] = g.cumcount()
    df["orh"] = g["high"].transform(lambda x: x.iloc[:3].max())  # opening range = first 15 minutes
    df["orl"] = g["low"].transform(lambda x: x.iloc[:3].min())
    return df


def intraday_decide(rows, daily_up, capital, risk_pct):
    df = intraday_frame(rows)
    r = df.iloc[-1]
    last, atr = float(r["close"]), float(r["atr"])
    score, why = 0, []
    if r["vwap"] == r["vwap"]:
        up = last > r["vwap"]
        score += 1 if up else -1
        why.append("Price is above today's VWAP (buyers in control)." if up else "Price is below today's VWAP (sellers in control).")
    up = r["e9"] > r["e21"]
    score += 1 if up else -1
    why.append("Short-term trend is up (9 EMA above 21 EMA)." if up else "Short-term trend is down (9 EMA below 21 EMA).")
    if r["bar"] >= 3:
        if last > r["orh"]:
            score += 2; why.append("Broke above the first-15-minute high (opening range breakout).")
        elif last < r["orl"]:
            score -= 2; why.append("Broke below the first-15-minute low (opening range breakdown).")
        else:
            why.append("Still inside the opening range: no breakout yet.")
    rsi = float(r["rsi"])
    if 55 <= rsi <= 75:
        score += 1; why.append(f"RSI {rsi:.0f}: intraday momentum is positive.")
    elif rsi > 75:
        score -= 1; why.append(f"RSI {rsi:.0f}: stretched, a pullback is likely.")
    elif rsi <= 45:
        score -= 1; why.append(f"RSI {rsi:.0f}: intraday momentum is weak.")
    if daily_up is not None:
        score += 1 if daily_up else -1
        why.append("Daily trend agrees (above 50-day average)." if daily_up else "Daily trend is down (below 50-day average).")
    decision = "BUY (intraday)" if score >= 3 else "SELL / AVOID (intraday)" if score <= -3 else "WAIT (do nothing)"
    note = "Exit before 3:15 PM IST. Do not carry overnight."
    live = int(r["day"]) == int((time.time() + IST_OFFSET) // 86400)
    if not live:
        note = "Market is closed. This is based on the last session. " + note
    elif r["mod"] < 570:
        decision, note = "WAIT (do nothing)", "First 15 minutes: wait for the opening range to form."
    elif r["mod"] >= 900:
        decision, note = "WAIT (do nothing)", "After 3:00 PM: too late for a new intraday trade."
    out = {"decision": decision, "score": score, "reasons": why, "note": note, "price": last}
    if decision.startswith(("BUY", "SELL")) and atr > 0:
        d = 1 if decision.startswith("BUY") else -1
        out.update({"stop_loss": round(last - d * 1.5 * atr, 2), "target": round(last + d * 2.25 * atr, 2),
                    "shares_for_your_risk": math.floor(capital * risk_pct / 100 / (1.5 * atr))})
    return out


def longterm_decide(rows):
    c = pd.Series([r["close"] for r in rows])
    if len(c) < 260:
        return {"error": "Not enough history for a long-term view."}
    e200, last = ema(c, 200), float(c.iloc[-1])
    score, why = 0, []
    if last > e200.iloc[-1]:
        score += 2; why.append("Price is above its 200-day average (long-term uptrend).")
    else:
        score -= 2; why.append("Price is below its 200-day average (long-term downtrend).")
    if e200.iloc[-1] > e200.iloc[-21]:
        score += 1; why.append("The 200-day average is rising.")
    else:
        score -= 1; why.append("The 200-day average is falling.")
    if c.iloc[-22] / c.iloc[-252] - 1 > 0:
        score += 1; why.append("It gained over the past year (excluding the last month).")
    else:
        score -= 1; why.append("It lost over the past year (excluding the last month).")
    dd = 1 - last / float(c.iloc[-252:].max())
    if dd < 0.10:
        score += 1; why.append("Price is near its 52-week high.")
    elif dd > 0.35:
        score -= 1; why.append(f"Price is {dd * 100:.0f}% below its 52-week high.")
    decision = "ACCUMULATE (long-term trend positive)" if score >= 3 else \
        "AVOID / REDUCE (long-term trend negative)" if score <= -3 else "NEUTRAL (no clear long-term trend)"
    note = ("Buy in 3 to 4 parts over weeks, not all at once. Re-check if it closes below the 200-day average. "
            if score >= 3 else "") + "Price trend only. Check profits, debt and valuation before investing for years."
    return {"decision": decision, "score": score, "reasons": why, "note": note}


@app.get("/api/horizons")
def horizons(symbol: str, capital: float = 100000, risk: float = 1.0):
    sym = clean(symbol)
    capital, risk = min(max(capital, 1000), 1e9), min(max(risk, 0.1), 5)
    dg = cached(("c", sym, "1D"), 60, lambda: candles_src(sym, "1D"))
    out = {"symbol": sym}
    try:
        g5 = cached(("c", sym, "5m"), 5, lambda: candles_src(sym, "5m"))
        if not g5 or len(g5[0]) < 40:
            raise ValueError("No intraday data right now")
        up = None
        if dg and len(dg[0]) >= 60:
            cc = pd.Series([r["close"] for r in dg[0]])
            up = bool(cc.iloc[-1] > ema(cc, 50).iloc[-1])
        out["intraday"] = intraday_decide(g5[0], up, capital, risk)
    except Exception as e:
        out["intraday"] = {"error": "Intraday view unavailable: " + str(e)}
    out["longterm"] = longterm_decide(dg[0]) if dg else {"error": "No data right now"}
    out["disclaimer"] = "Educational decision support only. Not investment advice."
    return out


def sim_day(o, h, l, c, mod, atr, sig):
    out, i, n = [], 0, len(c)
    while i < n - 1 and len(out) < 2:
        if sig[i] == 0 or mod[i] < 570 or mod[i] > 840 or atr[i] != atr[i]:
            i += 1
            continue
        d, buy, j = sig[i], o[i + 1], i + 1
        stop, tgt = c[i] - d * 1.5 * atr[i], c[i] + d * 2.25 * atr[i]
        while True:
            if (l[j] <= stop) if d > 0 else (h[j] >= stop):
                px = stop
                break
            if (h[j] >= tgt) if d > 0 else (l[j] <= tgt):
                px = tgt
                break
            if mod[j] >= 915 or j >= n - 1:
                px = c[j]
                break
            j += 1
        out.append(d * (px - buy) / buy - 2 * COST_I)
        i = j + 1
    return out


@app.get("/api/intraday_lab")
def intraday_lab(symbol: str):
    sym = clean(symbol)

    def run():
        rows = kite_candles(sym, "5m", 60) if kite_ok() else None
        rows = rows or get_candles(sym, "5m", "60d")
        if not rows or len(rows) < 600:
            return {"error": "Not enough intraday history to test this stock."}
        df = intraday_frame(rows)
        up = (df["close"] > df["e9"]) & (df["e9"] > df["e21"])
        x_up = (df["e9"] > df["e21"]) & (df["e9"].shift(1) <= df["e21"].shift(1))
        x_dn = (df["e9"] < df["e21"]) & (df["e9"].shift(1) >= df["e21"].shift(1))
        late = df["bar"] >= 3
        sigs = {
            "Opening range breakout": (((df["close"] > df["orh"]) & late).astype(int) - ((df["close"] < df["orl"]) & late).astype(int)),
            "VWAP + EMA cross": ((x_up & (df["close"] > df["vwap"])).astype(int) - (x_dn & (df["close"] < df["vwap"])).astype(int)),
            "EMA 9/21 cross + RSI": ((x_up & (df["rsi"] > 50)).astype(int) - (x_dn & (df["rsi"] < 50)).astype(int)),
        }
        days = sorted(df["day"].unique())
        if len(days) < 12:
            return {"error": "Not enough trading days to test this stock."}
        half = len(days) // 2
        res = []
        for name, sg in sigs.items():
            halves = [[], []]
            for dday in days:
                m = (df["day"] == dday).to_numpy()
                d = df[m]
                tr = sim_day(d["open"].to_numpy(), d["high"].to_numpy(), d["low"].to_numpy(), d["close"].to_numpy(),
                             d["mod"].to_numpy(), d["atr"].to_numpy(), sg[m].to_numpy())
                halves[0 if dday in days[:half] else 1].extend(tr)
            a, b = trade_stats(halves[0], 0), trade_stats(halves[1], 0)
            ok = a["total"] > 0 and b["total"] > 0 and a["trades"] + b["trades"] >= 10 and b["pf"] >= 1.1
            res.append({"name": name, "first": a, "second": b, "passed": ok, "signal_now": int(sg.iloc[-1])})
        passed = sorted([r for r in res if r["passed"]], key=lambda r: -r["first"]["total"])
        best = passed[0] if passed else None
        verdict = (f"'{best['name']}' made money in both halves. Still a small sample." if best else
                   "No intraday strategy was profitable in both halves for this stock. Do nothing.")
        return {"symbol": sym, "verdict": verdict, "best": best["name"] if best else None, "results": res, "days": len(days)}
    res = cached(("ilab", sym), 1800, run)
    if not res:
        raise HTTPException(503, "No data for this symbol right now")
    return res


@app.get("/api/symbols")
def symbols():
    return WATCHLIST


@app.get("/api/candles")
def candles(symbol: str, tf: str = "5m"):
    sym = clean(symbol)
    if tf not in TIMEFRAMES:
        raise HTTPException(400, "Unknown timeframe")
    interval, period, intraday = TIMEFRAMES[tf]
    res = cached(("c", sym, tf), 5 if intraday else 60, lambda: candles_src(sym, tf))
    if not res:
        raise HTTPException(503, "No live data for this symbol right now")
    return {"symbol": sym, "tf": tf, "intraday": intraday, "source": res[1], "candles": res[0][-1500:]}


@app.get("/api/quote")
def quote(symbol: str):
    sym = clean(symbol)
    q = cached(("q", sym), 3, lambda: quote_src(sym))
    if not q:
        raise HTTPException(503, "No live data for this symbol right now")
    change = (q["price"] - q["prev"]) if q["prev"] else None
    return {
        "symbol": sym, "price": q["price"], "prev_close": q["prev"], "change": change,
        "change_pct": (change / q["prev"] * 100) if change is not None else None,
        "market_open": is_market_open(), "source": q.get("source", "yahoo"),
        "kite_configured": KITE_ON, "kite_connected": kite_ok(),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


@app.get("/api/watchlist")
def watchlist():
    data = cached(("w",), 10 if kite_ok() else 30, batch_src) or {}
    out = []
    for s in WATCHLIST:
        q = data.get(s)
        pct = ((q["last"] - q["prev"]) / q["prev"] * 100) if q and q["prev"] else None
        out.append({"symbol": s, "last": q["last"] if q else None, "pct": pct})
    return out


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/")
def index():
    return FileResponse(HERE / "index.html")
