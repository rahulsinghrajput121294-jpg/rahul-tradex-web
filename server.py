"""Rahul TradeX - standalone live chart website (FastAPI + Yahoo Finance)."""
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import yfinance as yf
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

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


@app.get("/api/symbols")
def symbols():
    return WATCHLIST


@app.get("/api/candles")
def candles(symbol: str, tf: str = "5m"):
    sym = clean(symbol)
    if tf not in TIMEFRAMES:
        raise HTTPException(400, "Unknown timeframe")
    interval, period, intraday = TIMEFRAMES[tf]
    rows = cached(("c", sym, tf), 10 if intraday else 60, lambda: get_candles(sym, interval, period))
    if not rows:
        raise HTTPException(503, "No live data for this symbol right now")
    return {"symbol": sym, "tf": tf, "intraday": intraday, "candles": rows[-1500:]}


@app.get("/api/quote")
def quote(symbol: str):
    sym = clean(symbol)
    q = cached(("q", sym), 5, lambda: get_quote(sym))
    if not q:
        raise HTTPException(503, "No live data for this symbol right now")
    change = (q["price"] - q["prev"]) if q["prev"] else None
    return {
        "symbol": sym, "price": q["price"], "prev_close": q["prev"], "change": change,
        "change_pct": (change / q["prev"] * 100) if change is not None else None,
        "market_open": is_market_open(),
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


@app.get("/api/watchlist")
def watchlist():
    data = cached(("w",), 30, get_batch) or {}
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
