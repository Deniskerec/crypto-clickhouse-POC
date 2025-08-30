# api/server.py
import os
from typing import List, Dict, Any

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from src.db import ch_client
from api.collector import collector

APP_TITLE = "Crypto ClickHouse API"

ALLOWED_ORIGINS = [
    *[o.strip() for o in os.getenv(
        "ALLOWED_ORIGINS",
        "http://127.0.0.1:8080,http://localhost:8080"
    ).split(",") if o.strip()],
]

app = FastAPI(title=APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def rows_to_dicts(result) -> List[Dict[str, Any]]:
    """clickhouse-connect result -> list of dicts."""
    cols = result.column_names
    out: List[Dict[str, Any]] = []
    for row in result.result_rows:
        out.append({cols[i]: row[i] for i in range(len(cols))})
    return out


# ---------- Collector control ----------
@app.post("/collector/start")
async def start_collector():
    started = await collector.start()
    return {"started": started, "status": collector.status()}

@app.post("/collector/stop")
async def stop_collector():
    stopped = await collector.stop()
    return {"stopped": stopped, "status": collector.status()}

@app.get("/collector/status")
async def collector_status():
    return collector.status()


# ---------- Data endpoints ----------
@app.get("/ohlcv")
def ohlcv(symbol: str, minutes: int = 60):
    """
    1-min OHLCV for the last N minutes for a symbol.
    """
    q = """
    SELECT
      toStartOfMinute(ts) AS minute,
      argMin(price, ts) AS open,
      max(price)        AS high,
      min(price)        AS low,
      anyLast(price)    AS close,
      sum(qty)          AS volume,
      count()           AS trades
    FROM crypto.trades
    WHERE symbol = %(symbol)s
      AND ts >= now() - INTERVAL %(minutes)s MINUTE
    GROUP BY minute
    ORDER BY minute
    """
    client = ch_client()
    res = client.query(q, parameters={"symbol": symbol, "minutes": minutes})
    rows = rows_to_dicts(res)
    # make ISO strings
    for r in rows:
        if hasattr(r["minute"], "isoformat"):
            r["minute"] = r["minute"].isoformat()
    return rows


@app.get("/top_symbols")
def top_symbols(minutes: int = 10, limit: int = 10):
    """
    Top symbols by traded volume in the last N minutes.
    """
    q = """
    SELECT
      symbol,
      sum(qty)  AS volume,
      count()   AS trades
    FROM crypto.trades
    WHERE ts >= now() - INTERVAL %(minutes)s MINUTE
    GROUP BY symbol
    ORDER BY volume DESC
    LIMIT %(limit)s
    """
    client = ch_client()
    res = client.query(q, parameters={"minutes": minutes, "limit": limit})
    return rows_to_dicts(res)


@app.get("/live_trades")
def live_trades(symbol: str, window_sec: int = 60):
    """
    Raw trades for the last N seconds for a symbol (newest first).
    """
    q = """
    SELECT
      ts,
      symbol,
      price,
      qty,
      is_buyer_maker
    FROM crypto.trades
    WHERE symbol = %(symbol)s
      AND ts >= now() - INTERVAL %(sec)s SECOND
    ORDER BY ts DESC
    LIMIT 500
    """
    client = ch_client()
    res = client.query(q, parameters={"symbol": symbol, "sec": window_sec})
    rows = rows_to_dicts(res)
    for r in rows:
        if hasattr(r["ts"], "isoformat"):
            r["ts"] = r["ts"].isoformat()
    return rows


@app.get("/live_buy_sell")
def live_buy_sell(minutes: int = 10, top: int = 5):
    """
    Per-symbol Buy/Sell aggregates over the last N minutes.
    Returns top symbols by total volume.
    """
    q = """
    WITH base AS
    (
        SELECT
          symbol,
          sumIf(qty, is_buyer_maker = 0) AS buy_volume,
          sumIf(qty, is_buyer_maker = 1) AS sell_volume,
          sumIf(price*qty, is_buyer_maker = 0) / nullIf(sumIf(qty, is_buyer_maker = 0), 0) AS avg_buy_price,
          sumIf(price*qty, is_buyer_maker = 1) / nullIf(sumIf(qty, is_buyer_maker = 1), 0) AS avg_sell_price,
          count() / %(minutes)s AS trades_per_min,
          buy_volume + sell_volume AS total_vol
        FROM crypto.trades
        WHERE ts >= now() - INTERVAL %(minutes)s MINUTE
        GROUP BY symbol
    )
    SELECT
      symbol,
      buy_volume,
      sell_volume,
      avg_buy_price,
      avg_sell_price,
      trades_per_min
    FROM base
    ORDER BY total_vol DESC
    LIMIT %(top)s
    """
    client = ch_client()
    res = client.query(q, parameters={"minutes": minutes, "top": top})
    return rows_to_dicts(res)


@app.get("/hist_buy_sell")
def hist_buy_sell(
    symbol: str,
    minutes: int = Query(60, ge=1, description="Lookback window in minutes"),
):
    """
    Per-minute series for buy/sell volume & avg price & trades/min for one symbol.
    """
    q = """
    SELECT
      toStartOfMinute(ts) AS minute,
      sumIf(qty, is_buyer_maker = 0) AS buy_volume,
      sumIf(qty, is_buyer_maker = 1) AS sell_volume,
      sumIf(price*qty, is_buyer_maker = 0) / nullIf(sumIf(qty, is_buyer_maker = 0), 0) AS avg_buy_price,
      sumIf(price*qty, is_buyer_maker = 1) / nullIf(sumIf(qty, is_buyer_maker = 1), 0) AS avg_sell_price,
      count() AS trades
    FROM crypto.trades
    WHERE symbol = %(symbol)s
      AND ts >= now() - INTERVAL %(minutes)s MINUTE
    GROUP BY minute
    ORDER BY minute
    """
    client = ch_client()
    res = client.query(q, parameters={"symbol": symbol, "minutes": minutes})
    rows = rows_to_dicts(res)
    for r in rows:
        if hasattr(r["minute"], "isoformat"):
            r["minute"] = r["minute"].isoformat()
    return rows