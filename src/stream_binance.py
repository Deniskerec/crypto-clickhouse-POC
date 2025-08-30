import os
import ssl
import json
import asyncio
from datetime import datetime, timezone

import websockets          # WebSocket client to receive Binance live trades
import certifi             # Up-to-date CA bundle for TLS verification
from dotenv import load_dotenv  # Load settings from .env

from db import ch_client   # Your ClickHouse client factory (reads .env)

# Load environment variables from .env into process env
load_dotenv()

# ---------- Config pulled from environment (.env) ----------
# Comma-separated list of Binance symbols to subscribe to (e.g., "btcusdt,ethusdt")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")]
# How many rows to buffer before inserting to ClickHouse (bigger = fewer inserts, lower latency to DB is higher)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
# Flush buffer every N seconds even if BATCH_SIZE not reached (prevents data waiting too long)
FLUSH_EVERY_SEC = int(os.getenv("FLUSH_EVERY_SEC", "5"))
# Fully-qualified destination table (defaults to crypto.trades)
TABLE = f"{os.getenv('CH_DATABASE','crypto')}.trades"

# Create a ClickHouse client (TLS-enabled with certifi trust store)
CLIENT = ch_client()

# In-memory buffer to batch trades before inserting
BUFFER = []

# ---------- Helpers ----------

def combined_stream_url(symbols):
    """
    Build Binance combined-stream URL for multiple symbols.
    Example:
      symbols = ["btcusdt","ethusdt"]
      -> wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
    """
    streams = "/".join(f"{s}@trade" for s in symbols)
    return f"wss://stream.binance.com:9443/stream?streams={streams}"

def ms_to_dt(ms: int):
    """
    Convert Binance millisecond timestamps to a timezone-aware UTC datetime.
    ClickHouse driver accepts datetime objects for DateTime columns.
    """
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

def flush():
    """
    Insert buffered rows into ClickHouse in a single bulk insert.
    Then clear the buffer. No-op if buffer is empty.
    """
    global BUFFER
    if not BUFFER:
        return
    CLIENT.insert(
        TABLE,
        BUFFER,
        column_names=["symbol","trade_id","price","qty","ts","is_buyer_maker"],
    )
    print(f"✅ Inserted {len(BUFFER)} rows")
    BUFFER = []

async def periodic_flush():
    """
    Background task that flushes the buffer every FLUSH_EVERY_SEC seconds.
    This ensures data keeps flowing even during low-traffic periods.
    """
    while True:
        await asyncio.sleep(FLUSH_EVERY_SEC)
        flush()

# ---------- Main streaming coroutine ----------

async def run():
    """
    Connect to Binance combined WebSocket stream, read trade messages,
    normalize fields, buffer them, and batch-insert to ClickHouse.
    """
    url = combined_stream_url(SYMBOLS)
    print(f"Connecting: {url}")

    # Create TLS context and load certifi CA bundle (prevents SSL errors)
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.load_verify_locations(certifi.where())

    # Start periodic time-based flusher in the background
    flusher = asyncio.create_task(periodic_flush())
    try:
        # Open WebSocket connection (ping keeps connection alive)
        async with websockets.connect(url, ssl=ssl_ctx, ping_interval=20, ping_timeout=20) as ws:
            print(f"Streaming {len(SYMBOLS)} symbols → {TABLE} (Ctrl+C to stop)")
            # Continuously read messages from Binance
            async for msg in ws:
                # Combined stream wraps payload: {"stream":"...","data":{...trade...}}
                env = json.loads(msg)
                ev = env.get("data", {})

                # Normalize into a tuple matching ClickHouse columns
                row = (
                    ev.get("s"),                 # symbol (e.g., 'BTCUSDT')
                    int(ev.get("t", 0)),         # trade_id
                    float(ev.get("p", "0")),     # price
                    float(ev.get("q", "0")),     # quantity
                    ms_to_dt(ev.get("T", 0)),    # trade time as datetime (UTC)
                    1 if ev.get("m") else 0      # is_buyer_maker (bool → UInt8)
                )

                # Add to buffer and flush if batch is full
                BUFFER.append(row)
                if len(BUFFER) >= BATCH_SIZE:
                    flush()
    finally:
        # Always cancel the periodic flusher and flush any remaining rows on exit
        flusher.cancel()
        flush()

# ---------- Entry point ----------

if __name__ == "__main__":
    try:
        # Run the async stream loop until interrupted
        asyncio.run(run())
    except KeyboardInterrupt:
        # Graceful shutdown on Ctrl+C
        print("\nStopping… flushing buffer.")
        flush()