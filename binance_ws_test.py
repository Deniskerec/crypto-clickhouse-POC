import os
import ssl
import json
import asyncio
from datetime import datetime, timezone

import websockets
import certifi
from clickhouse_connect import get_client

# -----------------------------
# Binance config
# -----------------------------
BINANCE_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"
BATCH_SIZE = 10
BUFFER = []

# -----------------------------
# ClickHouse Cloud connection
# -----------------------------
CH_HOST = "c7qmke2kau.eu-central-1.aws.clickhouse.cloud"   # replace with your host
CH_PORT = 8443
CH_USER = "default"                                        # or your own user
CH_PASS = "celF5MgwL.A~b"                                  # paste from Cloud UI

# client with certifi CA bundle
client = get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASS,
    secure=True,                    # required
    verify=True,                    # enforce cert validation
    ca_cert=certifi.where(),        # use certifi trust store
)

TABLE = "default.trades"


# -----------------------------
# Helpers
# -----------------------------
def ms_to_dt(ms: int) -> datetime:
    # return a timezone-aware datetime in UTC
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


def flush():
    """Insert buffered rows into ClickHouse."""
    global BUFFER
    if not BUFFER:
        return
    client.insert(
        TABLE,
        BUFFER,
        column_names=["symbol", "trade_id", "price", "qty", "ts", "is_buyer_maker"],
    )
    print(f"✅ Inserted {len(BUFFER)} rows into {TABLE}")
    BUFFER = []


# -----------------------------
# Main async stream
# -----------------------------
async def run():
    print("Connecting to Binance…")
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.load_verify_locations(certifi.where())

    async with websockets.connect(BINANCE_URL, ssl=ssl_ctx, ping_interval=20, ping_timeout=20) as ws:
        print("Connected. Streaming BTCUSDT → ClickHouse Cloud (Ctrl+C to stop).\n")
        async for msg in ws:
            ev = json.loads(msg)
            row = (
                ev.get("s"),  # symbol
                int(ev.get("t", 0)),  # trade_id
                float(ev.get("p", "0")),  # price
                float(ev.get("q", "0")),  # qty
                ms_to_dt(ev.get("T", 0)),  # <-- datetime object (NOT string)
                1 if ev.get("m") else 0  # is_buyer_maker
            )
            BUFFER.append(row)
            if len(BUFFER) >= BATCH_SIZE:
                flush()


# -----------------------------
# Entrypoint
# -----------------------------
if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("\nStopping… flushing buffer.")
        flush()