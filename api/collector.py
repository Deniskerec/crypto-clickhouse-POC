# src/collector.py
import os, ssl, json, asyncio
from datetime import datetime, timezone
from typing import Optional
import websockets, certifi
from dotenv import load_dotenv
from src.db import ch_client

load_dotenv()

CH_DATABASE = os.getenv("CH_DATABASE", "crypto")
SYMBOLS = [s.strip() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",")]
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
FLUSH_EVERY_SEC = int(os.getenv("FLUSH_EVERY_SEC", "5"))
TABLE = f"{CH_DATABASE}.trades"

def combined_url(symbols): return f"wss://stream.binance.com:9443/stream?streams={'/'.join(f'{s}@trade' for s in symbols)}"
def ms_to_dt(ms: int):     return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

class Collector:
    def __init__(self):
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._inserted = 0
        self._last_flush: Optional[datetime] = None
        self._symbols = SYMBOLS
        self._last_error: Optional[str] = None
        self._state = "idle"  # idle|starting|running|stopping

    def status(self) -> dict:
        return {
            "running": self._running,
            "state": self._state,
            "inserted_rows": self._inserted,
            "last_flush": self._last_flush.isoformat() if self._last_flush else None,
            "last_error": self._last_error,
            "symbols": list(self._symbols),
            "batch_size": BATCH_SIZE,
            "flush_every_sec": FLUSH_EVERY_SEC,
            "table": TABLE,
        }

    async def start(self) -> bool:
        if self._running or self._state in ("starting", "running"):
            return False
        self._state, self._last_error = "starting", None
        self._task = asyncio.create_task(self._run())
        # wait briefly to see if startup fails
        try:
            await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            pass
        return True

    async def stop(self) -> bool:
        if not self._running and self._state != "running":
            return False
        self._state = "stopping"
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._state, self._running = "idle", False
        return True

    async def _run(self):
        client = None
        buffer = []
        url = combined_url(self._symbols)
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.load_verify_locations(certifi.where())

        async def flush():
            nonlocal buffer, client
            if not buffer:
                return
            client.insert(
                TABLE, buffer,
                column_names=["symbol","trade_id","price","qty","ts","is_buyer_maker"],
            )
            self._inserted += len(buffer)
            self._last_flush = datetime.now(timezone.utc)
            buffer = []

        async def periodic_flusher():
            while True:
                await asyncio.sleep(FLUSH_EVERY_SEC)
                await flush()

        try:
            client = ch_client()  # might raise if creds/bad host
            self._running, self._state = True, "running"
            flusher = asyncio.create_task(periodic_flusher())
            try:
                async with websockets.connect(url, ssl=ssl_ctx, ping_interval=20, ping_timeout=20) as ws:
                    async for msg in ws:
                        if not self._running:
                            break
                        env = json.loads(msg)
                        ev = env.get("data", {})
                        buffer.append((
                            ev.get("s"),
                            int(ev.get("t", 0)),
                            float(ev.get("p", "0")),
                            float(ev.get("q", "0")),
                            ms_to_dt(ev.get("T", 0)),
                            1 if ev.get("m") else 0,
                        ))
                        if len(buffer) >= BATCH_SIZE:
                            await flush()
            finally:
                flusher.cancel()
                try:
                    await flusher
                except asyncio.CancelledError:
                    pass
                await flush()
        except Exception as e:
            # surface error to /collector/status
            self._last_error = f"{type(e).__name__}: {e}"
        finally:
            self._running = False
            if self._state != "stopping":
                self._state = "idle"

collector = Collector()