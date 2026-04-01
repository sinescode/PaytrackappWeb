"""
telegram_service.py — Async Telegram photo sender (web-app edition)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  • Full async/await with aiohttp — zero thread-pool overhead
  • Connection pool reuse across all sends (TCPConnector)
  • Semaphore-capped concurrency (default 25 parallel sends)
  • Adaptive back-off on Telegram 429 / rate-limit responses
  • Exponential back-off with jitter on transient errors
  • BatchReport.to_dict() — return directly from your API endpoint
  • Proper async context-manager so the connector is always closed
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Sequence

import aiohttp

log = logging.getLogger(__name__)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BOT_TOKEN    = "8420317823:AAEzXkGqo7zWJ6tMclS1expTf6lZ4Jd25Hw"
TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# Tune these for your load profile
MAX_CONCURRENT  = 25    # parallel in-flight requests
MAX_RETRIES     = 4     # per-user retry ceiling
BASE_DELAY      = 0.4   # seconds — first retry delay
MAX_DELAY       = 16.0  # seconds — back-off ceiling
CONNECT_TIMEOUT = 8     # seconds
READ_TIMEOUT    = 20    # seconds


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
@dataclass
class SendResult:
    user_id : str
    ok      : bool
    blocked : bool  = False
    error   : str   = ""
    attempts: int   = 0
    elapsed : float = 0.0   # seconds

    def to_dict(self) -> dict:
        return {
            "user_id" : self.user_id,
            "ok"      : self.ok,
            "blocked" : self.blocked,
            "error"   : self.error,
            "attempts": self.attempts,
            "elapsed" : round(self.elapsed, 3),
        }


@dataclass
class BatchReport:
    total    : int
    succeeded: int
    blocked  : int
    failed   : int
    elapsed  : float
    results  : list[SendResult] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        return self.succeeded / self.total * 100 if self.total else 0.0

    def to_dict(self, include_results: bool = False) -> dict:
        d = {
            "total"       : self.total,
            "succeeded"   : self.succeeded,
            "blocked"     : self.blocked,
            "failed"      : self.failed,
            "elapsed_s"   : round(self.elapsed, 2),
            "success_rate": round(self.success_rate, 1),
            "rate_msg_s"  : round(self.total / self.elapsed, 1) if self.elapsed else 0,
        }
        if include_results:
            d["results"] = [r.to_dict() for r in self.results]
        return d


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def _is_blocked(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in [
        "blocked", "not found", "deleted",
        "deactivated", "chat not found", "forbidden",
    ])


def _build_caption(user_id: str, date: str) -> str:
    uid = html.escape(user_id)
    dt  = html.escape(date)
    return (

        "🧾 <b>Payment Receipt</b>\n\n"
        f"👤 <b>User ID:</b> <code>{uid}</code>\n"
        f"📅 <b>Date:</b> {dt}\n\n"
        "📨 Please <b>forward this message</b> to admin for payment verification.\n"
        "👨‍💼 <b>Admin:</b> @turja_un\n"

    )


def _extract_retry_after(body: str) -> float | None:
    try:
        data = json.loads(body)
        return float(data.get("parameters", {}).get("retry_after", 0)) or None
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class TelegramService:
    """
    Async Telegram photo sender.

    Recommended (reuse session across many calls):
        async with TelegramService() as svc:
            result = await svc.send_photo(uid, photo_bytes, date)
            report = await svc.send_many(users)
            return JSONResponse(report.to_dict())

    One-liner convenience:
        report = await TelegramService.quick_send(users)
    """

    def __init__(
        self,
        max_concurrent: int = MAX_CONCURRENT,
        max_retries   : int = MAX_RETRIES,
    ) -> None:
        self._sem       = asyncio.Semaphore(max_concurrent)
        self._max_retry = max_retries
        self._connector : aiohttp.TCPConnector | None = None
        self._session   : aiohttp.ClientSession | None = None

    # ── lifecycle ─────────────────────────────────────────────────────────────
    async def __aenter__(self) -> "TelegramService":
        await self._open()
        return self

    async def __aexit__(self, *_) -> None:
        await self._close()

    async def _open(self) -> None:
        self._connector = aiohttp.TCPConnector(
            limit           = MAX_CONCURRENT + 5,
            limit_per_host  = MAX_CONCURRENT + 5,
            ttl_dns_cache   = 300,
            ssl             = True,
        )
        self._session = aiohttp.ClientSession(
            connector = self._connector,
            timeout   = aiohttp.ClientTimeout(
                connect = CONNECT_TIMEOUT,
                total   = READ_TIMEOUT,
            ),
        )

    async def _close(self) -> None:
        if self._session:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        await asyncio.sleep(0.1)   # let SSL shutdown cleanly

    # ── single send ───────────────────────────────────────────────────────────
    async def send_photo(
        self,
        user_id    : str,
        photo_bytes: bytes,
        date       : str,
    ) -> SendResult:
        """Send one photo with full retry / back-off logic."""
        t0      = time.monotonic()
        caption = _build_caption(user_id, date)

        if len(caption) > 1024:
            return SendResult(
                user_id = user_id,
                ok      = False,
                error   = f"Caption too long ({len(caption)} chars)",
                elapsed = time.monotonic() - t0,
            )

        delay  = BASE_DELAY
        result = SendResult(user_id=user_id, ok=False)

        for attempt in range(1, self._max_retry + 1):
            result.attempts = attempt

            async with self._sem:
                try:
                    status, body = await self._post_photo(user_id, caption, photo_bytes)
                except aiohttp.ClientError as exc:
                    result.error = str(exc)
                    log.warning("uid=%s attempt=%d network: %s", user_id, attempt, exc)
                    if attempt < self._max_retry:
                        await asyncio.sleep(delay + random.uniform(0, 0.3))
                        delay = min(delay * 2, MAX_DELAY)
                    continue

            if status == 200 and '"ok":true' in body:
                result.ok    = True
                result.error = ""
                break

            if status == 429:
                retry_after = _extract_retry_after(body) or delay
                log.warning("uid=%s rate-limited, sleeping %.1fs", user_id, retry_after)
                await asyncio.sleep(retry_after + random.uniform(0, 0.5))
                delay = min(retry_after * 1.5, MAX_DELAY)
                continue

            if _is_blocked(body):
                result.blocked = True
                result.error   = "User blocked bot"
                break

            if status in (400, 403):
                result.error = body
                break

            result.error = body
            log.warning("uid=%s attempt=%d status=%d", user_id, attempt, status)
            if attempt < self._max_retry:
                await asyncio.sleep(delay + random.uniform(0, 0.3))
                delay = min(delay * 2, MAX_DELAY)

        result.elapsed = time.monotonic() - t0
        return result

    async def _post_photo(
        self,
        user_id    : str,
        caption    : str,
        photo_bytes: bytes,
    ) -> tuple[int, str]:
        data = aiohttp.FormData()
        data.add_field("chat_id",    user_id)
        data.add_field("caption",    caption)
        data.add_field("parse_mode", "HTML")
        data.add_field(
            "photo",
            photo_bytes,
            filename     = "payment.png",
            content_type = "image/png",
        )
        async with self._session.post(f"{TELEGRAM_API}/sendPhoto", data=data) as resp:
            return resp.status, await resp.text()

    # ── batch send ────────────────────────────────────────────────────────────
    async def send_many(
        self,
        users: Sequence[tuple[str, bytes, str]],
    ) -> BatchReport:
        """
        Send to many users concurrently.
        users: list of (user_id, photo_bytes, date)
        """
        t0      = time.monotonic()
        tasks   = [self.send_photo(uid, pb, dt) for uid, pb, dt in users]
        results = await asyncio.gather(*tasks)
        elapsed = time.monotonic() - t0

        succeeded = sum(1 for r in results if r.ok)
        blocked   = sum(1 for r in results if r.blocked)
        failed    = len(results) - succeeded - blocked

        return BatchReport(
            total     = len(results),
            succeeded = succeeded,
            blocked   = blocked,
            failed    = failed,
            elapsed   = elapsed,
            results   = list(results),
        )

    # ── class-level convenience ───────────────────────────────────────────────
    @classmethod
    async def quick_send(
        cls,
        users: Sequence[tuple[str, bytes, str]],
    ) -> BatchReport:
        """One-liner: open session → send all → close → return report."""
        async with cls() as svc:
            return await svc.send_many(users)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Synchronous shim — backward-compatible with old call-sites
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def send_photo(user_id: str, photo_bytes: bytes, date: str) -> dict:
    return asyncio.run(_async_send_one(user_id, photo_bytes, date))


def send_photo_with_retry(
    user_id    : str,
    photo_bytes: bytes,
    date       : str,
    max_retries: int = MAX_RETRIES,
) -> dict:
    return asyncio.run(_async_send_one(user_id, photo_bytes, date, max_retries))


async def _async_send_one(
    user_id    : str,
    photo_bytes: bytes,
    date       : str,
    max_retries: int = MAX_RETRIES,
) -> dict:
    async with TelegramService(max_retries=max_retries) as svc:
        r = await svc.send_photo(user_id, photo_bytes, date)
    if r.ok:
        return {"ok": True}
    d: dict = {"ok": False, "error": r.error}
    if r.blocked:
        d["blocked"] = True
    return d
