"""
Transport layer for LMArenaBridge.

Contains stream response classes, userscript proxy queue helpers, and proxy fetch
implementations. Cross-module globals (from main.py) are accessed via _m() late-import
so test patches on main.X remain effective.
"""

import asyncio
import json
import time
import uuid
from typing import Optional
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException, Request
from . import constants as _constants

HTTPStatus = _constants.HTTPStatus
DEFAULT_PICKUP_TIMEOUT_SECONDS = _constants.DEFAULT_USERSCRIPT_PROXY_PICKUP_TIMEOUT_SECONDS


def _m():
    """Late import of main module so tests can patch main.X and it is reflected here."""
    from . import main
    return main


class BrowserFetchStreamResponse:
    def __init__(
        self,
        status_code: int,
        headers: Optional[dict],
        text: str = "",
        method: str = "POST",
        url: str = "",
        lines_queue: Optional[asyncio.Queue] = None,
        done_event: Optional[asyncio.Event] = None,
        job_id: str | None = None,
    ):
        self.status_code = int(status_code or 0)
        self.headers = headers or {}
        self._text = text or ""
        self._method = str(method or "POST")
        self._url = str(url or "")
        self._lines_queue = lines_queue
        self._done_event = done_event
        self.job_id = str(job_id or "")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        return False

    async def aclose(self) -> None:
        return None

    @property
    def text(self) -> str:
        if self._lines_queue is not None and not self._text:
            # This is a bit dangerous in a property because it's sync, 
            # but BrowserFetchStreamResponse is used in contexts where .text is expected.
            # However, in this codebase, we mostly use await aread() or aiter_lines().
            # Let's make it safe by NOT buffering here, but informing that it might be empty
            # OR better: the codebase should use await aread().
            return self._text
        return self._text

    async def aiter_lines(self):
        if self._lines_queue is not None:
            # Streaming mode
            while True:
                if self._done_event and self._done_event.is_set() and self._lines_queue.empty():
                    break
                try:
                    # Brief timeout to check done_event occasionally
                    line = await asyncio.wait_for(self._lines_queue.get(), timeout=1.0)
                    if line is None: # Sentinel for EOF
                        break
                    yield line
                except asyncio.TimeoutError:
                    continue
        else:
            # Buffered mode
            for line in self._text.splitlines():
                yield line

    async def aread(self) -> bytes:
        if self._lines_queue is not None:
            # If we try to read the full body of a streaming response, we buffer it all first.
            collected = []
            async for line in self.aiter_lines():
                collected.append(line)
            self._text = "\n".join(collected)
            self._lines_queue = None
            self._done_event = None
        return self._text.encode("utf-8")

    def raise_for_status(self) -> None:
        if self.status_code == 0 or self.status_code >= 400:
            request = httpx.Request(self._method, self._url or "https://lmarena.ai/")
            response = httpx.Response(self.status_code or 502, request=request, content=self._text.encode("utf-8"))
            raise httpx.HTTPStatusError(f"HTTP {self.status_code}", request=request, response=response)




def _touch_userscript_poll(now: Optional[float] = None) -> None:
    """
    Update userscript-proxy "last seen" timestamps.

    Only external proxy pollers should update this signal. Internal workers must not
    mark the proxy as active.
    """
    ts = float(now if now is not None else _m().time.time())
    _m().USERSCRIPT_PROXY_LAST_POLL_AT = ts
    # Legacy timestamp used by older code paths/tests.
    _m().last_userscript_poll = ts


def _get_userscript_proxy_queue() -> asyncio.Queue:
    if _m()._USERSCRIPT_PROXY_QUEUE is None:
        _m()._USERSCRIPT_PROXY_QUEUE = asyncio.Queue()
    return _m()._USERSCRIPT_PROXY_QUEUE


def _userscript_proxy_is_active(config: Optional[dict] = None) -> bool:
    cfg = config or _m().get_config()
    poll_timeout = 25
    try:
        poll_timeout = int(cfg.get("userscript_proxy_poll_timeout_seconds", 25))
    except Exception:
        poll_timeout = 25
    active_window = max(10, min(poll_timeout + 10, 90))
    try:
        last = max(float(_m().USERSCRIPT_PROXY_LAST_POLL_AT or 0.0), float(_m().last_userscript_poll or 0.0))
    except Exception:
        last = float(_m().USERSCRIPT_PROXY_LAST_POLL_AT or 0.0)
    try:
        delta = float(_m().time.time()) - float(last)
    except Exception:
        delta = 999999.0
    # Guard against clock skew / patched clocks in tests: a "last poll" timestamp in the future is not active.
    if delta < 0:
        return False
    return delta <= float(active_window)


def _userscript_proxy_check_secret(request: Request) -> None:
    cfg = _m().get_config()
    secret = str(cfg.get("userscript_proxy_secret") or "").strip()
    if secret and request.headers.get("X-LMBridge-Secret") != secret:
        raise HTTPException(status_code=401, detail="Invalid userscript proxy secret")


def _cleanup_userscript_proxy_jobs(config: Optional[dict] = None) -> None:
    cfg = config or _m().get_config()
    ttl_seconds = 90
    try:
        ttl_seconds = int(cfg.get("userscript_proxy_job_ttl_seconds", 90))
    except Exception:
        ttl_seconds = 90
    ttl_seconds = max(10, min(ttl_seconds, 600))

    now = _m().time.time()
    expired: list[str] = []
    for job_id, job in list(_m()._USERSCRIPT_PROXY_JOBS.items()):
        created_at = float(job.get("created_at") or 0.0)
        done = bool(job.get("done"))
        picked_up = False
        try:
            picked_up_event = job.get("picked_up_event")
            if isinstance(picked_up_event, asyncio.Event):
                picked_up = bool(picked_up_event.is_set())
        except Exception:
            picked_up = False
        if done and (now - created_at) > ttl_seconds:
            expired.append(job_id)
        # If a job was never picked up, expire it even if not marked done (stuck/abandoned queue entries).
        elif (not done) and (not picked_up) and (now - created_at) > ttl_seconds:
            expired.append(job_id)
        # Safety: even if picked up, expire if it's been in-flight for too long (e.g. browser crash).
        elif (not done) and picked_up and (now - created_at) > (ttl_seconds * 5):
            expired.append(job_id)
    for job_id in expired:
        _m()._USERSCRIPT_PROXY_JOBS.pop(job_id, None)


def _mark_userscript_proxy_inactive() -> None:
    """
    Mark the userscript-proxy as inactive.

    Do this when we detect proxy health/timeouts so strict-model routing stops preferring a proxy that is not
    responding. The proxy becomes active again once a real poll/push updates the timestamps.
    """
    _m().USERSCRIPT_PROXY_LAST_POLL_AT = 0.0
    _m().last_userscript_poll = 0.0


async def _finalize_userscript_proxy_job(job_id: str, *, error: Optional[str] = None, remove: bool = False) -> None:
    """
    Finalize a userscript-proxy job without touching proxy "last seen" timestamps.

    This is intentionally separate from proxy push handling: server-side timeouts must not keep the proxy
    marked as "active" because that would route future requests back into a dead proxy.
    """
    jid = str(job_id or "").strip()
    if not jid:
        return
    job = _m()._USERSCRIPT_PROXY_JOBS.get(jid)
    if not isinstance(job, dict):
        return

    if error and not job.get("error"):
        job["error"] = str(error)

    if job.get("_finalized"):
        if remove:
            _m()._USERSCRIPT_PROXY_JOBS.pop(jid, None)
        return

    job["_finalized"] = True
    job["done"] = True

    done_event = job.get("done_event")
    if isinstance(done_event, asyncio.Event):
        done_event.set()
    status_event = job.get("status_event")
    if isinstance(status_event, asyncio.Event):
        status_event.set()

    q = job.get("lines_queue")
    if isinstance(q, asyncio.Queue):
        try:
            q.put_nowait(None)
        except Exception:
            try:
                await q.put(None)
            except Exception:
                pass

    if remove:
        _m()._USERSCRIPT_PROXY_JOBS.pop(jid, None)


class UserscriptProxyStreamResponse:
    def __init__(self, job_id: str, timeout_seconds: int = 120):
        self.job_id = str(job_id)
        self._status_code: int = 200
        self._headers: dict = {}
        self._timeout_seconds = int(timeout_seconds or 120)
        self._method = "POST"
        self._url = "https://lmarena.ai/"

    @property
    def status_code(self) -> int:
        # Do not rely on a snapshot: proxy workers can report status after `__aenter__` returns.
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if isinstance(job, dict):
            status = job.get("status_code")
            if isinstance(status, int):
                return int(status)
        return int(self._status_code or 0)

    @status_code.setter
    def status_code(self, value: int) -> None:
        try:
            self._status_code = int(value)
        except Exception:
            self._status_code = 0

    @property
    def headers(self) -> dict:
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if isinstance(job, dict):
            headers = job.get("headers")
            if isinstance(headers, dict):
                return headers
        return self._headers

    @headers.setter
    def headers(self, value: dict) -> None:
        self._headers = value if isinstance(value, dict) else {}

    async def __aenter__(self):
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if not isinstance(job, dict):
            self.status_code = 503
            return self
        # Give the proxy a short window to report the upstream HTTP status before we snapshot it, but don't
        # block if it has already started streaming lines (some proxy implementations report status late).
        status_event = job.get("status_event")
        should_wait_status = False
        if isinstance(status_event, asyncio.Event) and not status_event.is_set():
            should_wait_status = True
            try:
                if job.get("error"):
                    should_wait_status = False
            except Exception:
                pass
            done_event = job.get("done_event")
            if isinstance(done_event, asyncio.Event) and done_event.is_set():
                should_wait_status = False
            q = job.get("lines_queue")
            if isinstance(q, asyncio.Queue) and not q.empty():
                should_wait_status = False

        if should_wait_status:
            try:
                await asyncio.wait_for(
                    status_event.wait(),
                    timeout=min(15.0, float(max(1, self._timeout_seconds))),
                )
            except Exception:
                pass
        self._method = str(job.get("method") or "POST")
        self._url = str(job.get("url") or self._url)
        status = job.get("status_code")
        if isinstance(status, int):
            self.status_code = int(status)
        headers = job.get("headers")
        if isinstance(headers, dict):
            self.headers = headers
        return self

    async def __aexit__(self, exc_type, exc, tb) -> bool:
        await self.aclose()
        return False

    async def aclose(self) -> None:
        # Do not eagerly delete completed jobs here.
        #
        # Callers may need to inspect `status_code`/`error` after the context exits (e.g. to decide whether to
        # fall back to Chrome fetch). Jobs are pruned by `_cleanup_userscript_proxy_jobs()` on a short TTL.
        return None

    async def aiter_lines(self):
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if not isinstance(job, dict):
            return
        q = job.get("lines_queue")
        done_event = job.get("done_event")
        if not isinstance(q, asyncio.Queue) or not isinstance(done_event, asyncio.Event):
            return

        deadline = _m().time.time() + float(max(5, self._timeout_seconds))
        while True:
            if done_event.is_set() and q.empty():
                break
            remaining = deadline - _m().time.time()
            if remaining <= 0:
                job["error"] = job.get("error") or "userscript proxy timeout"
                job["done"] = True
                done_event.set()
                break
            timeout = max(0.25, min(2.0, remaining))
            try:
                item = await asyncio.wait_for(q.get(), timeout=timeout)
            except asyncio.TimeoutError:
                continue
            if item is None:
                break
            yield str(item)

    async def aread(self) -> bytes:
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if not isinstance(job, dict):
            return b""
        q = job.get("lines_queue")
        if not isinstance(q, asyncio.Queue):
            return b""
        items: list[str] = []
        try:
            while True:
                item = q.get_nowait()
                if item is None:
                    break
                items.append(str(item))
        except Exception:
            pass
        return ("\n".join(items)).encode("utf-8")

    def raise_for_status(self) -> None:
        job = _m()._USERSCRIPT_PROXY_JOBS.get(self.job_id)
        if isinstance(job, dict) and job.get("error"):
            request = httpx.Request(self._method, self._url)
            response = httpx.Response(503, request=request, content=str(job.get("error")).encode("utf-8"))
            raise httpx.HTTPStatusError("Userscript proxy error", request=request, response=response)
        status = int(self.status_code or 0)
        if status == 0 or status >= 400:
            request = httpx.Request(self._method, self._url)
            response = httpx.Response(status or 502, request=request)
            raise httpx.HTTPStatusError(f"HTTP {status}", request=request, response=response)


def _normalize_userscript_proxy_url(url: str) -> str:
    """
    Convert LMArena absolute URLs into same-origin paths for in-page fetch.

    Only allow arena.ai/lmarena.ai origins. Any non-allowlisted URL returns an empty string
    so callers can reject it.
    """
    text = str(url or "").strip()
    if not text:
        return ""
    if text.startswith("/"):
        return text
    try:
        parts = urlsplit(text)
    except Exception:
        return ""
    if not parts.scheme or not parts.netloc:
        return ""
    if str(parts.scheme or "").lower() != "https":
        return ""
    host = str(parts.netloc or "").split("@")[-1].split(":")[0].lower()
    if host not in _constants.ARENA_HOST_TO_ORIGIN:
        return ""
    path = parts.path or "/"
    if parts.query:
        path = f"{path}?{parts.query}"
    return path


async def fetch_lmarena_stream_via_userscript_proxy(
    http_method: str,
    url: str,
    payload: dict,
    timeout_seconds: int = 120,
    headers: Optional[dict] = None,
) -> Optional[UserscriptProxyStreamResponse]:
    config = _m().get_config()
    _cleanup_userscript_proxy_jobs(config)

    job_id = str(uuid.uuid4())
    lines_queue: asyncio.Queue = asyncio.Queue()
    done_event: asyncio.Event = asyncio.Event()
    status_event: asyncio.Event = asyncio.Event()
    picked_up_event: asyncio.Event = asyncio.Event()

    proxy_url = _normalize_userscript_proxy_url(str(url))
    if not proxy_url:
        raise HTTPException(
            status_code=400,
            detail="Userscript proxy only supports https://arena.ai and https://lmarena.ai URLs.",
        )

    safe_headers: dict[str, str] = {}
    for key, value in (headers or {}).items():
        if not key:
            continue
        header_name = str(key)
        if header_name.lower() == "x-lmbridge-secret":
            continue
        if value is None:
            continue
        safe_headers[header_name] = str(value)
    if "Content-Type" not in safe_headers:
        safe_headers["Content-Type"] = "text/plain;charset=UTF-8"
    job = {
        "created_at": _m().time.time(),
        "job_id": job_id,
        # Job lifecycle markers used by the server-side stream handler to apply timeouts correctly.
        # - phase: queued -> picked_up -> signup -> fetch
        # - picked_up_at_monotonic: set when any proxy worker/poller claims the job
        # - upstream_started_at_monotonic: set when the proxy begins processing the request (may include preflight)
        # - upstream_fetch_started_at_monotonic: set when the upstream HTTP fetch is initiated (after preflight)
        "phase": "queued",
        "picked_up_at_monotonic": None,
        "upstream_started_at_monotonic": None,
        "upstream_fetch_started_at_monotonic": None,
        "url": str(url),
        "method": str(http_method or "POST"),
        "payload": {
            "url": proxy_url,
            "method": str(http_method or "POST"),
            "headers": safe_headers,
            "body": json.dumps(payload) if (payload is not None and str(http_method).upper() not in {"GET", "HEAD"}) else "",
        },
        "lines_queue": lines_queue,
        "done_event": done_event,
        "status_event": status_event,
        "picked_up_event": picked_up_event,
        "done": False,
        "status_code": 200,
        "headers": {},
        "body_preview": "",
        "body_preview_content_type": "",
        "error": None,
    }
    _m()._USERSCRIPT_PROXY_JOBS[job_id] = job
    _m().debug_print(f"📫 Enqueued userscript proxy job {job_id[:8]} -> {job['payload'].get('method')} {proxy_url}")
    await _get_userscript_proxy_queue().put(job_id)
    return UserscriptProxyStreamResponse(job_id, timeout_seconds=timeout_seconds)


async def _wait_for_userscript_pickup(job_id: str, timeout_seconds: int) -> bool:
    job = _m()._USERSCRIPT_PROXY_JOBS.get(str(job_id))
    if not isinstance(job, dict):
        return False
    picked = job.get("picked_up_event")
    if isinstance(picked, asyncio.Event) and picked.is_set():
        return True
    if not isinstance(picked, asyncio.Event):
        return True
    try:
        await asyncio.wait_for(picked.wait(), timeout=max(1, int(timeout_seconds)))
        return True
    except Exception:
        await _finalize_userscript_proxy_job(
            job_id,
            error="userscript proxy did not pick up job in time",
            remove=False,
        )
        return False


async def fetch_via_proxy_queue(
    url: str,
    payload: object,
    http_method: str = "POST",
    timeout_seconds: int = 120,
    streaming: bool = False,
    headers: Optional[dict] = None,
) -> Optional[object]:
    """
    Fallback transport: delegates the request to a connected Userscript via the Task Queue.
    """
    # Prefer the streaming-capable proxy endpoints when available.
    proxy_stream = await _m().fetch_lmarena_stream_via_userscript_proxy(
        http_method=http_method,
        url=url,
        payload=payload if payload is not None else {},
        timeout_seconds=timeout_seconds,
        headers=headers,
    )
    if proxy_stream is None:
        return None
    job_id = str(getattr(proxy_stream, "job_id", "") or "")
    pickup_timeout = DEFAULT_PICKUP_TIMEOUT_SECONDS
    try:
        pickup_timeout = int(
            (_m().get_config() or {}).get(
                "userscript_proxy_pickup_timeout_seconds",
                DEFAULT_PICKUP_TIMEOUT_SECONDS,
            )
        )
    except Exception:
        pickup_timeout = DEFAULT_PICKUP_TIMEOUT_SECONDS
    pickup_timeout = max(3, min(pickup_timeout, 60))
    if job_id:
        picked = await _wait_for_userscript_pickup(job_id, pickup_timeout)
        if not picked:
            return None

    if streaming:
        return proxy_stream

    # Non-streaming call: buffer everything and return a plain response wrapper.
    collected_lines: list[str] = []
    async with proxy_stream as response:
        async for line in response.aiter_lines():
            collected_lines.append(str(line))

    job_error = ""
    try:
        job = _m()._USERSCRIPT_PROXY_JOBS.get(getattr(proxy_stream, "job_id", ""))
        if isinstance(job, dict) and job.get("error"):
            job_error = str(job.get("error") or "")
    except Exception:
        job_error = ""

    body_text = "\n".join(collected_lines)
    if job_error and not body_text:
        body_text = job_error

    status_code = getattr(proxy_stream, "status_code", 200)
    if job_error:
        status_code = HTTPStatus.SERVICE_UNAVAILABLE

    return BrowserFetchStreamResponse(
        status_code=status_code,
        headers=getattr(proxy_stream, "headers", {}),
        text=body_text,
        method=http_method,
        url=url,
        job_id=job_id,
    )
