import asyncio
import builtins as _builtins
import json
import os
import re
import shutil
import sys
import uuid
import time
import secrets
import base64
import mimetypes
import hashlib
from collections import defaultdict
from contextlib import asynccontextmanager, AsyncExitStack
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime, timezone, timedelta
from urllib.parse import urlsplit, urlparse, parse_qs

import uvicorn
from fastapi import FastAPI, HTTPException, Depends, status, Form, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.security import APIKeyHeader

# Import from modularized modules
from . import constants
from . import config as _config_module
from .config import get_models, save_models

from .transport import (
    _touch_userscript_poll,
    _get_userscript_proxy_queue,
    _userscript_proxy_is_active,
    _userscript_proxy_check_secret,
    _cleanup_userscript_proxy_jobs,
    _mark_userscript_proxy_inactive,
    _finalize_userscript_proxy_job,
    _normalize_userscript_proxy_url,
    fetch_lmarena_stream_via_userscript_proxy,
    fetch_via_proxy_queue,
)

# Aliases for backward compatibility
DEBUG = constants.DEBUG
PORT = constants.PORT
HTTPStatus = constants.HTTPStatus
STATUS_MESSAGES = constants.STATUS_MESSAGES
MAX_IMAGE_SIZE_BYTES = constants.MAX_IMAGE_SIZE_BYTES
SUPPORTED_IMAGE_MIME_TYPES = constants.SUPPORTED_IMAGE_MIME_TYPES

USERSCRIPT_PROXY_REQUIRED_MESSAGE = (
    "Userscript proxy is required. Start Firefox, load the extension, "
    "open an arena.ai/lmarena.ai tab, then retry."
)

# Backoff functions
def get_rate_limit_sleep_seconds(retry_after: Optional[str], attempt: int) -> int:
    return constants.get_rate_limit_backoff_seconds(retry_after, attempt)

def get_general_backoff_seconds(attempt: int) -> int:
    return constants.get_general_backoff_seconds(attempt)


def safe_print(*args, **kwargs) -> None:
    """
    Print without crashing on Windows console encoding issues (e.g., GBK can't encode emoji).
    This must never raise, because it's used inside request handlers/streaming generators.
    """
    try:
        _builtins.print(*args, **kwargs)
    except UnicodeEncodeError:
        file = kwargs.get("file") or sys.stdout
        sep = kwargs.get("sep", " ")
        end = kwargs.get("end", "\n")
        flush = bool(kwargs.get("flush", False))

        try:
            text = sep.join(str(a) for a in args) + end
            encoding = getattr(file, "encoding", None) or getattr(sys.stdout, "encoding", None) or "utf-8"
            safe_text = text.encode(encoding, errors="backslashreplace").decode(encoding, errors="ignore")
            file.write(safe_text)
            if flush:
                try:
                    file.flush()
                except Exception:
                    pass
        except Exception:
            return


# Ensure all module-level `print(...)` calls are resilient to Windows console encoding issues.
# (Some environments default to GBK, which cannot encode emoji.)
print = safe_print  # type: ignore[assignment]


def debug_print(*args, **kwargs):
    """Print debug messages only if DEBUG is True"""
    if DEBUG:
        print(*args, **kwargs)


def get_status_emoji(status_code: int) -> str:
    if 200 <= status_code < 300:
        return "✅"
    elif 300 <= status_code < 400:
        return "↪️"
    elif 400 <= status_code < 500:
        if status_code == 401:
            return "🔒"
        elif status_code == 403:
            return "🚫"
        elif status_code == 404:
            return "❓"
        elif status_code == 429:
            return "⏱️"
        return "⚠️"
    elif 500 <= status_code < 600:
        return "❌"
    return "ℹ️"


def log_http_status(status_code: int, context: str = "") -> None:
    emoji = get_status_emoji(status_code)
    message = STATUS_MESSAGES.get(status_code, f"Unknown Status {status_code}")
    if context:
        debug_print(f"{emoji} HTTP {status_code}: {message} ({context})")
    else:
        debug_print(f"{emoji} HTTP {status_code}: {message}")


def _extract_header(headers: Optional[dict], header_name: str) -> str:
    if not isinstance(headers, dict):
        return ""
    needle = header_name.lower()
    for key, value in headers.items():
        if str(key).lower() == needle:
            return str(value or "").strip()
    return ""


def _format_upstream_diagnostic(
    status_code: int,
    *,
    headers: Optional[dict] = None,
    body_text: Optional[str] = None,
    job: Optional[dict] = None,
    preview_limit: int = 300,
) -> str:
    content_type = _extract_header(headers, "content-type")
    if not content_type and isinstance(job, dict):
        content_type = str(job.get("body_preview_content_type") or "").strip()
    preview = ""
    if isinstance(job, dict):
        preview = str(job.get("body_preview") or "")
    if not preview:
        preview = str(body_text or "")
    preview = preview.strip().replace("\r\n", "\n").replace("\r", "\n").replace("\n", " ")
    if preview_limit > 0 and len(preview) > preview_limit:
        preview = preview[:preview_limit].rstrip() + "..."
    parts = [f"Upstream HTTP {status_code}."]
    if content_type:
        parts.append(f"content-type={content_type}.")
    if preview:
        parts.append(f"body-preview={preview}")
    return " ".join(parts).strip()



STREAM_CREATE_EVALUATION_PATH = "/nextjs-api/stream/create-evaluation"


USERSCRIPT_PROXY_LAST_POLL_AT: float = 0.0
_USERSCRIPT_PROXY_QUEUE: Optional[asyncio.Queue] = None
_USERSCRIPT_PROXY_JOBS: dict[str, dict] = {}


# Custom UUIDv7 implementation (using correct Unix epoch)
def uuid7():
    """
    Generate a UUIDv7 using Unix epoch (milliseconds since 1970-01-01)
    matching the browser's implementation.
    """
    timestamp_ms = int(time.time() * 1000)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    
    uuid_int = timestamp_ms << 80
    uuid_int |= (0x7000 | rand_a) << 64
    uuid_int |= (0x8000000000000000 | rand_b)
    
    hex_str = f"{uuid_int:032x}"
    return f"{hex_str[0:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"

def _get_signed_url_expiry(url: str) -> Optional[float]:
    """Extract expiry timestamp from an S3/R2 signed URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        # S3/R2 standard headers for signed URLs
        date_str = params.get("X-Amz-Date", [None])[0]
        expires_str = params.get("X-Amz-Expires", [None])[0]
        if not date_str or not expires_str:
            return None
        
        # Parse timestamp format: 20260425T071405Z
        dt = datetime.strptime(date_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        return dt.timestamp() + int(expires_str)
    except (ValueError, TypeError, IndexError, AttributeError):
        return None

def check_link_expiry(url: str) -> bool:
    """Check if an S3/R2 signed URL is still valid based on its query params."""
    expiry_ts = _get_signed_url_expiry(url)
    if expiry_ts is None:
        return False
    # Return True if current time is before expiry (with 60s safety buffer)
    return time.time() < (expiry_ts - 60)

# Image upload helper functions
async def upload_image_to_lmarena(image_data: bytes, mime_type: str, filename: str) -> Optional[tuple]:
    """
    Upload an image to LMArena R2 storage and return the key and download URL.
    Uses MD5 caching to avoid re-uploading the same image.
    """
    raise HTTPException(
        status_code=503,
        detail=(
            "Image upload is unavailable in proxy-only mode. "
            "Refresh models via the userscript proxy to restore this feature."
        ),
    )

def _coerce_message_content_to_text(content) -> str:
    """Best-effort coercion of message content to plain text (no images)."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for part in content:
            if isinstance(part, dict):
                if part.get("type") == "text":
                    parts.append(str(part.get("text", "")))
                elif "text" in part:
                    parts.append(str(part.get("text", "")))
                elif "content" in part:
                    parts.append(str(part.get("content", "")))
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join([p for p in parts if p is not None]).strip()
    return str(content)


async def process_message_content(content, model_capabilities: dict) -> tuple[str, List[dict]]:
    """
    Process message content, handle images if present and model supports them.
    
    Args:
        content: Message content (string or list of content parts)
        model_capabilities: Model's capability dictionary
    
    Returns:
        Tuple of (text_content, experimental_attachments)
    """
    # Check if model supports image input
    supports_images = model_capabilities.get('inputCapabilities', {}).get('image', False)
    
    # If content is a string, return it as-is
    if isinstance(content, str):
        return content, []
    
    # If content is a list (OpenAI format with multiple parts)
    if isinstance(content, list):
        text_parts = []
        attachments = []
        
        for part in content:
            if isinstance(part, dict):
                if part.get('type') == 'text':
                    text_parts.append(part.get('text', ''))
                elif 'text' in part:
                    text_parts.append(part.get('text', ''))
                elif 'content' in part:
                    text_parts.append(part.get('content', ''))
                    
                elif part.get('type') == 'image_url' and supports_images:
                    raise HTTPException(
                        status_code=503,
                        detail=(
                            "Image inputs are unavailable in proxy-only mode. "
                            "Refresh models via the userscript proxy to restore this feature."
                        ),
                    )
                elif part.get('type') == 'image_url' and not supports_images:
                    debug_print(f"⚠️  Image provided but model doesn't support images")
            elif isinstance(part, str):
                text_parts.append(part)
        
        # Combine text parts
        text_content = '\n'.join(text_parts).strip()
        return text_content, attachments
    
    # Fallback
    return str(content), []

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await startup_event()
    except Exception as e:
        debug_print(f"❌ Error during startup: {e}")
    yield

app = FastAPI(lifespan=lifespan)

# Add CORS middleware to handle preflight requests and avoid 405 errors
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https?://.*",  # WARNING: For development only. Restrict to specific origins in production.
    allow_credentials=True,
    allow_methods=["*"],  # This includes GET, POST, PUT, DELETE, OPTIONS, etc.
    allow_headers=["*"],
)

# --- Constants & Global State ---
CONFIG_FILE = constants.CONFIG_FILE
MODELS_FILE = constants.MODELS_FILE
API_KEY_HEADER = APIKeyHeader(name="Authorization", auto_error=False)

# In-memory stores
# { "api_key": { "conversation_id": session_data } }
chat_sessions: Dict[str, Dict[str, dict]] = defaultdict(dict)
# { "session_id": "username" }
dashboard_sessions = {}
# { "api_key": [timestamp1, timestamp2, ...] }
api_key_usage = defaultdict(list)
# { "model_id": count }
model_usage_stats = defaultdict(int)
# Token cycling: current index for round-robin selection
current_token_index = 0
# Track config file path changes to reset per-config state in tests/dev.
_LAST_CONFIG_FILE: Optional[str] = None

# --- Helper Functions ---

def get_config():
    global current_token_index, _LAST_CONFIG_FILE
    # If tests or callers swap CONFIG_FILE at runtime, reset the token round-robin index so token selection
    # is deterministic per config file.
    if _LAST_CONFIG_FILE != CONFIG_FILE:
        _LAST_CONFIG_FILE = CONFIG_FILE
        current_token_index = 0
    try:
        with open(CONFIG_FILE, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        debug_print(f"⚠️  Config file error: {e}, using defaults")
        config = {}
    except Exception as e:
        debug_print(f"⚠️  Unexpected error reading config: {e}, using defaults")
        config = {}

    # Ensure default keys exist
    try:
        _config_module._apply_config_defaults(config)
    except Exception as e:
        debug_print(f"⚠️  Error setting config defaults: {e}")

    return config


def load_usage_stats():
    """Load usage stats from config into memory"""
    global model_usage_stats
    try:
        config = get_config()
        model_usage_stats = defaultdict(int, config.get("usage_stats", {}))
    except Exception as e:
        debug_print(f"⚠️  Error loading usage stats: {e}, using empty stats")
        model_usage_stats = defaultdict(int)

def save_config(config) -> None:
    try:
        # Persist in-memory stats to the config dict before saving
        config["usage_stats"] = dict(model_usage_stats)
        tmp_path = f"{CONFIG_FILE}.tmp"
        with open(tmp_path, "w") as f:
            json.dump(config, f, indent=4)
        os.replace(tmp_path, CONFIG_FILE)
    except Exception as e:
        debug_print(f"❌ Error saving config: {e}")

# --- Dashboard Authentication ---

async def get_current_session(request: Request):
    session_id = request.cookies.get("session_id")
    if session_id and session_id in dashboard_sessions:
        return dashboard_sessions[session_id]
    return None

# --- API Key Authentication & Rate Limiting ---

async def rate_limit_api_key(key: str = Depends(API_KEY_HEADER)):
    config = get_config()
    api_keys = config.get("api_keys", [])

    api_key_str = None
    if key and key.startswith("Bearer "):
        api_key_str = key[7:].strip()

    # If no API keys configured, allow anonymous access (optional auth)
    if not api_keys:
        return {"key": "anonymous", "name": "Anonymous", "rpm": 9999}

    # If keys are configured but none provided, use first available key
    if not api_key_str:
        api_key_str = api_keys[0]["key"]

    key_data = next((k for k in api_keys if k["key"] == api_key_str), None)
    if not key_data:
        raise HTTPException(status_code=401, detail="Invalid API Key.")

    # Rate Limiting
    rate_limit = key_data.get("rpm", 60)
    current_time = time.time()

    # Clean up old timestamps (older than 60 seconds)
    api_key_usage[api_key_str] = [t for t in api_key_usage[api_key_str] if current_time - t < 60]

    if len(api_key_usage[api_key_str]) >= rate_limit:
        # Calculate seconds until oldest request expires (60 seconds window)
        oldest_timestamp = min(api_key_usage[api_key_str])
        retry_after = int(60 - (current_time - oldest_timestamp))
        retry_after = max(1, retry_after)  # At least 1 second

        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later.",
            headers={"Retry-After": str(retry_after)}
        )

    api_key_usage[api_key_str].append(current_time)

    return key_data

# --- Core Logic ---

async def refresh_models_via_proxy(*, require_active: bool = True) -> list:
    """Refresh the cached models list via the userscript proxy."""
    if require_active and not _userscript_proxy_is_active():
        raise HTTPException(status_code=503, detail=USERSCRIPT_PROXY_REQUIRED_MESSAGE)

    debug_print("🔄 Refreshing models via userscript proxy...")
    headers = {"Accept": "text/html"}
    response = await fetch_via_proxy_queue(
        url="https://arena.ai/?mode=direct",
        payload="",
        http_method="GET",
        timeout_seconds=60,
        headers=headers,
    )
    if response is None:
        raise HTTPException(status_code=503, detail="Userscript proxy request failed or timed out.")

    body_bytes = await response.aread()
    body_text = body_bytes.decode("utf-8", errors="replace")
    if response.status_code >= 400:
        raise HTTPException(
            status_code=503,
            detail=(
                "Userscript proxy failed to fetch models. "
                "Complete any challenges in the Arena tab and retry."
            ),
        )

    match = re.search(r'{\"initialModels\":(\[.*?\]),\"initialModel[A-Z]Id', body_text, re.DOTALL)
    if not match:
        raise HTTPException(
            status_code=503,
            detail="Models not found in Arena response. Ensure the Arena tab is loaded and retry.",
        )
    models_json = match.group(1).encode().decode("unicode_escape")
    models = json.loads(models_json)
    save_models(models)
    debug_print(f"✅ Saved {len(models)} models")
    return models


async def startup_event():
    # Prevent unit tests (TestClient/ASGITransport) from clobbering the user's real config.json
    # and running slow browser/network startup routines.
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return

    try:
        # Ensure config and models files exist
        config = get_config()
        if not config.get("api_keys"):
            config["api_keys"] = [
                {
                    "name": "Default Key",
                    "key": f"sk-lmab-{uuid.uuid4()}",
                    "rpm": 60,
                    "created": int(time.time()),
                }
            ]
        save_config(config)
        save_models(get_models())
        # Load usage stats from config
        load_usage_stats()

        # Start without any browser automation. Models are loaded from cache if present.
        cached_models = get_models()
        if not cached_models:
            debug_print("⚠️ Models cache is empty. Use the dashboard to refresh via userscript proxy.")
        
    except Exception as e:
        debug_print(f"❌ Error during startup: {e}")
        # Continue anyway - server should still start

# --- UI Endpoints (Login/Dashboard) ---

@app.get("/", response_class=HTMLResponse)
async def root_redirect():
    return RedirectResponse(url="/dashboard")

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: Optional[str] = None):
    if await get_current_session(request):
        return RedirectResponse(url="/dashboard")
    
    error_msg = '<div class="error-message">Invalid password. Please try again.</div>' if error else ''
    
    return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Login - LMArena Bridge</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    min-height: 100vh;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    padding: 20px;
                }}
                .login-container {{
                    background: white;
                    padding: 40px;
                    border-radius: 10px;
                    box-shadow: 0 10px 40px rgba(0,0,0,0.2);
                    width: 100%;
                    max-width: 400px;
                }}
                h1 {{
                    color: #333;
                    margin-bottom: 10px;
                    font-size: 28px;
                }}
                .subtitle {{
                    color: #666;
                    margin-bottom: 30px;
                    font-size: 14px;
                }}
                .form-group {{
                    margin-bottom: 20px;
                }}
                label {{
                    display: block;
                    margin-bottom: 8px;
                    color: #555;
                    font-weight: 500;
                }}
                input[type="password"] {{
                    width: 100%;
                    padding: 12px;
                    border: 2px solid #e1e8ed;
                    border-radius: 6px;
                    font-size: 16px;
                    transition: border-color 0.3s;
                }}
                input[type="password"]:focus {{
                    outline: none;
                    border-color: #667eea;
                }}
                button {{
                    width: 100%;
                    padding: 12px;
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    border: none;
                    border-radius: 6px;
                    font-size: 16px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: transform 0.2s;
                }}
                button:hover {{
                    transform: translateY(-2px);
                }}
                button:active {{
                    transform: translateY(0);
                }}
                .error-message {{
                    background: #fee;
                    color: #c33;
                    padding: 12px;
                    border-radius: 6px;
                    margin-bottom: 20px;
                    border-left: 4px solid #c33;
                .error-message {{
                    background: #fee;
                    color: #c33;
                    padding: 12px;
                    border-radius: 6px;
                    margin-bottom: 20px;
                    border-left: 4px solid #c33;
                }}
                .password-hint {{
                    font-size: 12px;
                    color: #888;
                    margin-top: 8px;
                }}
                .password-hint code {{
                    background: #f5f5f5;
                    padding: 2px 6px;
                    border-radius: 4px;
                    font-family: monospace;
                }}
            </style>
        </head>
        <body>
            <div class="login-container">
                <h1>LMArena Bridge</h1>
                <div class="subtitle">Sign in to access the dashboard</div>
                {error_msg}
                <form action="/login" method="post">
                    <div class="form-group">
                        <label for="password">Password</label>
                        <input type="password" id="password" name="password" placeholder="Enter your password" required autofocus>
                    <div class="form-group">
                        <label for="password">Password</label>
                        <input type="password" id="password" name="password" placeholder="Enter your password" required autofocus>
                        <div class="password-hint">Default password: <code>admin</code></div>
                    </div>
                    <button type="submit">Sign In</button>
                </form>
            </div>
        </body>
        </html>
    """

@app.post("/login")
async def login_submit(response: Response, password: str = Form(...)):
    config = get_config()
    if password == config.get("password"):
        session_id = str(uuid.uuid4())
        dashboard_sessions[session_id] = "admin"
        response = RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)
        response.set_cookie(key="session_id", value=session_id, httponly=True)
        return response
    return RedirectResponse(url="/login?error=1", status_code=status.HTTP_303_SEE_OTHER)

@app.get("/logout")
async def logout(request: Request, response: Response):
    session_id = request.cookies.get("session_id")
    if session_id in dashboard_sessions:
        del dashboard_sessions[session_id]
    response = RedirectResponse(url="/login", status_code=status.HTTP_303_SEE_OTHER)
    response.delete_cookie("session_id")
    return response

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(session: str = Depends(get_current_session)):
    if not session:
        return RedirectResponse(url="/login")

    try:
        config = get_config()
        models = get_models()
    except Exception as e:
        debug_print(f"❌ Error loading dashboard data: {e}")
        # Return error page
        return HTMLResponse(f"""
            <html><body style="font-family: sans-serif; padding: 40px; text-align: center;">
                <h1>⚠️ Dashboard Error</h1>
                <p>Failed to load configuration: {str(e)}</p>
                <p><a href="/logout">Logout</a> | <a href="/dashboard">Retry</a></p>
            </body></html>
        """, status_code=500)

    # Render API Keys
    keys_html = ""
    for key in config["api_keys"]:
        key_name = key.get("name") or "Unnamed Key"
        key_value = key.get("key") or ""
        rpm_value = key.get("rpm", 60)
        created_date = time.strftime('%Y-%m-%d %H:%M', time.localtime(key.get('created', 0)))
        keys_html += f"""
            <tr>
                <td><strong>{key_name}</strong></td>
                <td><code class="api-key-code">{key_value}</code></td>
                <td><span class="badge">{rpm_value} RPM</span></td>
                <td><small>{created_date}</small></td>
                <td>
                    <form action='/delete-key' method='post' style='margin:0;' onsubmit='return confirm("Delete this API key?");'>
                        <input type='hidden' name='key_id' value='{key_value}'>
                        <button type='submit' class='btn-delete'>Delete</button>
                    </form>
                </td>
            </tr>
        """

    # Render Models (limit to first 20 with text output)
    text_models = [m for m in models if m.get('capabilities', {}).get('outputCapabilities', {}).get('text')]
    models_html = ""
    for i, model in enumerate(text_models[:20]):
        rank = model.get('rank', '?')
        org = model.get('organization', 'Unknown')
        models_html += f"""
            <div class="model-card">
                <div class="model-header">
                    <span class="model-name">{model.get('publicName', 'Unnamed')}</span>
                    <span class="model-rank">Rank {rank}</span>
                </div>
                <div class="model-org">{org}</div>
            </div>
        """
    
    if not models_html:
        models_html = '<div class="no-data">No models cached. Refresh models via the userscript proxy.</div>'

    proxy_active = _userscript_proxy_is_active(config)
    proxy_status_label = "Active" if proxy_active else "Inactive"
    proxy_status_class = "status-good" if proxy_active else "status-bad"
    last_poll = max(float(USERSCRIPT_PROXY_LAST_POLL_AT or 0.0), float(last_userscript_poll or 0.0))
    if last_poll <= 0:
        proxy_age_display = "Never"
    else:
        proxy_age_seconds = max(0, int(time.time() - last_poll))
        proxy_age_display = f"{proxy_age_seconds}s ago"
    try:
        proxy_queue_size = int(_get_userscript_proxy_queue().qsize())
    except Exception:
        proxy_queue_size = 0
    proxy_pending_jobs = 0
    for job in _USERSCRIPT_PROXY_JOBS.values():
        if isinstance(job, dict) and not job.get("done"):
            proxy_pending_jobs += 1

    # Render Stats
    stats_html = ""
    if model_usage_stats:
        for model, count in sorted(model_usage_stats.items(), key=lambda x: x[1], reverse=True)[:10]:
            stats_html += f"<tr><td>{model}</td><td><strong>{count}</strong></td></tr>"
    else:
        stats_html = "<tr><td colspan='2' class='no-data'>No usage data yet</td></tr>"

    # Get recent activity count (last 24 hours)
    recent_activity = sum(1 for timestamps in api_key_usage.values() for t in timestamps if time.time() - t < 86400)

    return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Dashboard - LMArena Bridge</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>
            <style>
                @keyframes fadeIn {{
                    from {{ opacity: 0; transform: translateY(20px); }}
                    to {{ opacity: 1; transform: translateY(0); }}
                }}
                @keyframes slideIn {{
                    from {{ opacity: 0; transform: translateX(-20px); }}
                    to {{ opacity: 1; transform: translateX(0); }}
                }}
                @keyframes pulse {{
                    0%, 100% {{ transform: scale(1); }}
                    50% {{ transform: scale(1.05); }}
                }}
                @keyframes shimmer {{
                    0% {{ background-position: -1000px 0; }}
                    100% {{ background-position: 1000px 0; }}
                }}
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
                    background: #f5f7fa;
                    color: #333;
                    line-height: 1.6;
                }}
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px 0;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }}
                .header-content {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 20px;
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                }}
                h1 {{
                    font-size: 24px;
                    font-weight: 600;
                }}
                .logout-btn {{
                    background: rgba(255,255,255,0.2);
                    color: white;
                    padding: 8px 16px;
                    border-radius: 6px;
                    text-decoration: none;
                    transition: background 0.3s;
                }}
                .logout-btn:hover {{
                    background: rgba(255,255,255,0.3);
                }}
                .container {{
                    max-width: 1200px;
                    margin: 30px auto;
                    padding: 0 20px;
                }}
                .section {{
                    background: white;
                    border-radius: 10px;
                    padding: 25px;
                    margin-bottom: 25px;
                    box-shadow: 0 2px 8px rgba(0,0,0,0.05);
                }}
                .section-header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 20px;
                    padding-bottom: 15px;
                    border-bottom: 2px solid #f0f0f0;
                }}
                h2 {{
                    font-size: 20px;
                    color: #333;
                    font-weight: 600;
                }}
                .status-badge {{
                    padding: 6px 12px;
                    border-radius: 6px;
                    font-size: 13px;
                    font-weight: 600;
                }}
                .status-good {{ background: #d4edda; color: #155724; }}
                .status-bad {{ background: #f8d7da; color: #721c24; }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                th {{
                    background: #f8f9fa;
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                    color: #555;
                    font-size: 14px;
                    border-bottom: 2px solid #e9ecef;
                }}
                td {{
                    padding: 12px;
                    border-bottom: 1px solid #f0f0f0;
                }}
                tr:hover {{
                    background: #f8f9fa;
                }}
                .form-group {{
                    margin-bottom: 15px;
                }}
                label {{
                    display: block;
                    margin-bottom: 6px;
                    font-weight: 500;
                    color: #555;
                }}
                input[type="text"], input[type="number"], textarea {{
                    width: 100%;
                    padding: 10px;
                    border: 2px solid #e1e8ed;
                    border-radius: 6px;
                    font-size: 14px;
                    font-family: inherit;
                    transition: border-color 0.3s;
                }}
                input:focus, textarea:focus {{
                    outline: none;
                    border-color: #667eea;
                }}
                textarea {{
                    resize: vertical;
                    font-family: 'Courier New', monospace;
                    min-height: 100px;
                }}
                button, .btn {{
                    padding: 10px 20px;
                    border: none;
                    border-radius: 6px;
                    font-size: 14px;
                    font-weight: 600;
                    cursor: pointer;
                    transition: all 0.3s;
                }}
                button[type="submit"]:not(.btn-delete) {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                }}
                button[type="submit"]:not(.btn-delete):hover {{
                    transform: translateY(-2px);
                    box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
                }}
                .btn-delete {{
                    background: #dc3545;
                    color: white;
                    padding: 6px 12px;
                    font-size: 13px;
                }}
                .btn-delete:hover {{
                    background: #c82333;
                }}
                .api-key-code {{
                    background: #f8f9fa;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-family: 'Courier New', monospace;
                    font-size: 12px;
                    color: #495057;
                }}
                .badge {{
                    background: #e7f3ff;
                    color: #0066cc;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-size: 12px;
                    font-weight: 600;
                }}
                .model-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
                    gap: 15px;
                    margin-top: 15px;
                }}
                .model-card {{
                    background: #f8f9fa;
                    padding: 15px;
                    border-radius: 8px;
                    border-left: 4px solid #667eea;
                }}
                .model-header {{
                    display: flex;
                    justify-content: space-between;
                    align-items: center;
                    margin-bottom: 8px;
                }}
                .model-name {{
                    font-weight: 600;
                    color: #333;
                    font-size: 14px;
                }}
                .model-rank {{
                    background: #667eea;
                    color: white;
                    padding: 2px 8px;
                    border-radius: 12px;
                    font-size: 11px;
                    font-weight: 600;
                }}
                .model-org {{
                    color: #666;
                    font-size: 12px;
                }}
                .no-data {{
                    text-align: center;
                    color: #999;
                    padding: 20px;
                    font-style: italic;
                }}
                .stats-grid {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
                    gap: 20px;
                    margin-bottom: 20px;
                }}
                .stat-card {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 8px;
                    text-align: center;
                    animation: fadeIn 0.6s ease-out;
                    transition: transform 0.3s;
                }}
                .stat-card:hover {{
                    transform: translateY(-5px);
                    box-shadow: 0 8px 16px rgba(102, 126, 234, 0.4);
                }}
                .section {{
                    animation: slideIn 0.5s ease-out;
                }}
                .section:nth-child(2) {{ animation-delay: 0.1s; }}
                .section:nth-child(3) {{ animation-delay: 0.2s; }}
                .section:nth-child(4) {{ animation-delay: 0.3s; }}
                .model-card {{
                    animation: fadeIn 0.4s ease-out;
                    transition: transform 0.2s, box-shadow 0.2s;
                }}
                .model-card:hover {{
                    transform: translateY(-3px);
                    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
                }}
                .stat-value {{
                    font-size: 32px;
                    font-weight: bold;
                    margin-bottom: 5px;
                }}
                .stat-label {{
                    font-size: 14px;
                    opacity: 0.9;
                }}
                .form-row {{
                    display: grid;
                    grid-template-columns: 2fr 1fr auto;
                    gap: 10px;
                    align-items: end;
                }}
                @media (max-width: 768px) {{
                    .form-row {{
                        grid-template-columns: 1fr;
                    }}
                    .model-grid {{
                        grid-template-columns: 1fr;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <div class="header-content">
                    <h1>🚀 LMArena Bridge Dashboard</h1>
                    <a href="/logout" class="logout-btn">Logout</a>
                </div>
            </div>

            <div class="container">
                <!-- Stats Overview -->
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-value">{len(config['api_keys'])}</div>
                        <div class="stat-label">API Keys</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{len(text_models)}</div>
                        <div class="stat-label">Available Models</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-value">{sum(model_usage_stats.values())}</div>
                        <div class="stat-label">Total Requests</div>
                    </div>
                </div>

                <!-- Userscript Proxy -->
                <div class="section">
                    <div class="section-header">
                        <h2>🦊 Userscript Proxy</h2>
                        <span class="status-badge {proxy_status_class}">{proxy_status_label}</span>
                    </div>
                    <p style="color: #666; margin-bottom: 15px;">
                        All upstream Arena requests are executed by Firefox with the included extension.
                        Ensure the extension is running and an arena.ai/lmarena.ai tab is open.
                    </p>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 15px;">
                        <div style="background: #f8f9fa; padding: 12px; border-radius: 6px;">
                            <strong>Last Poll</strong><br>
                            <span style="color: #666;">{proxy_age_display}</span>
                        </div>
                        <div style="background: #f8f9fa; padding: 12px; border-radius: 6px;">
                            <strong>Queue Size</strong><br>
                            <span style="color: #666;">{proxy_queue_size}</span>
                        </div>
                        <div style="background: #f8f9fa; padding: 12px; border-radius: 6px;">
                            <strong>Pending Jobs</strong><br>
                            <span style="color: #666;">{proxy_pending_jobs}</span>
                        </div>
                    </div>
                </div>

                <!-- Models Cache -->
                <div class="section">
                    <div class="section-header">
                        <h2>📦 Models Cache</h2>
                    </div>
                    <p style="color: #666; margin-bottom: 15px;">
                        Models are loaded from the local cache file. Refresh requires the userscript proxy to be active.
                    </p>
                    <p style="margin-bottom: 10px;"><strong>Cached models:</strong> {len(models)}</p>
                    <form action="/refresh-models" method="post" style="margin-top: 15px;">
                        <button type="submit" style="background: #28a745;">🔄 Refresh Models via Proxy</button>
                    </form>
                </div>

                <!-- API Keys -->
                <div class="section">
                    <div class="section-header">
                        <h2>🔑 API Keys</h2>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Name</th>
                                <th>Key</th>
                                <th>Rate Limit</th>
                                <th>Created</th>
                                <th>Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {keys_html if keys_html else '<tr><td colspan="5" class="no-data">No API keys configured</td></tr>'}
                        </tbody>
                    </table>
                    
                    <h3 style="margin-top: 30px; margin-bottom: 15px; font-size: 18px;">Create New API Key</h3>
                    <form action="/create-key" method="post">
                        <div class="form-row">
                            <div class="form-group">
                                <label for="name">Key Name</label>
                                <input type="text" id="name" name="name" placeholder="e.g., Production Key" required>
                            </div>
                            <div class="form-group">
                                <label for="rpm">Rate Limit (RPM)</label>
                                <input type="number" id="rpm" name="rpm" value="60" min="1" max="1000" required>
                            </div>
                            <div class="form-group">
                                <label>&nbsp;</label>
                                <button type="submit">Create Key</button>
                            </div>
                        </div>
                    </form>
                </div>

                <!-- Usage Statistics -->
                <div class="section">
                    <div class="section-header">
                        <h2>📊 Usage Statistics</h2>
                    </div>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 30px;">
                        <div>
                            <h3 style="text-align: center; margin-bottom: 15px; font-size: 16px; color: #666;">Model Usage Distribution</h3>
                            <canvas id="modelPieChart" style="max-height: 300px;"></canvas>
                        </div>
                        <div>
                            <h3 style="text-align: center; margin-bottom: 15px; font-size: 16px; color: #666;">Request Count by Model</h3>
                            <canvas id="modelBarChart" style="max-height: 300px;"></canvas>
                        </div>
                    </div>
                    <table>
                        <thead>
                            <tr>
                                <th>Model</th>
                                <th>Requests</th>
                            </tr>
                        </thead>
                        <tbody>
                            {stats_html}
                        </tbody>
                    </table>
                </div>

                <!-- Available Models -->
                <div class="section">
                    <div class="section-header">
                        <h2>🤖 Available Models</h2>
                    </div>
                    <p style="color: #666; margin-bottom: 15px;">Showing top 20 text-based models (Rank 1 = Best)</p>
                    <div class="model-grid">
                        {models_html}
                    </div>
                </div>
            </div>
            
            <script>
                // Prepare data for charts
                const statsData = {json.dumps(dict(sorted(model_usage_stats.items(), key=lambda x: x[1], reverse=True)[:10]))};
                const modelNames = Object.keys(statsData);
                const modelCounts = Object.values(statsData);
                
                // Generate colors for charts
                const colors = [
                    '#667eea', '#764ba2', '#f093fb', '#4facfe',
                    '#43e97b', '#fa709a', '#fee140', '#30cfd0',
                    '#a8edea', '#fed6e3'
                ];
                
                // Pie Chart
                if (modelNames.length > 0) {{
                    const pieCtx = document.getElementById('modelPieChart').getContext('2d');
                    new Chart(pieCtx, {{
                        type: 'doughnut',
                        data: {{
                            labels: modelNames,
                            datasets: [{{
                                data: modelCounts,
                                backgroundColor: colors,
                                borderWidth: 2,
                                borderColor: '#fff'
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: true,
                            plugins: {{
                                legend: {{
                                    position: 'bottom',
                                    labels: {{
                                        padding: 15,
                                        font: {{
                                            size: 11
                                        }}
                                    }}
                                }},
                                tooltip: {{
                                    callbacks: {{
                                        label: function(context) {{
                                            const label = context.label || '';
                                            const value = context.parsed || 0;
                                            const total = context.dataset.data.reduce((a, b) => a + b, 0);
                                            const percentage = ((value / total) * 100).toFixed(1);
                                            return label + ': ' + value + ' (' + percentage + '%)';
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }});
                    
                    // Bar Chart
                    const barCtx = document.getElementById('modelBarChart').getContext('2d');
                    new Chart(barCtx, {{
                        type: 'bar',
                        data: {{
                            labels: modelNames,
                            datasets: [{{
                                label: 'Requests',
                                data: modelCounts,
                                backgroundColor: colors[0],
                                borderColor: colors[1],
                                borderWidth: 1
                            }}]
                        }},
                        options: {{
                            responsive: true,
                            maintainAspectRatio: true,
                            plugins: {{
                                legend: {{
                                    display: false
                                }},
                                tooltip: {{
                                    callbacks: {{
                                        label: function(context) {{
                                            return 'Requests: ' + context.parsed.y;
                                        }}
                                    }}
                                }}
                            }},
                            scales: {{
                                y: {{
                                    beginAtZero: true,
                                    ticks: {{
                                        stepSize: 1
                                    }}
                                }},
                                x: {{
                                    ticks: {{
                                        font: {{
                                            size: 10
                                        }},
                                        maxRotation: 45,
                                        minRotation: 45
                                    }}
                                }}
                            }}
                        }}
                    }});
                }} else {{
                    // Show "no data" message
                    document.getElementById('modelPieChart').parentElement.innerHTML = '<p style="text-align: center; color: #999; padding: 50px;">No usage data yet</p>';
                    document.getElementById('modelBarChart').parentElement.innerHTML = '<p style="text-align: center; color: #999; padding: 50px;">No usage data yet</p>';
                }}
            </script>
        </body>
        </html>
    """

@app.post("/create-key")
async def create_key(session: str = Depends(get_current_session), name: str = Form(...), rpm: int = Form(...)):
    if not session:
        return RedirectResponse(url="/login")
    try:
        config = get_config()
        new_key = {
            "name": name.strip(),
            "key": f"sk-lmab-{uuid.uuid4()}",
            "rpm": max(1, min(rpm, 1000)),  # Clamp between 1-1000
            "created": int(time.time())
        }
        config["api_keys"].append(new_key)
        save_config(config)
    except Exception as e:
        debug_print(f"❌ Error creating key: {e}")
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/delete-key")
async def delete_key(session: str = Depends(get_current_session), key_id: str = Form(...)):
    if not session:
        return RedirectResponse(url="/login")
    try:
        config = get_config()
        config["api_keys"] = [k for k in config["api_keys"] if k["key"] != key_id]
        save_config(config)
    except Exception as e:
        debug_print(f"❌ Error deleting key: {e}")
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

@app.post("/refresh-models")
async def refresh_models(session: str = Depends(get_current_session)):
    if not session:
        return RedirectResponse(url="/login")
    try:
        await refresh_models_via_proxy(require_active=True)
    except Exception as e:
        debug_print(f"❌ Error refreshing models: {e}")
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_303_SEE_OTHER)

# --- Userscript Proxy Support ---

# Timestamp of last userscript poll (legacy compatibility for tests/metrics).
last_userscript_poll: float = 0

@app.post("/api/v1/userscript/poll")
async def userscript_poll(request: Request):
    """
    Long-poll endpoint for the Tampermonkey/Violetmonkey proxy client (docs/lmbridge-proxy.user.js).
    Returns 204 when no jobs are available.
    """
    _userscript_proxy_check_secret(request)

    _touch_userscript_poll()

    try:
        data = await request.json()
    except Exception:
        data = {}

    cfg = get_config()
    timeout_seconds = data.get("timeout_seconds")
    if timeout_seconds is None:
        timeout_seconds = cfg.get("userscript_proxy_poll_timeout_seconds", 25)
    try:
        timeout_seconds = int(timeout_seconds)
    except Exception:
        timeout_seconds = 25
    timeout_seconds = max(0, min(timeout_seconds, 60))

    _cleanup_userscript_proxy_jobs(cfg)

    queue = _get_userscript_proxy_queue()
    end = time.time() + float(timeout_seconds)
    while True:
        remaining = end - time.time()
        if remaining <= 0:
            return Response(status_code=204)
        try:
            job_id = await asyncio.wait_for(queue.get(), timeout=remaining)
        except asyncio.TimeoutError:
            return Response(status_code=204)

        job = _USERSCRIPT_PROXY_JOBS.get(str(job_id))
        if not isinstance(job, dict):
            continue
        # Mark as picked up as soon as we hand the job to a poller so the server-side pickup timeout
        # doesn't trip while the poller/browser is starting.
        try:
            picked = job.get("picked_up_event")
            if isinstance(picked, asyncio.Event) and not picked.is_set():
                picked.set()
                if not job.get("picked_up_at_monotonic"):
                    job["picked_up_at_monotonic"] = time.monotonic()
            if str(job.get("phase") or "") == "queued":
                job["phase"] = "picked_up"
        except Exception:
            pass
        return {"job_id": str(job_id), "payload": job.get("payload") or {}}


@app.post("/api/v1/userscript/push")
async def userscript_push(request: Request):
    """
    Receives streamed lines from the userscript proxy and feeds them into the waiting request.
    """
    _userscript_proxy_check_secret(request)
    _touch_userscript_poll()

    try:
        data = await request.json()
    except Exception:
        data = {}

    job_id = str(data.get("job_id") or "").strip()
    if not job_id:
        raise HTTPException(status_code=400, detail="Missing job_id")

    job = _USERSCRIPT_PROXY_JOBS.get(job_id)
    if not isinstance(job, dict):
        raise HTTPException(status_code=404, detail="Unknown job_id")

    fetch_started = data.get("upstream_fetch_started")
    if fetch_started is None:
        fetch_started = data.get("fetch_started")
    status_code = data.get("status")
    if fetch_started or isinstance(status_code, int):
        try:
            if not job.get("upstream_fetch_started_at_monotonic"):
                job["upstream_fetch_started_at_monotonic"] = time.monotonic()
        except Exception:
            pass

    if isinstance(status_code, int):
        job["status_code"] = int(status_code)
        status_event = job.get("status_event")
        if isinstance(status_event, asyncio.Event):
            status_event.set()
    headers = data.get("headers")
    if isinstance(headers, dict):
        job["headers"] = headers

    body_preview = data.get("body_preview")
    if body_preview is None:
        body_preview = data.get("bodyPreview")
    if body_preview:
        job["body_preview"] = str(body_preview)
    body_preview_content_type = data.get("body_preview_content_type")
    if body_preview_content_type is None:
        body_preview_content_type = data.get("content_type")
    if body_preview_content_type:
        job["body_preview_content_type"] = str(body_preview_content_type)

    error = data.get("error")
    if error:
        job["error"] = str(error)

    lines = data.get("lines") or []
    if isinstance(lines, list):
        for line in lines:
            if line is None:
                continue
            await job["lines_queue"].put(str(line))

    if bool(data.get("done")):
        job["done"] = True
        done_event = job.get("done_event")
        if isinstance(done_event, asyncio.Event):
            done_event.set()
        status_event = job.get("status_event")
        if isinstance(status_event, asyncio.Event):
            status_event.set()
        await job["lines_queue"].put(None)

    return {"status": "ok"}


@app.get("/api/v1/userscript/status")
async def userscript_status(request: Request):
    """
    Status endpoint for userscript-proxy liveness debugging.
    """
    _userscript_proxy_check_secret(request)
    cfg = get_config()
    now = float(time.time())

    def _safe_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    poll_timeout_seconds = 25
    try:
        poll_timeout_seconds = int(cfg.get("userscript_proxy_poll_timeout_seconds", 25))
    except (TypeError, ValueError):
        poll_timeout_seconds = 25
    # Match _userscript_proxy_is_active window clamping.
    active_window_seconds = max(10, min(poll_timeout_seconds + 10, 90))
    last_poll = max(_safe_float(USERSCRIPT_PROXY_LAST_POLL_AT), _safe_float(last_userscript_poll))
    age_seconds = now - last_poll
    queue_size = 0
    try:
        queue_size = int(_get_userscript_proxy_queue().qsize())
    except Exception:
        queue_size = 0
    pending_jobs = 0
    for job in _USERSCRIPT_PROXY_JOBS.values():
        if isinstance(job, dict) and not job.get("done"):
            pending_jobs += 1
    return {
        "is_active": _userscript_proxy_is_active(cfg),
        "seconds_since_last_poll": age_seconds,
        "queue_size": queue_size,
        "pending_jobs": pending_jobs,
        "now_unix": now,
        "last_poll_unix": last_poll,
        "poll_timeout_seconds": poll_timeout_seconds,
        "active_window_seconds": active_window_seconds,
    }


# --- OpenAI Compatible API Endpoints ---

@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint for monitoring"""
    try:
        models = get_models()
        config = get_config()
        
        # Basic health checks
        proxy_active = _userscript_proxy_is_active(config)
        has_models = len(models) > 0
        has_api_keys = len(config.get("api_keys", [])) > 0

        status = "healthy" if (proxy_active and has_models) else "degraded"

        return {
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "checks": {
                "proxy_active": proxy_active,
                "models_loaded": has_models,
                "model_count": len(models),
                "api_keys_configured": has_api_keys,
            },
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e)
        }

@app.get("/api/v1/models")
async def list_models(api_key: dict = Depends(rate_limit_api_key)):
    try:
        models = get_models()
        if not models:
            debug_print("⚠️ Models cache is empty. Returning empty model list.")
        
        # Filter for models with text OR search OR image output capability and an organization (exclude stealth models)
        # Always include image models - no special key needed
        valid_models = [m for m in models 
                       if (m.get('capabilities', {}).get('outputCapabilities', {}).get('text')
                           or m.get('capabilities', {}).get('outputCapabilities', {}).get('search')
                           or m.get('capabilities', {}).get('outputCapabilities', {}).get('image'))
                       and m.get('organization')]
        
        return {
            "object": "list",
            "data": [
                {
                    "id": model.get("publicName"),
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": model.get("organization", "lmarena")
                } for model in valid_models if model.get("publicName")
            ]
        }
    except Exception as e:
        debug_print(f"❌ Error listing models: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to load models: {str(e)}")


@app.get("/api/v1/_debug/stream")
async def debug_stream(api_key: dict = Depends(rate_limit_api_key)):  # noqa: ARG001
    async def _gen():
        yield ": keep-alive\n\n"
        await asyncio.sleep(0.05)
        yield 'data: {"ok":true}\n\n'
        yield "data: [DONE]\n\n"

    return StreamingResponse(_gen(), media_type="text/event-stream")

@app.post("/api/v1/chat/completions")
async def api_chat_completions(request: Request, api_key: dict = Depends(rate_limit_api_key)):
    debug_print("\n" + "="*80)
    debug_print("🔵 NEW API REQUEST RECEIVED")
    debug_print("="*80)
    
    try:
        # Parse request body with error handling
        try:
            body = await request.json()
        except json.JSONDecodeError as e:
            debug_print(f"❌ Invalid JSON in request body: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid JSON in request body: {str(e)}")
        except Exception as e:
            debug_print(f"❌ Failed to read request body: {e}")
            raise HTTPException(status_code=400, detail=f"Failed to read request body: {str(e)}")
        
        debug_print(f"📥 Request body keys: {list(body.keys())}")
        
        # Validate required fields
        model_public_name = body.get("model")
        messages = body.get("messages", [])
        stream = body.get("stream", False)
        
        debug_print(f"🌊 Stream mode: {stream}")
        debug_print(f"🤖 Requested model: {model_public_name}")
        debug_print(f"💬 Number of messages: {len(messages)}")
        
        if not model_public_name:
            debug_print("❌ Missing 'model' in request")
            raise HTTPException(status_code=400, detail="Missing 'model' in request body.")
        
        if not messages:
            debug_print("❌ Missing 'messages' in request")
            raise HTTPException(status_code=400, detail="Missing 'messages' in request body.")
        
        if not isinstance(messages, list):
            debug_print("❌ 'messages' must be an array")
            raise HTTPException(status_code=400, detail="'messages' must be an array.")
        
        if len(messages) == 0:
            debug_print("❌ 'messages' array is empty")
            raise HTTPException(status_code=400, detail="'messages' array cannot be empty.")

        # Find model ID from public name
        try:
            models = get_models()
            debug_print(f"📚 Total models loaded: {len(models)}")
        except Exception as e:
            debug_print(f"❌ Failed to load models: {e}")
            raise HTTPException(
                status_code=503,
                detail="Failed to load model list from LMArena. Please try again later."
            )
        
        model_id = None
        model_org = None
        model_capabilities = {}
        
        for m in models:
            if m.get("publicName") == model_public_name:
                model_id = m.get("id")
                model_org = m.get("organization")
                model_capabilities = m.get("capabilities", {})
                break
        
        if not model_id:
            debug_print(f"❌ Model '{model_public_name}' not found in model list")
            raise HTTPException(
                status_code=404, 
                detail=f"Model '{model_public_name}' not found. Use /api/v1/models to see available models."
            )
        
        # Check if model is a stealth model (no organization)
        if not model_org:
            debug_print(f"❌ Model '{model_public_name}' is a stealth model (no organization)")
            raise HTTPException(
                status_code=403,
                detail="You do not have access to stealth models. Contact cloudwaddie for more info."
            )
        
        debug_print(f"✅ Found model ID: {model_id}")
        debug_print(f"🔧 Model capabilities: {model_capabilities}")
        
        # Determine modality based on model capabilities.
        # Priority: image > search > chat
        if model_capabilities.get("outputCapabilities", {}).get("image"):
            modality = "image"
        elif model_capabilities.get("outputCapabilities", {}).get("search"):
            modality = "search"
        else:
            modality = "chat"
        debug_print(f"🔍 Model modality: {modality}")

        # Log usage
        try:
            model_usage_stats[model_public_name] += 1
            # Save stats immediately after incrementing
            config = get_config()
            config["usage_stats"] = dict(model_usage_stats)
            save_config(config)
        except Exception as e:
            # Don't fail the request if usage logging fails
            debug_print(f"⚠️  Failed to log usage stats: {e}")

        # Extract system prompt if present and prepend to first user message
        system_prompt = ""
        system_messages = [m for m in messages if m.get("role") == "system"]
        if system_messages:
            system_prompt = "\n\n".join([_coerce_message_content_to_text(m.get("content", "")) for m in system_messages])
            debug_print(f"📋 System prompt found: {system_prompt[:100]}..." if len(system_prompt) > 100 else f"📋 System prompt: {system_prompt}")
        
        # Process last message content (may include images)
        try:
            last_message_content = messages[-1].get("content", "")
            try:
                prompt, experimental_attachments = await process_message_content(last_message_content, model_capabilities)
            except Exception as e:
                debug_print(f"❌ Failed to process message content: {e}")
                raise HTTPException(status_code=400, detail=f"Invalid message content: {str(e)}")
            
            # If there's a system prompt and this is the first user message, prepend it
            if system_prompt:
                prompt = f"{system_prompt}\n\n{prompt}"
                debug_print(f"✅ System prompt prepended to user message")
        except Exception as e:
            debug_print(f"❌ Failed to process message content: {e}")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to process message content: {str(e)}"
            )
        
        # Validate prompt
        if not prompt:
            # If no text but has attachments, that's okay for vision models
            if not experimental_attachments:
                debug_print("❌ Last message has no content")
                raise HTTPException(status_code=400, detail="Last message must have content.")
        
        # Log prompt length for debugging character limit issues
        debug_print(f"📝 User prompt length: {len(prompt)} characters")
        debug_print(f"🖼️  Attachments: {len(experimental_attachments)} images")
        debug_print(f"📝 User prompt preview: {prompt[:100]}..." if len(prompt) > 100 else f"📝 User prompt: {prompt}")
        
        # Check for reasonable character limit (LMArena appears to have limits)
        # Typical limit seems to be around 32K-64K characters based on testing
        MAX_PROMPT_LENGTH = 113567  # User hardcoded limit
        if len(prompt) > MAX_PROMPT_LENGTH:
            error_msg = f"Prompt too long ({len(prompt)} characters). LMArena has a character limit of approximately {MAX_PROMPT_LENGTH} characters. Please reduce the message size."
            debug_print(f"❌ {error_msg}")
            raise HTTPException(status_code=400, detail=error_msg)
        
        # Use API key + conversation tracking
        api_key_str = api_key["key"]

        # Keep payload format consistent with upstream expectations but avoid token minting automation.
        recaptcha_token = ""
        
        # Generate conversation ID from context (API key + model + first user message)
        import hashlib
        first_user_message = next((m.get("content", "") for m in messages if m.get("role") == "user"), "")
        if isinstance(first_user_message, list):
            # Handle array content format
            first_user_message = str(first_user_message)
        conversation_key = f"{api_key_str}_{model_public_name}_{first_user_message[:100]}"
        conversation_id = hashlib.sha256(conversation_key.encode()).hexdigest()[:16]
        
        debug_print(f"🔑 API Key: {api_key_str[:20]}...")
        debug_print(f"💭 Auto-generated Conversation ID: {conversation_id}")
        debug_print(f"🔑 Conversation key: {conversation_key[:100]}...")

        # Headers are prepared after selecting an auth token (or when falling back to browser-only transports).
        headers: dict[str, str] = {}
        
        # Check if conversation exists for this API key (robust to tests patching chat_sessions to a plain dict)
        per_key_sessions = chat_sessions.setdefault(api_key_str, {})
        session = per_key_sessions.get(conversation_id)
        
        # Detect retry: if session exists and last message is same user message (no assistant response after it)
        is_retry = False
        retry_message_id = None
        
        if session and len(session.get("messages", [])) >= 2:
            stored_messages = session["messages"]
            # Check if last stored message is from user with same content
            if stored_messages[-1]["role"] == "user" and stored_messages[-1]["content"] == prompt:
                # This is a retry - client sent same message again without assistant response
                is_retry = True
                retry_message_id = stored_messages[-1]["id"]
                # Get the assistant message ID that needs to be regenerated
                if len(stored_messages) >= 2 and stored_messages[-2]["role"] == "assistant":
                    # There was a previous assistant response - we'll retry that one
                    retry_message_id = stored_messages[-2]["id"]
                    debug_print(f"🔁 RETRY DETECTED - Regenerating assistant message {retry_message_id}")
        
        if is_retry and retry_message_id:
            debug_print(f"🔁 Using RETRY endpoint")
            # Use LMArena's retry endpoint
            # Format: PUT /nextjs-api/stream/retry-evaluation-session-message/{sessionId}/messages/{messageId}
            payload = {}
            url = f"https://arena.ai/nextjs-api/stream/retry-evaluation-session-message/{session['conversation_id']}/messages/{retry_message_id}"
            debug_print(f"📤 Target URL: {url}")
            debug_print(f"📦 Using PUT method for retry")
            http_method = "PUT"
        elif not session:
            debug_print("🆕 Creating NEW conversation session")
            # New conversation - Generate all IDs at once (like the browser does)
            session_id = str(uuid7())
            user_msg_id = str(uuid7())
            model_msg_id = str(uuid7())
            model_b_msg_id = str(uuid7())
            
            debug_print(f"🔑 Generated session_id: {session_id}")
            debug_print(f"👤 Generated user_msg_id: {user_msg_id}")
            debug_print(f"🤖 Generated model_msg_id: {model_msg_id}")
            debug_print(f"🤖 Generated model_b_msg_id: {model_b_msg_id}")
             
            payload = {
                "id": session_id,
                "mode": "direct",
                "modelAId": model_id,
                "userMessageId": user_msg_id,
                "modelAMessageId": model_msg_id,
                "modelBMessageId": model_b_msg_id,
                "userMessage": {
                    "content": prompt,
                    "experimental_attachments": experimental_attachments,
                    "metadata": {}
                },
                "modality": modality,
                "recaptchaV3Token": recaptcha_token, # <--- ADD TOKEN HERE
            }
            url = f"https://arena.ai{STREAM_CREATE_EVALUATION_PATH}"
            debug_print(f"📤 Target URL: {url}")
            debug_print(f"📦 Payload structure: Simple userMessage format")
            debug_print(f"🔍 Full payload: {json.dumps(payload, indent=2)}")
            http_method = "POST"
        else:
            debug_print("🔄 Using EXISTING conversation session")
            # Follow-up message - Generate new message IDs
            user_msg_id = str(uuid7())
            debug_print(f"👤 Generated followup user_msg_id: {user_msg_id}")
            model_msg_id = str(uuid7())
            debug_print(f"🤖 Generated followup model_msg_id: {model_msg_id}")
            model_b_msg_id = str(uuid7())
            debug_print(f"🤖 Generated followup model_b_msg_id: {model_b_msg_id}")
             
            payload = {
                "id": session["conversation_id"],
                "modelAId": model_id,
                "userMessageId": user_msg_id,
                "modelAMessageId": model_msg_id,
                "modelBMessageId": model_b_msg_id,
                "userMessage": {
                    "content": prompt,
                    "experimental_attachments": experimental_attachments,
                    "metadata": {}
                },
                "modality": modality,
                "recaptchaV3Token": recaptcha_token, # <--- ADD TOKEN HERE
            }
            url = f"https://arena.ai/nextjs-api/stream/post-to-evaluation/{session['conversation_id']}"
            debug_print(f"📤 Target URL: {url}")
            debug_print(f"📦 Payload structure: Simple userMessage format")
            debug_print(f"🔍 Full payload: {json.dumps(payload, indent=2)}")
            http_method = "POST"

        debug_print(f"\n🚀 Making API request to LMArena...")
        debug_print(f"⏱️  Timeout set to: 120 seconds")

        if not _userscript_proxy_is_active():
            raise HTTPException(status_code=503, detail=USERSCRIPT_PROXY_REQUIRED_MESSAGE)

        proxy_headers = {
            "Accept": "text/event-stream" if stream else "text/plain",
            "Content-Type": "text/plain;charset=UTF-8",
            "Referer": "https://arena.ai/?mode=direct",
            "Origin": "https://arena.ai",
        }

        if stream:
            async def generate_stream():
                nonlocal session_id, user_msg_id, model_msg_id, model_b_msg_id

                # Safety: don't keep client sockets open forever on repeated upstream failures.
                try:
                    stream_total_timeout_seconds = float(get_config().get("stream_total_timeout_seconds", 600))
                except Exception:
                    stream_total_timeout_seconds = 600.0
                stream_total_timeout_seconds = max(30.0, min(stream_total_timeout_seconds, 3600.0))
                stream_started_at = time.monotonic()

                # Flush an immediate comment to keep the client connection alive while we do heavy lifting upstream
                yield ": keep-alive\n\n"
                await asyncio.sleep(0)

                chunk_id = f"chatcmpl-{uuid.uuid4()}"
                response_text = ""
                reasoning_text = ""
                citations = []
                finish_reason = None

                stream_context = await fetch_via_proxy_queue(
                    url=url,
                    payload=payload if isinstance(payload, dict) else payload,
                    http_method=http_method,
                    timeout_seconds=120,
                    streaming=True,
                    headers=proxy_headers,
                )

                if stream_context is None:
                    error_message = (
                        "Userscript proxy did not pick up the job in time. "
                        "Ensure Firefox is running with the extension active and an Arena tab open."
                    )
                    error_chunk = {
                        "error": {
                            "message": error_message,
                            "type": "proxy_unavailable",
                            "code": HTTPStatus.GATEWAY_TIMEOUT,
                        }
                    }
                    yield f"data: {json.dumps(error_chunk)}\n\n"
                    yield "data: [DONE]\n\n"
                    return

                proxy_job_id = str(getattr(stream_context, "job_id", "") or "")

                async with stream_context as response:
                    log_http_status(response.status_code, "LMArena API Stream")

                    if response.status_code >= 400:
                        body_text = ""
                        try:
                            body_text = (await response.aread()).decode("utf-8", errors="replace")
                        except Exception:
                            body_text = ""
                        job = None
                        job_error = ""
                        if proxy_job_id:
                            job = _USERSCRIPT_PROXY_JOBS.get(proxy_job_id)
                            if isinstance(job, dict) and job.get("error"):
                                job_error = str(job.get("error") or "")
                        diagnostic = _format_upstream_diagnostic(
                            response.status_code,
                            headers=response.headers,
                            body_text=body_text,
                            job=job if isinstance(job, dict) else None,
                        )
                        if job_error and "challenge requires user action" in job_error.lower():
                            message = "Challenge requires user action in the Arena tab."
                        elif job_error:
                            message = f"{job_error} {diagnostic}".strip()
                        else:
                            message = diagnostic
                        error_chunk = {
                            "error": {
                                "message": message,
                                "type": "proxy_error",
                                "code": response.status_code,
                            }
                        }
                        yield f"data: {json.dumps(error_chunk)}\n\n"
                        yield "data: [DONE]\n\n"
                        return

                    async def _aiter_with_keepalive(it):
                        pending = asyncio.create_task(it.__anext__())
                        try:
                            while True:
                                done, _ = await asyncio.wait({pending}, timeout=1.0)
                                if pending not in done:
                                    yield None
                                    continue
                                try:
                                    item = pending.result()
                                except StopAsyncIteration:
                                    break
                                pending = asyncio.create_task(it.__anext__())
                                yield item
                        finally:
                            if pending is not None and not pending.done():
                                pending.cancel()

                    async for maybe_line in _aiter_with_keepalive(response.aiter_lines().__aiter__()):
                        if maybe_line is None:
                            yield ": keep-alive\n\n"
                            continue

                        line = str(maybe_line).strip()
                        if line.startswith("data:"):
                            line = line[5:].lstrip()
                        if not line:
                            continue

                        if line.startswith("ag:"):
                            chunk_data = line[3:]
                            try:
                                reasoning_chunk = json.loads(chunk_data)
                                reasoning_text += reasoning_chunk
                                chunk_response = {
                                    "id": chunk_id,
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": model_public_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {"reasoning_content": reasoning_chunk},
                                        "finish_reason": None,
                                    }],
                                }
                                yield f"data: {json.dumps(chunk_response)}\n\n"
                            except json.JSONDecodeError:
                                continue

                        elif line.startswith("a0:"):
                            chunk_data = line[3:]
                            try:
                                text_chunk = json.loads(chunk_data)
                                response_text += text_chunk
                                chunk_response = {
                                    "id": chunk_id,
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": model_public_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {"content": text_chunk},
                                        "finish_reason": None,
                                    }],
                                }
                                yield f"data: {json.dumps(chunk_response)}\n\n"
                            except json.JSONDecodeError:
                                continue

                        elif line.startswith("a2:"):
                            image_data = line[3:]
                            try:
                                image_list = json.loads(image_data)
                                if isinstance(image_list, list) and len(image_list) > 0:
                                    image_obj = image_list[0]
                                    if image_obj.get('type') == 'image':
                                        image_url = image_obj.get('image', '')
                                        response_text = f"![Generated Image]({image_url})"
                                        chunk_response = {
                                            "id": chunk_id,
                                            "object": "chat.completion.chunk",
                                            "created": int(time.time()),
                                            "model": model_public_name,
                                            "choices": [{
                                                "index": 0,
                                                "delta": {"content": response_text},
                                                "finish_reason": None,
                                            }],
                                        }
                                        yield f"data: {json.dumps(chunk_response)}\n\n"
                            except json.JSONDecodeError:
                                pass

                        elif line.startswith("ac:"):
                            citation_data = line[3:]
                            try:
                                citation_obj = json.loads(citation_data)
                                if 'argsTextDelta' in citation_obj:
                                    args_data = json.loads(citation_obj['argsTextDelta'])
                                    if 'source' in args_data:
                                        source = args_data['source']
                                        if isinstance(source, list):
                                            citations.extend(source)
                                        elif isinstance(source, dict):
                                            citations.append(source)
                            except json.JSONDecodeError:
                                pass

                        elif line.startswith("a3:"):
                            error_data = line[3:]
                            try:
                                error_message = json.loads(error_data)
                                debug_print(f"❌ Error in stream: {error_message}")
                            except json.JSONDecodeError:
                                pass

                        elif line.startswith("ad:"):
                            metadata_data = line[3:]
                            try:
                                metadata = json.loads(metadata_data)
                                finish_reason = metadata.get("finishReason", "stop")
                                final_chunk = {
                                    "id": chunk_id,
                                    "object": "chat.completion.chunk",
                                    "created": int(time.time()),
                                    "model": model_public_name,
                                    "choices": [{
                                        "index": 0,
                                        "delta": {},
                                        "finish_reason": finish_reason,
                                    }],
                                }
                                yield f"data: {json.dumps(final_chunk)}\n\n"
                            except json.JSONDecodeError:
                                continue

                assistant_message = {
                    "id": model_msg_id,
                    "role": "assistant",
                    "content": response_text.strip(),
                }
                if reasoning_text:
                    assistant_message["reasoning_content"] = reasoning_text.strip()
                if citations:
                    unique_citations = []
                    seen_urls = set()
                    for citation in citations:
                        citation_url = citation.get('url')
                        if citation_url and citation_url not in seen_urls:
                            seen_urls.add(citation_url)
                            unique_citations.append(citation)
                    assistant_message["citations"] = unique_citations

                if not session:
                    chat_sessions[api_key_str][conversation_id] = {
                        "conversation_id": session_id,
                        "model": model_public_name,
                        "messages": [
                            {"id": user_msg_id, "role": "user", "content": prompt},
                            assistant_message,
                        ],
                    }
                else:
                    chat_sessions[api_key_str][conversation_id]["messages"].append(
                        {"id": user_msg_id, "role": "user", "content": prompt}
                    )
                    chat_sessions[api_key_str][conversation_id]["messages"].append(assistant_message)

                yield "data: [DONE]\n\n"
                return

            return StreamingResponse(generate_stream(), media_type="text/event-stream")

        response = await fetch_via_proxy_queue(
            url=url,
            payload=payload if isinstance(payload, dict) else payload,
            http_method=http_method,
            timeout_seconds=120,
            headers=proxy_headers,
        )
        if response is None:
            raise HTTPException(
                status_code=504,
                detail=(
                    "Userscript proxy did not pick up the job in time. "
                    "Ensure Firefox is running with the extension active and an Arena tab open."
                ),
            )

        log_http_status(response.status_code, "LMArena API Response")

        response_bytes = await response.aread()
        response_text_body = response_bytes.decode("utf-8", errors="replace")

        if response.status_code >= 400:
            job = None
            job_error = ""
            job_id = str(getattr(response, "job_id", "") or "")
            if job_id:
                job = _USERSCRIPT_PROXY_JOBS.get(job_id)
                if isinstance(job, dict) and job.get("error"):
                    job_error = str(job.get("error") or "")
            diagnostic = _format_upstream_diagnostic(
                response.status_code,
                headers=getattr(response, "headers", {}),
                body_text=response_text_body,
                job=job if isinstance(job, dict) else None,
            )
            if job_error and "challenge requires user action" in job_error.lower():
                raise HTTPException(status_code=503, detail="Challenge requires user action in the Arena tab.")
            if "challenge requires user action" in response_text_body.lower():
                raise HTTPException(status_code=503, detail="Challenge requires user action in the Arena tab.")
            raise HTTPException(status_code=502, detail=diagnostic)

        response_text = ""
        reasoning_text = ""
        citations = []
        finish_reason = None
        line_count = 0
        text_chunks_found = 0
        reasoning_chunks_found = 0
        citation_chunks_found = 0
        metadata_found = 0

        error_message = None
        for line in response_text_body.splitlines():
            line_count += 1
            line = line.strip()
            if line.startswith("data: "):
                line = line[6:].strip()
            if not line:
                continue

            if line.startswith("ag:"):
                chunk_data = line[3:]
                reasoning_chunks_found += 1
                try:
                    reasoning_chunk = json.loads(chunk_data)
                    reasoning_text += reasoning_chunk
                except json.JSONDecodeError:
                    continue

            elif line.startswith("a0:"):
                chunk_data = line[3:]
                text_chunks_found += 1
                try:
                    text_chunk = json.loads(chunk_data)
                    response_text += text_chunk
                except json.JSONDecodeError:
                    continue

            elif line.startswith("a2:"):
                image_data = line[3:]
                try:
                    image_list = json.loads(image_data)
                    if isinstance(image_list, list) and len(image_list) > 0:
                        image_obj = image_list[0]
                        if image_obj.get('type') == 'image':
                            image_url = image_obj.get('image', '')
                            response_text = f"![Generated Image]({image_url})"
                except json.JSONDecodeError:
                    continue

            elif line.startswith("ac:"):
                citation_data = line[3:]
                citation_chunks_found += 1
                try:
                    citation_obj = json.loads(citation_data)
                    if 'argsTextDelta' in citation_obj:
                        args_data = json.loads(citation_obj['argsTextDelta'])
                        if 'source' in args_data:
                            source = args_data['source']
                            if isinstance(source, list):
                                citations.extend(source)
                            elif isinstance(source, dict):
                                citations.append(source)
                except json.JSONDecodeError:
                    continue

            elif line.startswith("a3:"):
                error_data = line[3:]
                try:
                    error_message = json.loads(error_data)
                except json.JSONDecodeError:
                    error_message = error_data

            elif line.startswith("ad:"):
                metadata_data = line[3:]
                metadata_found += 1
                try:
                    metadata = json.loads(metadata_data)
                    finish_reason = metadata.get("finishReason")
                except json.JSONDecodeError:
                    continue

        if not response_text:
            if error_message:
                error_detail = f"LMArena API error: {error_message}"
                return {"error": {"message": error_detail, "type": "upstream_error", "code": "lmarena_error"}}
            error_detail = (
                "LMArena API returned empty response. This could be due to invalid auth token, "
                "expired session, or upstream rate limiting."
            )
            return {"error": {"message": error_detail, "type": "upstream_error", "code": "empty_response"}}

        assistant_message = {
            "id": model_msg_id,
            "role": "assistant",
            "content": response_text.strip(),
        }
        if reasoning_text:
            assistant_message["reasoning_content"] = reasoning_text.strip()
        if citations:
            unique_citations = []
            seen_urls = set()
            for citation in citations:
                citation_url = citation.get('url')
                if citation_url and citation_url not in seen_urls:
                    seen_urls.add(citation_url)
                    unique_citations.append(citation)
            assistant_message["citations"] = unique_citations

        if not session:
            chat_sessions[api_key_str][conversation_id] = {
                "conversation_id": session_id,
                "model": model_public_name,
                "messages": [
                    {"id": user_msg_id, "role": "user", "content": prompt},
                    assistant_message,
                ],
            }
        else:
            chat_sessions[api_key_str][conversation_id]["messages"].append(
                {"id": user_msg_id, "role": "user", "content": prompt}
            )
            chat_sessions[api_key_str][conversation_id]["messages"].append(assistant_message)

        message_obj = {
            "role": "assistant",
            "content": response_text.strip(),
        }
        if reasoning_text:
            message_obj["reasoning_content"] = reasoning_text.strip()
        if citations:
            unique_citations = []
            seen_urls = set()
            for citation in citations:
                citation_url = citation.get('url')
                if citation_url and citation_url not in seen_urls:
                    seen_urls.add(citation_url)
                    unique_citations.append(citation)
            message_obj["citations"] = unique_citations
            if unique_citations:
                footnotes = "\n\n---\n\n**Sources:**\n\n"
                for i, citation in enumerate(unique_citations, 1):
                    title = citation.get('title', 'Untitled')
                    url_value = citation.get('url', '')
                    footnotes += f"{i}. [{title}]({url_value})\n"
                message_obj["content"] = response_text.strip() + footnotes

        prompt_tokens = len(prompt)
        completion_tokens = len(response_text)
        reasoning_tokens = len(reasoning_text)
        total_tokens = prompt_tokens + completion_tokens + reasoning_tokens

        usage_obj = {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }
        if reasoning_tokens > 0:
            usage_obj["reasoning_tokens"] = reasoning_tokens

        final_response = {
            "id": f"chatcmpl-{uuid.uuid4()}",
            "object": "chat.completion",
            "created": int(time.time()),
            "model": model_public_name,
            "conversation_id": conversation_id,
            "choices": [{
                "index": 0,
                "message": message_obj,
                "finish_reason": "stop",
            }],
            "usage": usage_obj,
        }

        debug_print("\n✅ REQUEST COMPLETED SUCCESSFULLY")
        debug_print("="*80 + "\n")

        return final_response
    except HTTPException:
        raise
    except Exception as e:
        print(f"\n❌ TOP-LEVEL EXCEPTION")
        print(f"📛 Error type: {type(e).__name__}")
        print(f"📛 Error message: {str(e)}")
        print("="*80 + "\n")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# Anthropic API Models
from pydantic import BaseModel
from typing import Any

class AnthropicMessageRequest(BaseModel):
    model: str
    messages: List[Dict[str, Any]]
    max_tokens: int
    system: Optional[str] = None
    temperature: Optional[float] = None
    stream: Optional[bool] = False


@app.post("/api/v1/messages")
async def anthropic_messages(request: AnthropicMessageRequest, raw_request: Request, api_key: dict = Depends(rate_limit_api_key)):
    """
    Anthropic Messages API endpoint.
    Translates Anthropic-style requests to OpenAI-style and processes through LMArena.
    """
    debug_print("\n" + "="*80)
    debug_print("🟣 ANTHROPIC MESSAGES REQUEST RECEIVED")
    debug_print("="*80)
    debug_print(f"🤖 Model: {request.model}")
    debug_print(f"💬 Messages: {len(request.messages)}")
    debug_print(f"🌊 Stream: {request.stream}")

    # Convert Anthropic messages to OpenAI format
    openai_messages = []
    for msg in request.messages:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        if isinstance(content, list):
            # Handle content blocks
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get("type") == "text":
                    text_parts.append(block.get("text", ""))
            content = "\n".join(text_parts)
        openai_messages.append({"role": role, "content": content})

    # Add system message if present
    if request.system:
        openai_messages.insert(0, {"role": "system", "content": request.system})

    # Build OpenAI-style request body
    openai_body = {
        "model": request.model,
        "messages": openai_messages,
        "max_tokens": request.max_tokens,
        "stream": request.stream,
    }
    if request.temperature is not None:
        openai_body["temperature"] = request.temperature

    debug_print(f"📦 Converted to OpenAI format")

    # Create a mock request object for the chat completions handler
    class MockRequest:
        def __init__(self, body):
            self._body = body

        async def json(self):
            return self._body

        async def is_disconnected(self):
            return await raw_request.is_disconnected()

    mock_request = MockRequest(openai_body)

    if request.stream:
        # Streaming response - convert OpenAI SSE to Anthropic format
        async def anthropic_stream_generator():
            message_id = f"msg_{uuid.uuid4()}"
            accumulated_text = ""

            # Send message_start
            yield f"event: message_start\ndata: {json.dumps({'type': 'message_start', 'message': {'id': message_id, 'type': 'message', 'role': 'assistant', 'content': [], 'model': request.model, 'stop_reason': None, 'usage': {'input_tokens': sum(len(str(m.get('content', ''))) for m in openai_messages), 'output_tokens': 0}}})}\n\n"

            # Send content_block_start
            yield f"event: content_block_start\ndata: {json.dumps({'type': 'content_block_start', 'index': 0, 'content_block': {'type': 'text', 'text': ''}})}\n\n"

            try:
                result = await api_chat_completions(mock_request, api_key)

                if isinstance(result, StreamingResponse):
                    async for chunk in result.body_iterator:
                        chunk_str = chunk.decode('utf-8') if isinstance(chunk, bytes) else str(chunk)
                        for line in chunk_str.strip().split('\n'):
                            if line.startswith('data: '):
                                data = line[6:]
                                if data == '[DONE]':
                                    break
                                try:
                                    chunk_data = json.loads(data)
                                    delta = chunk_data.get("choices", [{}])[0].get("delta", {})
                                    if "content" in delta:
                                        content = delta["content"]
                                        accumulated_text += content
                                        yield f"event: content_block_delta\ndata: {json.dumps({'type': 'content_block_delta', 'index': 0, 'delta': {'type': 'text_delta', 'text': content}})}\n\n"
                                except json.JSONDecodeError:
                                    continue

            except Exception as e:
                yield f"event: error\ndata: {json.dumps({'type': 'error', 'error': {'message': str(e), 'type': 'internal_error'}})}\n\n"
                return

            # Send content_block_stop
            yield f"event: content_block_stop\ndata: {json.dumps({'type': 'content_block_stop', 'index': 0})}\n\n"

            # Send message_delta
            yield f"event: message_delta\ndata: {json.dumps({'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}, 'usage': {'output_tokens': len(accumulated_text)}})}\n\n"

            # Send message_stop
            yield f"event: message_stop\ndata: {json.dumps({'type': 'message_stop'})}\n\n"

        return StreamingResponse(
            anthropic_stream_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no"
            }
        )
    else:
        # Non-streaming response
        result = await api_chat_completions(mock_request, api_key)

        if isinstance(result, StreamingResponse):
            # Read streaming response
            full_text = ""
            async for chunk in result.body_iterator:
                chunk_str = chunk.decode('utf-8') if isinstance(chunk, bytes) else str(chunk)
                for line in chunk_str.strip().split('\n'):
                    if line.startswith('data: '):
                        data = line[6:]
                        if data == '[DONE]':
                            break
                        try:
                            chunk_data = json.loads(data)
                            delta = chunk_data.get("choices", [{}])[0].get("delta", {})
                            if "content" in delta:
                                full_text += delta["content"]
                        except json.JSONDecodeError:
                            continue

            return {
                "id": f"msg_{uuid.uuid4()}",
                "type": "message",
                "role": "assistant",
                "content": [{"type": "text", "text": full_text}],
                "model": request.model,
                "stop_reason": "end_turn",
                "usage": {"input_tokens": 0, "output_tokens": len(full_text.split())}
            }

        if isinstance(result, dict) and "error" in result:
            raise HTTPException(status_code=400, detail=result["error"].get("message", "Unknown error"))

        # Convert OpenAI response to Anthropic format
        choices = result.get("choices", [])
        content = ""
        if choices:
            content = choices[0].get("message", {}).get("content", "")

        return {
            "id": result.get("id", f"msg_{uuid.uuid4()}"),
            "type": "message",
            "role": "assistant",
            "content": [{"type": "text", "text": content}],
            "model": request.model,
            "stop_reason": "end_turn",
            "usage": {"input_tokens": result.get("usage", {}).get("prompt_tokens", 0), "output_tokens": result.get("usage", {}).get("completion_tokens", len(content))}
        }


if __name__ == "__main__":
    # Avoid crashes on Windows consoles with non-UTF8 code pages (e.g., GBK) when printing emojis.
    try:
        import sys

        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    print("=" * 60)
    print("🚀 LMArena Bridge Server Starting...")
    print("=" * 60)
    print(f"📍 Dashboard: http://localhost:{PORT}/dashboard")
    print(f"🔐 Login: http://localhost:{PORT}/login")
    print(f"📚 API Base URL: http://localhost:{PORT}/api/v1")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
