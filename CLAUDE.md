# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run the server
python -m src.main

# Run all tests
python -m pytest tests/

# Run a single test file
python -m pytest tests/ -v

# Syntax check
python -c "import ast; ast.parse(open('src/main.py', encoding='utf-8').read()); print('OK')"
```

Python version: **3.12** (see `.python-version`).

## Architecture

This is a Firefox-first OpenAI-compatible API bridge to LM Arena (`arena.ai`). The Python server exposes
`POST /api/v1/chat/completions` and routes all upstream requests through the Firefox extension userscript
proxy. There is **no** browser automation or token minting in Python.

### Module structure (`src/`)

- **`main.py`** — FastAPI app, route handlers, request/streaming logic, dashboard, startup.
- **`constants.py`** — Hardcoded values: HTTP status codes, timeouts, allowlists, proxy defaults.
- **`config.py`** — Config file I/O (`get_config`, `save_config`, `get_models`, `save_models`).
- **`state.py`** — In-memory shared state (chat sessions, usage stats).
- **`transport.py`** — Userscript proxy queue + stream response helpers.

### Cross-module pattern (`_m()` late import)

Transport helpers access `main.py` globals via a lazy import helper:

```python
def _m():
    from . import main
    return main
```

Key globals that must stay in `main.py`: `CONFIG_FILE`, `chat_sessions`, `_USERSCRIPT_PROXY_JOBS`,
`_USERSCRIPT_PROXY_QUEUE`, `USERSCRIPT_PROXY_LAST_POLL_AT`, `last_userscript_poll`.

### Transport layer

The chat completions endpoint always enqueues a userscript proxy job and waits for streamed results
from `/api/v1/userscript/push`. If the proxy is inactive, requests return 503 with instructions to
start Firefox + the extension.

### Userscript proxy

A two-sided long-poll system:
- `POST /api/v1/userscript/poll` — Extension polls for fetch jobs
- `POST /api/v1/userscript/push` — Extension pushes response chunks back
- `GET /api/v1/userscript/status` — Liveness/debugging information

### Config file (`config.json`)

Key fields: `api_keys` (list of `{name, key, rpm, created}`), `password`, `userscript_proxy_secret`,
`userscript_proxy_poll_timeout_seconds`, `userscript_proxy_job_ttl_seconds`,
`userscript_proxy_pickup_timeout_seconds`.
