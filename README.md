# LM Arena Bridge (Firefox-first)

A Firefox-first OpenAI-compatible API bridge for LM Arena. All upstream requests to `arena.ai` / `lmarena.ai`
are executed by the included Firefox extension; the Python server acts as an API layer, job queue, and
streaming multiplexer.

> **Important:** This project **does not** bypass Cloudflare/Turnstile/reCAPTCHA. If a challenge appears,
> you must complete it manually in Firefox.

## Requirements

- Python 3.10+ (3.12 recommended)
- Firefox (desktop)
- The included extension in `./firefox-extension/`

## Install

```bash
pip install -r requirements.txt
```

## Run

1. **Start the server**
   ```bash
   python -m src.main
   ```
2. **Load the Firefox extension**
   - Open `about:debugging#/runtime/this-firefox`
   - Click **Load Temporary Add-on…**
   - Select `firefox-extension/manifest.json`
3. **Configure the extension**
   - Open the extension options
   - Set **Bridge Base URL** to `http://127.0.0.1:8000`
   - (Optional) set a **Userscript Proxy Secret** (must match `userscript_proxy_secret` in `config.json`)
4. **Open Arena**
   - Open `https://arena.ai` or `https://lmarena.ai` in Firefox
   - Log in and complete any challenges
5. **Use the API**
   - OpenAI-compatible base URL: `http://127.0.0.1:8000/api/v1`

## Endpoints

OpenAI-compatible:
- `POST /api/v1/chat/completions` (streaming and non-stream)
- `GET /api/v1/models`

Userscript proxy:
- `POST /api/v1/userscript/poll`
- `POST /api/v1/userscript/push`
- `GET /api/v1/userscript/status`

## Models Cache

Models are loaded from `models.json` on disk. If the cache is missing or empty, `/api/v1/models` returns an
empty list and the server logs a warning.

Use the dashboard button **Refresh Models via Proxy** to repopulate the cache (requires the extension to be
active).

## Troubleshooting

- **503 Userscript proxy required**
  - Start Firefox, load the extension, and open an Arena tab.
- **504 Userscript proxy did not pick up the job**
  - Ensure the extension is polling and the tab is active.
- **Challenge requires user action**
  - Complete the challenge in the Arena tab, then retry.
- **Upstream 401/403**
  - Usually means you are logged out; re-authenticate in Firefox.

## OpenWebUI

Set OpenWebUI’s OpenAI base URL to:

```
http://127.0.0.1:8000/api/v1
```

## Notes

- Image upload is currently unavailable in proxy-only mode.
- The server starts instantly and does not run any browser automation.
