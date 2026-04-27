# LMArenaBridge Firefox Userscript Proxy

This extension implements the LMArenaBridge userscript-proxy client in Firefox. It polls the local bridge for jobs, executes them in a logged-in Arena tab, and streams results back to the bridge.

## Install (Temporary Add-on)

1. Open Firefox and go to `about:debugging#/runtime/this-firefox`.
2. Click **Load Temporary Add-on…**
3. Select `firefox-extension/manifest.json`.

## Configure

1. Open the extension **Options** page.
2. Set:
   - **Bridge Base URL** (default `http://127.0.0.1:8000`)
   - **Userscript Proxy Secret** (optional; must match `userscript_proxy_secret` in `config.json`)
   - **Poll Timeout** (seconds; default 25)
   - **Preferred Arena Origin** (`https://lmarena.ai` or `https://arena.ai`)
3. Save settings.

## Use

1. Start LMArenaBridge locally: `python -m src.main`.
2. Open a tab on `https://arena.ai` or `https://lmarena.ai` and log in.
3. Make a streaming request to the bridge (as documented in the main README).

## Behavior Notes

- Only `https://arena.ai` and `https://lmarena.ai` URLs are allowed.
- Bridge communication is restricted to `localhost` / `127.0.0.1`.
- The proxy secret stays in background storage and is never sent to web pages.
- If a Cloudflare/Turnstile/reCAPTCHA interstitial is detected, the job is terminated with the error:  
  **“Challenge requires user action in the Arena tab”.**

