# LMArenaBridge Userscript Proxy Guardrails

- Do **not** bypass or defeat Cloudflare Turnstile, reCAPTCHA, or other anti-bot measures.
- If a challenge/interstitial is detected, report: **“Challenge requires user action in the Arena tab”** and end the job.
- Enforce a strict allowlist: only `https://arena.ai` and `https://lmarena.ai`.
- Never leak the userscript proxy secret to web pages or content scripts.
- Bridge communication must target **localhost/127.0.0.1** only.
- Process userscript-proxy jobs **sequentially** (one at a time).
- Always push a final `done=true` update for every job, even if there are zero lines.
