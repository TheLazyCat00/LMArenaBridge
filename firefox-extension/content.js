const extensionApi = typeof browser !== "undefined" ? browser : chrome;

const MESSAGE_SOURCE = "lmbridge-proxy-page";
const ALLOWED_ORIGINS = ["https://arena.ai", "https://lmarena.ai"];

let activeJobId = null;

function validateJobUrl(rawUrl) {
  const url = String(rawUrl || "");
  if (!url) {
    return { ok: false, error: "Job URL is missing." };
  }
  if (url.startsWith("/")) {
    return { ok: true, url };
  }
  let parsed;
  try {
    parsed = new URL(url);
  } catch (error) {
    return { ok: false, error: "Job URL is invalid." };
  }
  if (!ALLOWED_ORIGINS.includes(parsed.origin)) {
    return { ok: false, error: "Job URL is not in the allowlist." };
  }
  return { ok: true, url };
}

function injectPageBridge() {
  if (window.__lmbridgeProxyInjected) {
    return;
  }
  window.__lmbridgeProxyInjected = true;
  const script = document.createElement("script");
  script.textContent = String.raw`
    (() => {
      if (window.__lmbridgeProxyBridgeReady) return;
      window.__lmbridgeProxyBridgeReady = true;
      const MESSAGE_SOURCE = "lmbridge-proxy-page";
      const MAX_LINES = 50;
      const MAX_BYTES = 32768;
      const CHALLENGE_CHECK_BYTES = 1024;
      const BODY_PREVIEW_BYTES = 512;
      let runningJobId = null;

      function sendUpdate(jobId, update) {
        window.postMessage(
          { source: MESSAGE_SOURCE, type: "update", jobId, ...update },
          "*"
        );
      }

      function challengeError(jobId, contentType, preview) {
        const snippet = String(preview || "").slice(0, BODY_PREVIEW_BYTES);
        sendUpdate(jobId, {
          error: "Challenge requires user action in the Arena tab",
          body_preview: snippet,
          body_preview_content_type: contentType || "",
          done: true,
        });
      }

      function detectChallenge(contentType, sample) {
        const type = String(contentType || "").toLowerCase();
        if (type.includes("text/html")) {
          return true;
        }
        const text = String(sample || "").toLowerCase();
        return (
          text.includes("just a moment") ||
          text.includes("cloudflare") ||
          text.includes("cf-")
        );
      }

      function createBatcher(jobId) {
        let lines = [];
        let bytes = 0;
        const flush = () => {
          if (!lines.length) {
            return;
          }
          sendUpdate(jobId, { lines });
          lines = [];
          bytes = 0;
        };
        const pushLine = (line) => {
          lines.push(line);
          bytes += line.length;
          if (lines.length >= MAX_LINES || bytes >= MAX_BYTES) {
            flush();
          }
        };
        return { flush, pushLine };
      }

      async function streamResponse(jobId, response) {
        const headers = {};
        try {
          response.headers.forEach((value, key) => {
            headers[key] = value;
          });
        } catch (error) {
          // ignore header extraction failures
        }
        sendUpdate(jobId, {
          status: response.status,
          headers,
          upstream_fetch_started: true,
        });

        const contentType = response.headers.get("content-type") || "";
        if (detectChallenge(contentType, "")) {
          challengeError(jobId, contentType, "");
          return;
        }

        if (response.status >= 400) {
          let errorText = "";
          try {
            errorText = await response.text();
          } catch (error) {
            errorText = "";
          }
          const preview = String(errorText || "").slice(0, BODY_PREVIEW_BYTES);
          if (detectChallenge(contentType, preview)) {
            challengeError(jobId, contentType, preview);
            return;
          }
          sendUpdate(jobId, {
            body_preview: preview,
            body_preview_content_type: contentType,
            done: true,
          });
          return;
        }

        const reader =
          response.body && typeof response.body.getReader === "function"
            ? response.body.getReader()
            : null;
        if (!reader) {
          const text = await response.text();
          if (detectChallenge(contentType, text)) {
            challengeError(jobId, contentType, text);
            return;
          }
          const batcher = createBatcher(jobId);
          const normalized = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
          const parts = normalized.split("\n");
          for (const part of parts) {
            if (part !== "") {
              batcher.pushLine(part);
            }
          }
          batcher.flush();
          sendUpdate(jobId, { done: true });
          return;
        }

        const decoder = new TextDecoder();
        let buffer = "";
        let checkedChallenge = false;
        const batcher = createBatcher(jobId);
        while (true) {
          const { value, done } = await reader.read();
          if (done) {
            break;
          }
          const chunk = decoder.decode(value, { stream: true });
          if (!checkedChallenge) {
            const sample = (buffer + chunk).slice(0, CHALLENGE_CHECK_BYTES);
            if (detectChallenge(contentType, sample)) {
              try {
                await reader.cancel();
              } catch (error) {
                // ignore
              }
              challengeError(jobId, contentType, sample);
              return;
            }
            checkedChallenge = sample.length >= CHALLENGE_CHECK_BYTES;
          }
          buffer += chunk;
          buffer = buffer.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
          const parts = buffer.split("\n");
          buffer = parts.pop() || "";
          for (const part of parts) {
            batcher.pushLine(part);
          }
        }
        if (buffer) {
          batcher.pushLine(buffer);
        }
        batcher.flush();
        sendUpdate(jobId, { done: true });
      }

      window.addEventListener("message", async (event) => {
        if (event.source !== window) return;
        const data = event.data;
        if (!data || data.source !== MESSAGE_SOURCE || data.type !== "job") return;
        const jobId = String(data.jobId || "");
        const payload = data.payload || {};
        if (!jobId) {
          return;
        }
        if (runningJobId && runningJobId !== jobId) {
          sendUpdate(jobId, { error: "Another job is already running.", done: true });
          return;
        }
        runningJobId = jobId;
        try {
          const response = await fetch(payload.url, {
            method: payload.method || "POST",
            headers: payload.headers || {},
            body: payload.body || null,
            credentials: "include",
          });
          await streamResponse(jobId, response);
        } catch (error) {
          sendUpdate(jobId, {
            error: error && error.message ? error.message : "Fetch failed",
            done: true,
          });
        } finally {
          runningJobId = null;
        }
      });
    })();
  `;
  (document.head || document.documentElement).appendChild(script);
  script.remove();
}

function forwardUpdate(data) {
  if (!data || data.source !== MESSAGE_SOURCE || data.type !== "update") {
    return;
  }
  if (!activeJobId || data.jobId !== activeJobId) {
    return;
  }
    const update = {
      type: "lmbridge-proxy-update",
      jobId: data.jobId,
      status: data.status,
      headers: data.headers,
      lines: data.lines,
      error: data.error,
      body_preview: data.body_preview,
      body_preview_content_type: data.body_preview_content_type,
      done: data.done,
      upstream_fetch_started: data.upstream_fetch_started,
    };
  extensionApi.runtime.sendMessage(update);
  if (data.done) {
    activeJobId = null;
  }
}

window.addEventListener("message", (event) => {
  if (event.source !== window) {
    return;
  }
  forwardUpdate(event.data);
});

extensionApi.runtime.onMessage.addListener((message) => {
  if (!message || message.type !== "lmbridge-proxy-job") {
    return undefined;
  }
  if (activeJobId && activeJobId !== message.jobId) {
    extensionApi.runtime.sendMessage({
      type: "lmbridge-proxy-update",
      jobId: message.jobId,
      error: "Another job is already running.",
      done: true,
    });
    return undefined;
  }
  const urlCheck = validateJobUrl(message.payload?.url);
  if (!urlCheck.ok) {
    extensionApi.runtime.sendMessage({
      type: "lmbridge-proxy-update",
      jobId: message.jobId,
      error: urlCheck.error,
      done: true,
    });
    return undefined;
  }
  injectPageBridge();
  activeJobId = message.jobId;
  window.postMessage(
    { source: MESSAGE_SOURCE, type: "job", jobId: message.jobId, payload: message.payload },
    "*"
  );
  return undefined;
});
