const extensionApi = typeof browser !== "undefined" ? browser : chrome;

const POLL_PATH = "/api/v1/userscript/poll";
const PUSH_PATH = "/api/v1/userscript/push";
const SECRET_HEADER = "X-LMBridge-Secret";
const ALLOWED_HOSTS = new Set(["arena.ai", "www.arena.ai", "lmarena.ai", "www.lmarena.ai"]);
const MIN_BACKOFF_SECONDS = 1;
const MAX_BACKOFF_SECONDS = 30;
const MAX_SEND_ATTEMPTS = 5;

const DEFAULT_SETTINGS = {
  bridgeBaseUrl: "http://127.0.0.1:8000",
  userscriptProxySecret: "",
  pollTimeoutSeconds: 25,
  arenaPreferredOrigin: "https://lmarena.ai",
};

const statusState = {
  state: "idle",
  lastError: "",
  lastPollAt: null,
  lastJobId: null,
  lastJobAt: null,
  backoffSeconds: 0,
  activeJobId: null,
};

let pollLoopRunning = false;
let pollBackoffSeconds = 0;
let activeJobContext = null;
let pollLoopEnabled = true;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getSettings() {
  const stored = await extensionApi.storage.local.get(DEFAULT_SETTINGS);
  return { ...DEFAULT_SETTINGS, ...stored };
}

function normalizeBaseUrl(value) {
  const raw = String(value || "").trim();
  if (!raw) {
    return { ok: false, error: "Bridge base URL is required." };
  }
  let parsed;
  try {
    parsed = new URL(raw);
  } catch (error) {
    return { ok: false, error: "Bridge base URL is invalid." };
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    return { ok: false, error: "Bridge base URL must be http or https." };
  }
  if (parsed.username || parsed.password) {
    return { ok: false, error: "Bridge base URL must not contain credentials." };
  }
  if (!["127.0.0.1", "localhost"].includes(parsed.hostname)) {
    return { ok: false, error: "Bridge base URL must be localhost or 127.0.0.1." };
  }
  const basePath = parsed.pathname.replace(/\/$/, "");
  return { ok: true, url: `${parsed.origin}${basePath}` };
}

function normalizeArenaOrigin(origin) {
  if (origin === "https://arena.ai" || origin === "https://lmarena.ai") {
    return origin;
  }
  return DEFAULT_SETTINGS.arenaPreferredOrigin;
}

function isAllowedJobUrl(rawUrl) {
  const url = String(rawUrl || "");
  if (!url) {
    return { ok: false, error: "Job URL is missing." };
  }
  if (url.startsWith("/")) {
    return { ok: true, url, originHint: null };
  }
  let parsed;
  try {
    parsed = new URL(url);
  } catch (error) {
    return { ok: false, error: "Job URL is invalid." };
  }
  if (parsed.protocol !== "https:" || !ALLOWED_HOSTS.has(parsed.hostname)) {
    return { ok: false, error: "Job URL is not in the allowlist." };
  }
  return { ok: true, url, originHint: `${parsed.protocol}//${parsed.hostname}` };
}

function nextBackoff() {
  pollBackoffSeconds = pollBackoffSeconds
    ? Math.min(pollBackoffSeconds * 2, MAX_BACKOFF_SECONDS)
    : MIN_BACKOFF_SECONDS;
  return pollBackoffSeconds;
}

function resetBackoff() {
  pollBackoffSeconds = 0;
}

async function updateStatus(update) {
  Object.assign(statusState, update);
  statusState.backoffSeconds = pollBackoffSeconds;
  await extensionApi.storage.local.set({ proxyStatus: { ...statusState } });
}

async function ensureArenaTab(preferredOrigin) {
  const tabs = await extensionApi.tabs.query({
    url: ["https://arena.ai/*", "https://lmarena.ai/*"],
  });
  if (tabs.length > 0) {
    const preferred = tabs.find((tab) => tab.url && tab.url.startsWith(preferredOrigin));
    return preferred || tabs[0];
  }
  const created = await extensionApi.tabs.create({ url: preferredOrigin });
  await waitForTabComplete(created.id);
  return created;
}

async function waitForTabComplete(tabId) {
  if (!tabId) {
    return;
  }
  const tab = await extensionApi.tabs.get(tabId);
  if (tab && tab.status === "complete") {
    return;
  }
  await new Promise((resolve) => {
    const listener = (updatedId, info) => {
      if (updatedId === tabId && info.status === "complete") {
        extensionApi.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    };
    extensionApi.tabs.onUpdated.addListener(listener);
  });
}

async function sendJobToTab(tabId, message) {
  for (let attempt = 0; attempt < MAX_SEND_ATTEMPTS; attempt += 1) {
    try {
      await extensionApi.tabs.sendMessage(tabId, message);
      return true;
    } catch (error) {
      if (attempt === MAX_SEND_ATTEMPTS - 1) {
        throw error;
      }
      await sleep(500);
    }
  }
  return false;
}

async function pushUpdate(jobId, update) {
  if (!activeJobContext) {
    return;
  }
  const { baseUrl, secret } = activeJobContext;
  const payload = { job_id: jobId, ...update };
  const headers = { "Content-Type": "application/json" };
  if (secret) {
    headers[SECRET_HEADER] = secret;
  }
  await fetch(`${baseUrl}${PUSH_PATH}`, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  });
}

async function finalizeJob(jobId) {
  activeJobContext = null;
  await updateStatus({ activeJobId: null, state: "idle" });
}

async function handleJob(job, settings, baseUrl) {
  const jobId = String(job.job_id || "").trim();
  const payload = job.payload || {};
  const urlCheck = isAllowedJobUrl(payload.url);
  if (!jobId) {
    return;
  }
  activeJobContext = {
    jobId,
    baseUrl,
    secret: String(settings.userscriptProxySecret || "").trim(),
  };
  await updateStatus({
    state: "processing",
    activeJobId: jobId,
    lastJobId: jobId,
    lastJobAt: new Date().toISOString(),
  });
  if (!urlCheck.ok) {
    await pushUpdate(jobId, { error: urlCheck.error, done: true });
    await finalizeJob(jobId);
    return;
  }

  const origin = normalizeArenaOrigin(urlCheck.originHint || settings.arenaPreferredOrigin);
  try {
    const tab = await ensureArenaTab(origin);
    if (!tab || !tab.id) {
      await pushUpdate(jobId, { error: "No Arena tab available.", done: true });
      await finalizeJob(jobId);
      return;
    }
    await waitForTabComplete(tab.id);
    await sendJobToTab(tab.id, {
      type: "lmbridge-proxy-job",
      jobId,
      payload,
    });
  } catch (error) {
    await pushUpdate(jobId, { error: "Failed to dispatch job to Arena tab.", done: true });
    await finalizeJob(jobId);
  }
}

async function pollOnce() {
  if (activeJobContext) {
    await sleep(250);
    return;
  }
  const settings = await getSettings();
  const normalized = normalizeBaseUrl(settings.bridgeBaseUrl);
  if (!normalized.ok) {
    await updateStatus({ state: "error", lastError: normalized.error });
    await sleep(5000);
    return;
  }
  const headers = { "Content-Type": "application/json" };
  const secret = String(settings.userscriptProxySecret || "").trim();
  if (secret) {
    headers[SECRET_HEADER] = secret;
  }
  const timeoutSeconds = Number(settings.pollTimeoutSeconds || DEFAULT_SETTINGS.pollTimeoutSeconds);
  const payload = { timeout_seconds: Number.isFinite(timeoutSeconds) ? timeoutSeconds : 25 };
  await updateStatus({ state: "polling", lastError: "" });
  try {
    const response = await fetch(`${normalized.url}${POLL_PATH}`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    statusState.lastPollAt = new Date().toISOString();
    if (response.status === 204) {
      resetBackoff();
      await updateStatus({ state: "idle" });
      return;
    }
    if (!response.ok) {
      throw new Error(`Poll failed with status ${response.status}.`);
    }
    const data = await response.json();
    resetBackoff();
    await handleJob(data, settings, normalized.url);
  } catch (error) {
    const backoff = nextBackoff();
    await updateStatus({
      state: "error",
      lastError: error instanceof Error ? error.message : String(error),
      backoffSeconds: backoff,
    });
    await sleep(backoff * 1000);
  }
}

async function pollLoop() {
  if (pollLoopRunning) {
    return;
  }
  pollLoopRunning = true;
  while (pollLoopEnabled) {
    await pollOnce();
  }
  pollLoopRunning = false;
}


extensionApi.runtime.onMessage.addListener((message) => {
  if (!message || message.type !== "lmbridge-proxy-update") {
    return undefined;
  }
  if (!activeJobContext || message.jobId !== activeJobContext.jobId) {
    return undefined;
  }
  const update = {
    status: message.status,
    headers: message.headers,
    lines: message.lines,
    error: message.error,
    done: message.done,
    upstream_fetch_started: message.upstream_fetch_started,
  };
  const jobId = activeJobContext.jobId;
  return (async () => {
    try {
      await pushUpdate(jobId, update);
    } catch (error) {
      await updateStatus({
        state: "error",
        lastError: "Failed to push updates to bridge.",
      });
    }
    if (message.done) {
      await finalizeJob(jobId);
    } else {
      await updateStatus({ state: "streaming" });
    }
  })();
});

pollLoop().catch((error) => {
  updateStatus({
    state: "error",
    lastError: error instanceof Error ? error.message : "Polling failed to start.",
  });
});

if (extensionApi.runtime.onSuspend) {
  extensionApi.runtime.onSuspend.addListener(() => {
    pollLoopEnabled = false;
    if (activeJobContext) {
      const jobId = activeJobContext.jobId;
      pushUpdate(jobId, {
        error: "Extension suspended before job completion.",
        done: true,
      }).catch(() => {});
      finalizeJob(jobId).catch(() => {});
    }
  });
}
