const extensionApi = typeof browser !== "undefined" ? browser : chrome;

const DEFAULT_SETTINGS = {
  bridgeBaseUrl: "http://127.0.0.1:8000",
  userscriptProxySecret: "",
  pollTimeoutSeconds: 25,
  arenaPreferredOrigin: "https://lmarena.ai",
};

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
  if (!["127.0.0.1", "localhost"].includes(parsed.hostname)) {
    return { ok: false, error: "Bridge base URL must be localhost or 127.0.0.1." };
  }
  const basePath = parsed.pathname.replace(/\/$/, "");
  return { ok: true, url: `${parsed.origin}${basePath}` };
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value;
  }
}

function setError(message) {
  setText("validationError", message || "");
}

function setSuccess(message) {
  setText("saveStatus", message || "");
}

async function loadSettings() {
  try {
    const stored = await extensionApi.storage.local.get(DEFAULT_SETTINGS);
    document.getElementById("bridgeBaseUrl").value = stored.bridgeBaseUrl || "";
    document.getElementById("userscriptProxySecret").value = stored.userscriptProxySecret || "";
    document.getElementById("pollTimeoutSeconds").value = stored.pollTimeoutSeconds;
    document.getElementById("arenaPreferredOrigin").value =
      stored.arenaPreferredOrigin || DEFAULT_SETTINGS.arenaPreferredOrigin;
  } catch (error) {
    setError("Failed to load settings.");
  }
}

async function loadStatus() {
  try {
    const stored = await extensionApi.storage.local.get({ proxyStatus: {} });
    updateStatusDisplay(stored.proxyStatus || {});
  } catch (error) {
    setError("Failed to load status.");
  }
}

function updateStatusDisplay(status) {
  setText("statusState", status.state || "idle");
  setText("statusError", status.lastError || "None");
  setText("statusPoll", status.lastPollAt || "Never");
  setText("statusJob", status.activeJobId || "None");
  setText("statusBackoff", `${status.backoffSeconds || 0}s`);
}

async function saveSettings() {
  const baseUrl = document.getElementById("bridgeBaseUrl").value;
  const validation = normalizeBaseUrl(baseUrl);
  setError("");
  setSuccess("");
  if (!validation.ok) {
    setError(validation.error);
    return;
  }
  const settings = {
    bridgeBaseUrl: baseUrl.trim(),
    userscriptProxySecret: document.getElementById("userscriptProxySecret").value,
    pollTimeoutSeconds: Number(document.getElementById("pollTimeoutSeconds").value || 25),
    arenaPreferredOrigin: document.getElementById("arenaPreferredOrigin").value,
  };
  try {
    await extensionApi.storage.local.set(settings);
    setSuccess("Settings saved.");
  } catch (error) {
    setError("Failed to save settings.");
  }
}

document.getElementById("saveButton").addEventListener("click", () => {
  saveSettings();
});

extensionApi.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") {
    return;
  }
  if (changes.proxyStatus) {
    updateStatusDisplay(changes.proxyStatus.newValue || {});
  }
});

loadSettings();
loadStatus();
