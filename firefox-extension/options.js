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
  return { ok: true, url: parsed.toString() };
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) {
    el.textContent = value;
  }
}

async function loadSettings() {
  const stored = await extensionApi.storage.local.get(DEFAULT_SETTINGS);
  document.getElementById("bridgeBaseUrl").value = stored.bridgeBaseUrl || "";
  document.getElementById("userscriptProxySecret").value = stored.userscriptProxySecret || "";
  document.getElementById("pollTimeoutSeconds").value = stored.pollTimeoutSeconds ?? 25;
  document.getElementById("arenaPreferredOrigin").value =
    stored.arenaPreferredOrigin || "https://lmarena.ai";
}

async function loadStatus() {
  const stored = await extensionApi.storage.local.get({ proxyStatus: {} });
  updateStatusDisplay(stored.proxyStatus || {});
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
  const errorEl = document.getElementById("validationError");
  const statusEl = document.getElementById("saveStatus");
  errorEl.textContent = "";
  statusEl.textContent = "";
  if (!validation.ok) {
    errorEl.textContent = validation.error;
    return;
  }
  const settings = {
    bridgeBaseUrl: baseUrl.trim(),
    userscriptProxySecret: document.getElementById("userscriptProxySecret").value,
    pollTimeoutSeconds: Number(document.getElementById("pollTimeoutSeconds").value || 25),
    arenaPreferredOrigin: document.getElementById("arenaPreferredOrigin").value,
  };
  await extensionApi.storage.local.set(settings);
  statusEl.textContent = "Settings saved.";
}

document.getElementById("saveButton").addEventListener("click", () => {
  saveSettings().catch(() => {});
});

extensionApi.storage.onChanged.addListener((changes, area) => {
  if (area !== "local") {
    return;
  }
  if (changes.proxyStatus) {
    updateStatusDisplay(changes.proxyStatus.newValue || {});
  }
});

loadSettings().catch(() => {});
loadStatus().catch(() => {});
