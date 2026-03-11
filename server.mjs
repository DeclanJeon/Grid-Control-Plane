import http from "node:http";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const port = Number(process.env.CONTROL_PORT || 19090);
const host = String(process.env.CONTROL_HOST || "127.0.0.1").trim() || "127.0.0.1";
const ttlSeconds = Number(process.env.PRESENCE_TTL_SECONDS || 40);
const authToken = String(process.env.CONTROL_AUTH_TOKEN || "dev-change-me").trim() || "dev-change-me";
const rateWindowMs = Number(process.env.RATE_LIMIT_WINDOW_MS || 60_000);
const rateLimitMax = Number(process.env.RATE_LIMIT_MAX || 120);
const dataFile = process.env.CONTROL_DATA_FILE
  ? path.resolve(process.env.CONTROL_DATA_FILE)
  : path.join(__dirname, "control-plane-data.json");
const requestedStorageBackend = String(process.env.CONTROL_STORAGE_BACKEND || "file").trim().toLowerCase() || "file";
const requestedQueueBackend = String(process.env.CONTROL_QUEUE_BACKEND || "inline").trim().toLowerCase() || "inline";
const queueJournalFile = process.env.CONTROL_QUEUE_JOURNAL_FILE
  ? path.resolve(process.env.CONTROL_QUEUE_JOURNAL_FILE)
  : `${dataFile}.ledger-journal`;
const pgDsn = String(process.env.CONTROL_PG_DSN || "").trim();
const pgTable = String(process.env.CONTROL_PG_TABLE || "control_plane_state").trim() || "control_plane_state";
const redisUrl = String(process.env.CONTROL_REDIS_URL || "").trim();
const bullmqQueue = String(process.env.CONTROL_BULLMQ_QUEUE || "grid-ledger-events").trim() || "grid-ledger-events";
const bullmqWorkerConcurrency = Math.max(1, Number(process.env.CONTROL_BULLMQ_WORKER_CONCURRENCY || 8));

const supportedStorageBackends = new Set(["file", "postgres"]);
const supportedQueueBackends = new Set(["inline", "file_journal", "redis_bullmq"]);

let storageBackendStatus = resolveStorageBackend(requestedStorageBackend);
let queueBackendStatus = resolveQueueBackend(requestedQueueBackend);
let effectiveStorageBackend = storageBackendStatus.effective;
let effectiveQueueBackend = queueBackendStatus.effective;

const backendRuntime = {
  storage: {
    implemented: false,
    initialized: false,
    error: "",
    pgClient: null,
  },
  queue: {
    implemented: false,
    initialized: false,
    error: "",
    bullmqQueue: null,
    bullmqWorker: null,
  },
};

function resolveStorageBackend(requested) {
  if (!supportedStorageBackends.has(requested)) {
    return { effective: "file", reason: "unsupported_requested_fallback" };
  }
  if (requested === "postgres" && !pgDsn) {
    return { effective: "file", reason: "missing_config_fallback" };
  }
  return { effective: requested, reason: "configured_supported" };
}

function resolveQueueBackend(requested) {
  if (!supportedQueueBackends.has(requested)) {
    return { effective: "inline", reason: "unsupported_requested_fallback" };
  }
  if (requested === "redis_bullmq" && !redisUrl) {
    return { effective: "inline", reason: "missing_config_fallback" };
  }
  return { effective: requested, reason: "configured_supported" };
}

function isSafeSqlIdentifier(name) {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(String(name || ""));
}

async function initializeStorageBackendRuntime() {
  backendRuntime.storage.implemented = false;
  backendRuntime.storage.initialized = false;
  backendRuntime.storage.error = "";

  if (effectiveStorageBackend !== "postgres") {
    backendRuntime.storage.implemented = true;
    backendRuntime.storage.initialized = true;
    return;
  }

  try {
    const mod = await import("pg");
    const PgClient = mod.Client;
    if (!PgClient) {
      throw new Error("pg.Client not available");
    }
    if (!isSafeSqlIdentifier(pgTable)) {
      throw new Error(`invalid CONTROL_PG_TABLE: ${pgTable}`);
    }
    const client = new PgClient({ connectionString: pgDsn });
    client.on("error", (err) => {
      backendRuntime.storage.error = String(err && err.message ? err.message : err);
      storageBackendStatus = { effective: "file", reason: "runtime_connection_lost_fallback" };
      effectiveStorageBackend = storageBackendStatus.effective;
      backendRuntime.storage.initialized = false;
      backendRuntime.storage.pgClient = null;
      console.warn(`[control-plane] postgres client error; fallback to file storage: ${backendRuntime.storage.error}`);
    });
    await client.connect();
    await client.query(
      `CREATE TABLE IF NOT EXISTS ${pgTable} (id INTEGER PRIMARY KEY, state_json TEXT NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW())`
    );
    backendRuntime.storage.pgClient = client;
    backendRuntime.storage.implemented = true;
    backendRuntime.storage.initialized = true;
  } catch (err) {
    backendRuntime.storage.error = String(err && err.message ? err.message : err);
    storageBackendStatus = { effective: "file", reason: "runtime_init_fallback" };
    effectiveStorageBackend = storageBackendStatus.effective;
  }
}

async function initializeQueueBackendRuntime() {
  backendRuntime.queue.implemented = false;
  backendRuntime.queue.initialized = false;
  backendRuntime.queue.error = "";

  if (effectiveQueueBackend !== "redis_bullmq") {
    backendRuntime.queue.implemented = true;
    backendRuntime.queue.initialized = true;
    return;
  }

  try {
    const mod = await import("bullmq");
    const Queue = mod.Queue;
    const Worker = mod.Worker;
    const redisMod = await import("ioredis");
    const Redis = redisMod.default;
    if (!Queue || !Worker) {
      throw new Error("bullmq Queue/Worker not available");
    }
    if (!Redis) {
      throw new Error("ioredis not available");
    }
    const probe = new Redis(redisUrl, {
      lazyConnect: true,
      maxRetriesPerRequest: 1,
      enableReadyCheck: false,
      connectTimeout: 1000,
    });
    await probe.connect();
    await probe.ping();
    await probe.quit();

    const connection = { url: redisUrl };
    const queue = new Queue(bullmqQueue, { connection });
    const worker = new Worker(
      bullmqQueue,
      async (job) => {
        const item = job.data || {};
        applyLedgerEventToRollup(item);
        scheduleSave();
      },
      { connection, concurrency: bullmqWorkerConcurrency }
    );
    worker.on("error", (err) => {
      backendRuntime.queue.error = String(err && err.message ? err.message : err);
      queueBackendStatus = { effective: "inline", reason: "runtime_connection_lost_fallback" };
      effectiveQueueBackend = queueBackendStatus.effective;
      backendRuntime.queue.initialized = false;
      backendRuntime.queue.bullmqQueue = null;
      backendRuntime.queue.bullmqWorker = null;
      console.warn(`[control-plane] bullmq worker error; fallback to inline queue: ${backendRuntime.queue.error}`);
    });
    backendRuntime.queue.bullmqQueue = queue;
    backendRuntime.queue.bullmqWorker = worker;
    backendRuntime.queue.implemented = true;
    backendRuntime.queue.initialized = true;
  } catch (err) {
    backendRuntime.queue.error = String(err && err.message ? err.message : err);
    queueBackendStatus = { effective: "inline", reason: "runtime_init_fallback" };
    effectiveQueueBackend = queueBackendStatus.effective;
  }
}

async function initializeBackendsRuntime() {
  await initializeStorageBackendRuntime();
  await initializeQueueBackendRuntime();
}

async function attemptBackendRepromote() {
  if (requestedStorageBackend === "postgres" && pgDsn && effectiveStorageBackend !== "postgres") {
    storageBackendStatus = { effective: "postgres", reason: "configured_supported" };
    effectiveStorageBackend = "postgres";
    await initializeStorageBackendRuntime();
    if (effectiveStorageBackend === "postgres" && backendRuntime.storage.initialized) {
      try {
        await saveState();
      } catch (err) {
        console.warn(`[control-plane] saveState after storage re-promote failed: ${String(err)}`);
      }
    }
  }

  if (requestedQueueBackend === "redis_bullmq" && redisUrl && effectiveQueueBackend !== "redis_bullmq") {
    queueBackendStatus = { effective: "redis_bullmq", reason: "configured_supported" };
    effectiveQueueBackend = "redis_bullmq";
    await initializeQueueBackendRuntime();
    if (effectiveQueueBackend === "redis_bullmq" && backendRuntime.queue.initialized) {
      const pending = ledgerEvents.slice(ledgerCursor);
      for (const item of pending) {
        const eventId = String(item.event_id || "").trim();
        if (eventId && processedRollupEventIds.has(eventId)) {
          continue;
        }
        try {
          await enqueueLedgerEvent(item);
        } catch (err) {
          console.warn(`[control-plane] replay enqueue after queue re-promote failed: ${String(err)}`);
          break;
        }
      }
    }
  }
}

const presence = new Map();
const files = new Map();
const messages = [];
const sessions = new Map();
const trustAttestations = [];
const ledgerEvents = [];
const processedLedgerEventIds = new Set();
const processedRollupEventIds = new Set();
const ledgerRollups = new Map();
const messageSendWindow = new Map();
const rateBucket = new Map();
const auditLogs = [];
const sessionPathPenaltyEma = new Map();
let ledgerCursor = 0;

let saveTimer = null;
let journalWriteChain = Promise.resolve();
const ledgerDecayAlpha = Math.max(0, Math.min(1, Number(process.env.LEDGER_DECAY_ALPHA || 0.9)));
const messageCooldownMs = Math.max(0, Number(process.env.MESSAGE_COOLDOWN_MS || 2000));
const sessionPathEmaAlpha = Math.max(0, Math.min(1, Number(process.env.SESSION_PATH_EMA_ALPHA || 0.7)));

if (process.env.NODE_ENV === "production" && authToken === "dev-change-me") {
  throw new Error("CONTROL_AUTH_TOKEN must be set in production");
}

function nowIso() {
  return new Date().toISOString();
}

function runtimeBackendsStatus() {
  const storageSupported = supportedStorageBackends.has(requestedStorageBackend);
  const queueSupported = supportedQueueBackends.has(requestedQueueBackend);
  return {
    storage: {
      requested: requestedStorageBackend,
      effective: effectiveStorageBackend,
      supported: [...supportedStorageBackends],
      migration_ready: storageSupported && requestedStorageBackend === effectiveStorageBackend,
      effective_reason: storageBackendStatus.reason,
      data_file: dataFile,
      pg_dsn_configured: Boolean(pgDsn),
      pg_table: pgTable,
      implemented: backendRuntime.storage.implemented,
      initialized: backendRuntime.storage.initialized,
      runtime_error: backendRuntime.storage.error,
    },
    queue: {
      requested: requestedQueueBackend,
      effective: effectiveQueueBackend,
      supported: [...supportedQueueBackends],
      migration_ready: queueSupported && requestedQueueBackend === effectiveQueueBackend,
      effective_reason: queueBackendStatus.reason,
      mode: effectiveQueueBackend === "file_journal"
        ? "process_local_journal"
        : effectiveQueueBackend === "redis_bullmq"
          ? "worker_queue"
          : "process_local",
      journal_file: queueJournalFile,
      redis_url_configured: Boolean(redisUrl),
      bullmq_queue: bullmqQueue,
      bullmq_worker_concurrency: bullmqWorkerConcurrency,
      implemented: backendRuntime.queue.implemented,
      initialized: backendRuntime.queue.initialized,
      runtime_error: backendRuntime.queue.error,
    },
  };
}

function queueJournalAppend(entry) {
  if (effectiveQueueBackend !== "file_journal") {
    return;
  }
  const line = `${JSON.stringify(entry)}\n`;
  journalWriteChain = journalWriteChain
    .then(() => fs.appendFile(queueJournalFile, line, "utf8"))
    .catch((err) => {
      console.warn(`[control-plane] ledger journal append failed: ${String(err)}`);
    });
}

function runtimeReadinessStatus() {
  const backends = runtimeBackendsStatus();
  const blockers = [];

  if (authToken === "dev-change-me") {
    blockers.push("default_auth_token_in_use");
  }
  if (backends.storage.effective !== backends.storage.requested) {
    blockers.push("storage_backend_fallback_active");
  }
  if (backends.queue.effective !== backends.queue.requested) {
    blockers.push("queue_backend_fallback_active");
  }
  if (backends.storage.requested === "postgres" && !backends.storage.pg_dsn_configured) {
    blockers.push("storage_backend_config_missing");
  }
  if (backends.queue.requested === "redis_bullmq" && !backends.queue.redis_url_configured) {
    blockers.push("queue_backend_config_missing");
  }
  if (!backends.storage.implemented) {
    blockers.push("storage_backend_runtime_not_implemented");
  }
  if (!backends.queue.implemented) {
    blockers.push("queue_backend_runtime_not_implemented");
  }
  if (backends.storage.effective === "file") {
    blockers.push("storage_not_externalized");
  }
  if (backends.queue.effective === "inline") {
    blockers.push("queue_not_externalized");
  }

  return {
    ready_for_production: blockers.length === 0,
    blocker_count: blockers.length,
    blockers,
    backends,
  };
}

function keepMax(arr, max) {
  if (arr.length > max) {
    arr.splice(0, arr.length - max);
  }
}

function addAudit(entry) {
  auditLogs.push({ at: nowIso(), ...entry });
  keepMax(auditLogs, 5000);
}

async function readJson(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on("data", (chunk) => chunks.push(chunk));
    req.on("end", () => {
      try {
        const raw = Buffer.concat(chunks).toString("utf8");
        if (!raw.trim()) {
          resolve({});
          return;
        }
        resolve(JSON.parse(raw));
      } catch (err) {
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, statusCode, payload) {
  res.writeHead(statusCode, {
    ...corsHeaders(res.req),
    "content-type": "application/json; charset=utf-8",
  });
  res.end(JSON.stringify(payload));
}

function allowedOrigin(origin) {
  const value = String(origin || "").trim();
  if (!value) return "";
  if (value === "http://tauri.localhost" || value === "tauri://localhost") {
    return value;
  }
  if (/^https?:\/\/(localhost|127\.0\.0\.1)(:\d+)?$/i.test(value)) {
    return value;
  }
  return "";
}

function corsHeaders(req) {
  const origin = allowedOrigin(req?.headers?.origin);
  const headers = {
    Vary: "Origin",
  };
  if (origin) {
    headers["Access-Control-Allow-Origin"] = origin;
    headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, X-Grid-Role, X-Grid-Token";
    headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS";
  }
  return headers;
}

function clientIp(req) {
  const xf = req.headers["x-forwarded-for"];
  if (typeof xf === "string" && xf.trim()) {
    return xf.split(",")[0].trim();
  }
  return req.socket.remoteAddress || "unknown";
}

function extractToken(req) {
  const auth = String(req.headers.authorization || "");
  if (auth.startsWith("Bearer ")) {
    return auth.slice("Bearer ".length).trim();
  }
  return String(req.headers["x-grid-token"] || "").trim();
}

function checkRateLimit(req) {
  const ip = clientIp(req);
  const token = extractToken(req) || "anon";
  const tokenKey = token.slice(0, 16);
  const key = `${ip}:${tokenKey}:${Math.floor(Date.now() / rateWindowMs)}`;
  const n = (rateBucket.get(key) || 0) + 1;
  rateBucket.set(key, n);
  return n <= rateLimitMax;
}

function isAuthorized(req) {
  return extractToken(req) === authToken;
}

function isAdminRequest(pathname) {
  return pathname.startsWith("/v1/admin/");
}

function hasAdminRole(req) {
  const role = String(req.headers["x-grid-role"] || "").trim().toLowerCase();
  return role === "admin";
}

function shouldRequireAuth(pathname) {
  if (pathname === "/health") return false;
  return pathname.startsWith("/v1/");
}

function collectPresenceSnapshot() {
  const now = Date.now();
  const items = [];
  for (const [key, row] of presence.entries()) {
    if (row.expiresAt <= now) {
      presence.delete(key);
      continue;
    }
    items.push({
      instance_id: row.instance_id,
      host: row.host,
      app: row.app,
      role: row.role,
      mode: row.mode,
      broker_url: row.broker_url,
      active: row.active,
      updated_at: row.updated_at,
      expires_at: new Date(row.expiresAt).toISOString(),
    });
  }
  return items;
}

function upsertPresenceFromSync(payload) {
  const instanceId = String(payload.instance_id || "").trim() || `inst-${randomUUID()}`;
  const host = String(payload.host || "unknown-host").trim() || "unknown-host";
  const app = String(payload.app || "desktop-tauri").trim() || "desktop-tauri";
  const nodes = Array.isArray(payload.nodes) ? payload.nodes : [];

  const now = Date.now();
  for (const key of presence.keys()) {
    if (key.startsWith(`${instanceId}:`)) {
      presence.delete(key);
    }
  }

  for (const node of nodes) {
    const role = String(node.role || "peer").trim() || "peer";
    const mode = String(node.mode || "runtime").trim() || "runtime";
    const key = `${instanceId}:${role}:${mode}`;
    presence.set(key, {
      instance_id: instanceId,
      host,
      app,
      role,
      mode,
      broker_url: String(node.broker_url || "").trim(),
      active: Boolean(node.active),
      updated_at: nowIso(),
      expiresAt: now + ttlSeconds * 1000,
    });
  }
}

function scheduleSave() {
  if (saveTimer) {
    clearTimeout(saveTimer);
  }
  saveTimer = setTimeout(() => {
    saveTimer = null;
    void saveState().catch((err) => {
      console.warn(`[control-plane] saveState failed: ${String(err)}`);
    });
  }, 250);
}

async function saveState() {
  const payload = {
    files: [...files.values()],
    messages,
    sessions: [...sessions.values()],
    trustAttestations,
    ledgerEvents,
    ledgerRollups: [...ledgerRollups.entries()],
    sessionPathPenaltyEma: [...sessionPathPenaltyEma.entries()],
  };
  if (effectiveStorageBackend === "postgres" && backendRuntime.storage.pgClient) {
    const text = JSON.stringify(payload);
    await backendRuntime.storage.pgClient.query(
      `INSERT INTO ${pgTable}(id, state_json, updated_at) VALUES (1, $1, NOW()) ON CONFLICT (id) DO UPDATE SET state_json = EXCLUDED.state_json, updated_at = NOW()`,
      [text]
    );
    return;
  }
  const tmp = `${dataFile}.tmp`;
  await fs.writeFile(tmp, JSON.stringify(payload, null, 2), "utf8");
  await fs.rename(tmp, dataFile);
}

async function loadState() {
  let loaded = false;
  if (effectiveStorageBackend === "postgres" && backendRuntime.storage.pgClient) {
    try {
      const result = await backendRuntime.storage.pgClient.query(
        `SELECT state_json FROM ${pgTable} WHERE id = 1`
      );
      if (result.rows.length > 0 && result.rows[0].state_json) {
        const payload = JSON.parse(String(result.rows[0].state_json));
        hydrateFromPayload(payload);
        loaded = true;
      }
    } catch {
      // postgres path unavailable; proceed to file fallback
    }
  }

  if (!loaded) {
    try {
      const text = await fs.readFile(dataFile, "utf8");
      const payload = JSON.parse(text);
      hydrateFromPayload(payload);
    } catch {
      // no persisted state yet
    }
  }

  if (effectiveQueueBackend === "file_journal") {
    try {
      const text = await fs.readFile(queueJournalFile, "utf8");
      const lines = text.split("\n").filter((x) => x.trim().length > 0);
      for (const line of lines) {
        try {
          const raw = JSON.parse(line);
          const item = makeLedgerEvent(raw || {});
          if (item.instance_id && item.type && Number.isFinite(item.value)) {
            upsertLedgerEventIfNew(item, "replay");
          }
        } catch {
          // skip invalid journal line
        }
      }
      processLedgerWorkerBatch(1000);
    } catch {
      // no journal yet
    }
  }
}

function hydrateFromPayload(payload) {
  for (const f of payload.files || []) {
    if (f?.file_id) files.set(f.file_id, f);
  }
  for (const m of payload.messages || []) {
    messages.push(m);
  }
  for (const s of payload.sessions || []) {
    if (s?.session_id) sessions.set(s.session_id, s);
  }
  for (const t of payload.trustAttestations || []) {
    trustAttestations.push(t);
  }
  for (const e of payload.ledgerEvents || []) {
    ledgerEvents.push(e);
    if (e?.external_event_id) {
      processedLedgerEventIds.add(e.external_event_id);
    }
    if (e?.event_id) {
      processedRollupEventIds.add(e.event_id);
    }
  }
  for (const [instanceId, rollup] of payload.ledgerRollups || []) {
    if (instanceId && rollup && typeof rollup === "object") {
      ledgerRollups.set(instanceId, {
        instance_id: String(rollup.instance_id || instanceId),
        score: Number(rollup.score || 0),
        event_count: Number(rollup.event_count || 0),
        by_type: rollup.by_type && typeof rollup.by_type === "object" ? rollup.by_type : {},
        updated_at: String(rollup.updated_at || nowIso()),
      });
    }
  }
  for (const [key, value] of payload.sessionPathPenaltyEma || []) {
    const k = String(key || "").trim();
    const v = Number(value || 0);
    if (k && Number.isFinite(v)) {
      sessionPathPenaltyEma.set(k, Math.max(0, Math.min(1, v)));
    }
  }
  ledgerCursor = ledgerEvents.length;
}

function normalizeLimit(value, fallback = 100, max = 500) {
  const n = Number(value || fallback);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

function fileTrustScore(fileId) {
  const forFile = trustAttestations.filter((x) => x.file_id === fileId);
  if (forFile.length === 0) return 0.5;
  const score = forFile.reduce((acc, x) => acc + (x.verdict === "safe" ? 1 : -1), 0);
  const normalized = (score / (forFile.length || 1) + 1) / 2;
  return Math.max(0, Math.min(1, normalized));
}

function fileTrustEvidence(fileId) {
  const forFile = trustAttestations.filter((x) => x.file_id === fileId);
  const safeCount = forFile.filter((x) => x.verdict === "safe").length;
  const unsafeCount = forFile.filter((x) => x.verdict === "unsafe").length;
  const latestReason = String(forFile[forFile.length - 1]?.reason || "").trim();
  const score = fileTrustScore(fileId);
  const status = score >= 0.85
    ? "safe_verified"
    : score >= 0.65
      ? "community_trusted"
      : score >= 0.45
        ? "unverified"
        : "risk_flagged";
  return {
    trust_score: score,
    trust_status: status,
    risk_flagged: status === "risk_flagged",
    safe_count: safeCount,
    unsafe_count: unsafeCount,
    latest_reason: latestReason,
  };
}

function filePolicyScore(file) {
  const trust = fileTrustScore(file.file_id);
  const owner = String(file.owner_instance_id || "").trim();
  const ownerRollup = owner ? ledgerRollups.get(owner) : null;
  const contribution = Number(ownerRollup?.score || 0);
  const contributionNorm = (Math.max(-100, Math.min(100, contribution)) + 100) / 200;
  const downloadNorm = Math.max(0, Math.min(1, Number(file.download_count || 0) / 100));
  return Number((0.7 * trust + 0.2 * contributionNorm + 0.1 * downloadNorm).toFixed(4));
}

function providerAvailabilityWeights(providerInstanceId, fileTrust) {
  const providerId = String(providerInstanceId || "").trim();
  if (!providerId) {
    return {
      uptime_weight: 0,
      relay_success_weight: 0,
      trust_weight: Number(fileTrust || 0),
    };
  }

  const providerPresence = collectPresenceSnapshot().filter((x) => x.instance_id === providerId);
  const activeCount = providerPresence.filter((x) => Boolean(x.active)).length;
  const uptimeWeight = providerPresence.length > 0 ? activeCount / providerPresence.length : 0;

  const rollup = ledgerRollups.get(providerId);
  const relaySuccessCount = Number(rollup?.by_type?.relay_success || 0);
  const relaySuccessWeight = Math.max(0, Math.min(1, relaySuccessCount / 20));

  return {
    uptime_weight: Number(uptimeWeight.toFixed(4)),
    relay_success_weight: Number(relaySuccessWeight.toFixed(4)),
    trust_weight: Number(Number(fileTrust || 0).toFixed(4)),
  };
}

function providerCandidatesForFile(file) {
  const providers = Array.isArray(file.providers) ? file.providers : [];
  const trust = fileTrustScore(file.file_id);
  const items = providers.map((providerId) => {
    const weights = providerAvailabilityWeights(providerId, trust);
    const availability = weights.uptime_weight + weights.relay_success_weight + weights.trust_weight;
    return {
      instance_id: providerId,
      ...weights,
      availability_score: Number(availability.toFixed(4)),
    };
  });
  return items.sort((a, b) => Number(b.availability_score || 0) - Number(a.availability_score || 0));
}

function sessionPathPenaltyKey(providerInstanceId, pathState) {
  const provider = String(providerInstanceId || "").trim();
  const path = String(pathState || "direct").trim().toLowerCase() || "direct";
  return `${provider}:${path}`;
}

function getSessionPathPenalty(providerInstanceId, pathState) {
  const key = sessionPathPenaltyKey(providerInstanceId, pathState);
  return Number(sessionPathPenaltyEma.get(key) || 0);
}

function updateSessionPathPenalty(providerInstanceId, pathState, failed) {
  const provider = String(providerInstanceId || "").trim();
  if (!provider) return 0;
  const key = sessionPathPenaltyKey(provider, pathState);
  const prev = Number(sessionPathPenaltyEma.get(key) || 0);
  const observed = failed ? 1 : 0;
  const next = sessionPathEmaAlpha * prev + (1 - sessionPathEmaAlpha) * observed;
  const normalized = Math.max(0, Math.min(1, Number(next.toFixed(4))));
  sessionPathPenaltyEma.set(key, normalized);
  return normalized;
}

function normalizeSessionCandidates(payload, fileId) {
  const file = files.get(fileId);
  const byProvider = new Map();
  if (file) {
    for (const x of providerCandidatesForFile(file)) {
      byProvider.set(String(x.instance_id || "").trim(), x);
    }
  }

  const raw = Array.isArray(payload.provider_candidates) ? payload.provider_candidates : [];
  const source = raw.length > 0 ? raw : file ? file.providers || [] : [];
  const candidates = [];

  for (const entry of source) {
    const fromObject = entry && typeof entry === "object";
    const instanceId = String(
      fromObject ? entry.instance_id || entry.provider_instance_id || "" : entry || ""
    ).trim();
    if (!instanceId) continue;
    const pathState = String(fromObject ? entry.path_state || entry.path || "direct" : "direct").trim() || "direct";
    const known = byProvider.get(instanceId);
    const availabilityScore = Number(
      fromObject && Number.isFinite(Number(entry.availability_score))
        ? Number(entry.availability_score)
        : Number(known?.availability_score || 0)
    );
    const penalty = getSessionPathPenalty(instanceId, pathState);
    const assignmentScore = Number((availabilityScore - penalty).toFixed(4));
    candidates.push({
      instance_id: instanceId,
      path_state: pathState,
      availability_score: Number(availabilityScore.toFixed(4)),
      path_penalty_ema: Number(penalty.toFixed(4)),
      assignment_score: assignmentScore,
    });
  }

  candidates.sort((a, b) => Number(b.assignment_score || 0) - Number(a.assignment_score || 0));
  return candidates;
}

function makeTrigrams(value) {
  const text = String(value || "").trim().toLowerCase();
  if (!text) return [];
  if (text.length < 3) return [text];
  const grams = [];
  for (let i = 0; i <= text.length - 3; i += 1) {
    grams.push(text.slice(i, i + 3));
  }
  return grams;
}

function trigramSimilarity(a, b) {
  const aGrams = makeTrigrams(a);
  const bGrams = makeTrigrams(b);
  if (aGrams.length === 0 || bGrams.length === 0) return 0;
  const bSet = new Set(bGrams);
  let overlap = 0;
  for (const gram of aGrams) {
    if (bSet.has(gram)) {
      overlap += 1;
    }
  }
  const denom = Math.max(aGrams.length, bGrams.length);
  return overlap / denom;
}

function fileSearchScore(file, query) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return 0;
  const name = String(file.name || "").trim().toLowerCase();
  const fileId = String(file.file_id || "").trim().toLowerCase();
  const prefix = name.startsWith(q) ? 1 : name.includes(q) ? 0.5 : 0;
  const trigram = trigramSimilarity(name, q);
  const idBoost = fileId.includes(q) ? 0.4 : 0;
  const score = 0.65 * prefix + 0.35 * trigram + idBoost;
  return Number(score.toFixed(4));
}

function makeSession(payload) {
  return {
    session_id: `sess-${randomUUID()}`,
    file_id: String(payload.file_id || "").trim(),
    requester_instance_id: String(payload.requester_instance_id || "").trim(),
    provider_candidates: Array.isArray(payload.provider_candidates) ? payload.provider_candidates : [],
    status: String(payload.status || "created"),
    path_state: String(payload.path_state || "direct").trim() || "direct",
    failure_reason: String(payload.failure_reason || "").trim(),
    recovery_progress: Math.max(0, Math.min(100, Number(payload.recovery_progress || 0) || 0)),
    last_path_update_at: nowIso(),
    created_at: nowIso(),
    updated_at: nowIso(),
    expires_at: new Date(Date.now() + ttlSeconds * 1000).toISOString(),
  };
}

function cleanupExpiredSessions() {
  const now = Date.now();
  for (const [id, s] of sessions.entries()) {
    if (Date.parse(s.expires_at) <= now) {
      sessions.delete(id);
    }
  }
}

function ledgerDeltaForEvent(type, value) {
  const t = String(type || "").trim();
  const n = Number(value || 0);
  if (!Number.isFinite(n)) return 0;
  if (t === "upload_bytes") return n / (1024 * 1024);
  if (t === "relay_success") return n * 3;
  if (t === "availability_minutes") return n * 0.2;
  if (t === "verified_helpful_attestation") return n * 4;
  if (t === "abuse_penalty") return -Math.abs(n);
  return n;
}

function applyLedgerEventToRollup(item) {
  const eventId = String(item.event_id || "").trim();
  if (eventId) {
    if (processedRollupEventIds.has(eventId)) {
      return;
    }
    processedRollupEventIds.add(eventId);
  }
  const key = String(item.instance_id || "").trim();
  if (!key) return;
  const prev = ledgerRollups.get(key) || {
    instance_id: key,
    score: 0,
    event_count: 0,
    by_type: {},
    updated_at: nowIso(),
  };
  const delta = ledgerDeltaForEvent(item.type, item.value);
  const nextScore = ledgerDecayAlpha * Number(prev.score || 0) + delta;
  const byType = { ...(prev.by_type || {}) };
  byType[item.type] = Number(byType[item.type] || 0) + 1;
  ledgerRollups.set(key, {
    instance_id: key,
    score: nextScore,
    event_count: Number(prev.event_count || 0) + 1,
    by_type: byType,
    updated_at: nowIso(),
  });
}

function processLedgerWorkerBatch(batchSize = 500) {
  let processed = 0;
  while (ledgerCursor < ledgerEvents.length && processed < batchSize) {
    const item = ledgerEvents[ledgerCursor];
    if (item) {
      applyLedgerEventToRollup(item);
    }
    ledgerCursor += 1;
    processed += 1;
  }
  if (processed > 0) {
    scheduleSave();
  }
  return processed;
}

async function enqueueLedgerEvent(item) {
  if (effectiveQueueBackend !== "redis_bullmq") {
    return;
  }
  const eventId = String(item.event_id || "").trim();
  if (eventId && processedRollupEventIds.has(eventId)) {
    return;
  }
  const q = backendRuntime.queue.bullmqQueue;
  if (!q) {
    return;
  }
  const jobId = String(item.external_event_id || item.event_id || randomUUID());
  try {
    await q.add("ledger-event", item, {
      jobId,
      removeOnComplete: 1000,
      removeOnFail: 1000,
      attempts: 3,
    });
  } catch (err) {
    backendRuntime.queue.error = String(err && err.message ? err.message : err);
    queueBackendStatus = { effective: "inline", reason: "enqueue_error_fallback" };
    effectiveQueueBackend = queueBackendStatus.effective;
    backendRuntime.queue.initialized = false;
    backendRuntime.queue.bullmqQueue = null;
    backendRuntime.queue.bullmqWorker = null;
    console.warn(`[control-plane] enqueue failed; fallback to inline queue: ${backendRuntime.queue.error}`);
    throw err;
  }
}

function makeLedgerEvent(body) {
  const externalEventId = String(body.external_event_id || body.event_id || body.nonce || "").trim();
  return {
    event_id: `led-${randomUUID()}`,
    external_event_id: externalEventId,
    instance_id: String(body.instance_id || body.peer_id || "").trim(),
    type: String(body.type || body.event_type || "").trim(),
    value: Number(body.value || 0),
    service_id: String(body.service_id || "").trim(),
    content_id: String(body.content_id || "").trim(),
    created_at: nowIso(),
  };
}

function upsertLedgerEventIfNew(item, source = "live") {
  const id = String(item.external_event_id || "").trim();
  if (id && processedLedgerEventIds.has(id)) {
    return false;
  }
  ledgerEvents.push(item);
  keepMax(ledgerEvents, 50_000);
  if (id) processedLedgerEventIds.add(id);
  if (source === "live") {
    queueJournalAppend(item);
    void enqueueLedgerEvent(item).catch((err) => {
      console.warn(`[control-plane] enqueueLedgerEvent failed: ${String(err)}`);
    });
  }
  return true;
}

setInterval(cleanupExpiredSessions, 5_000);
setInterval(() => {
  processLedgerWorkerBatch(500);
}, 5_000);
setInterval(() => {
  void attemptBackendRepromote().catch((err) => {
    console.warn(`[control-plane] backend re-promote attempt failed: ${String(err)}`);
  });
}, 5_000);

await initializeBackendsRuntime();
await loadState();

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || "/", `http://${req.headers.host || "127.0.0.1"}`);

  if (req.method === "OPTIONS") {
    res.writeHead(204, corsHeaders(req));
    res.end();
    return;
  }

  if (!checkRateLimit(req)) {
    addAudit({ method: req.method, path: url.pathname, result: "rate_limited", ip: clientIp(req) });
    sendJson(res, 429, { error: "rate limit exceeded" });
    return;
  }

  if (shouldRequireAuth(url.pathname) && !isAuthorized(req)) {
    addAudit({ method: req.method, path: url.pathname, result: "unauthorized", ip: clientIp(req) });
    sendJson(res, 401, { error: "unauthorized" });
    return;
  }

  if (isAdminRequest(url.pathname) && !hasAdminRole(req)) {
    addAudit({ method: req.method, path: url.pathname, result: "forbidden", ip: clientIp(req) });
    sendJson(res, 403, { error: "forbidden" });
    return;
  }

  if (req.method === "GET" && url.pathname === "/health") {
    sendJson(res, 200, {
      ok: true,
      service: "grid-control-plane",
      time: nowIso(),
      auth_required: true,
      auth_default_token_in_use: authToken === "dev-change-me",
    });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/presence/snapshot") {
    const items = collectPresenceSnapshot();
    sendJson(res, 200, { items, count: items.length, ttl_seconds: ttlSeconds });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/presence/sync") {
    try {
      const payload = await readJson(req);
      upsertPresenceFromSync(payload);
      const items = collectPresenceSnapshot();
      addAudit({ method: req.method, path: url.pathname, result: "ok", count: items.length, ip: clientIp(req) });
      sendJson(res, 200, { ok: true, count: items.length });
      return;
    } catch (err) {
      sendJson(res, 400, { error: `invalid payload: ${String(err)}` });
      return;
    }
  }

  if (req.method === "POST" && url.pathname === "/v1/files/register") {
    try {
      const body = await readJson(req);
      const ownerInstanceId = String(body.owner_instance_id || "").trim();
      const name = String(body.name || "").trim();
      const size = Number(body.size || 0);
      if (!ownerInstanceId || !name || !Number.isFinite(size) || size <= 0) {
        sendJson(res, 400, { error: "owner_instance_id, name, size are required" });
        return;
      }
      const file = {
        file_id: `file-${randomUUID()}`,
        owner_instance_id: ownerInstanceId,
        name,
        size: Math.floor(size),
        sha256: String(body.sha256 || "").trim(),
        piece_size: Number(body.piece_size || 0),
        piece_count: Number(body.piece_count || 0),
        download_count: 0,
        providers: [ownerInstanceId],
        created_at: nowIso(),
        updated_at: nowIso(),
      };
      files.set(file.file_id, file);
      scheduleSave();
      addAudit({ method: req.method, path: url.pathname, result: "ok", file_id: file.file_id, ip: clientIp(req) });
      sendJson(res, 200, { ok: true, item: file });
      return;
    } catch (err) {
      addAudit({ method: req.method, path: url.pathname, result: "invalid_payload", ip: clientIp(req) });
      sendJson(res, 400, { error: `invalid payload: ${String(err)}` });
      return;
    }
  }

  if (req.method === "GET" && url.pathname === "/v1/files") {
    const limit = normalizeLimit(url.searchParams.get("limit"), 100, 1000);
    const query = String(url.searchParams.get("query") || "").trim().toLowerCase();
    const rankingMode = String(url.searchParams.get("ranking_mode") || "default").trim().toLowerCase();
    let items = [...files.values()];
    if (query) {
      items = items
        .map((x) => ({ ...x, search_score: fileSearchScore(x, query) }))
        .filter((x) => Number(x.search_score || 0) > 0);
    }
    if (rankingMode === "policy") {
      items = items
        .map((x) => ({
          ...x,
          policy_score: Number.isFinite(Number(x.policy_score)) ? Number(x.policy_score) : filePolicyScore(x),
        }))
        .sort((a, b) => {
          const policyDiff = Number(b.policy_score || 0) - Number(a.policy_score || 0);
          if (query) {
            const searchDiff = Number(b.search_score || 0) - Number(a.search_score || 0);
            if (searchDiff !== 0) return searchDiff;
          }
          if (policyDiff !== 0) return policyDiff;
          return String(a.name || "").localeCompare(String(b.name || ""));
        });
    } else if (query) {
      items = items.sort((a, b) => {
        const diff = Number(b.search_score || 0) - Number(a.search_score || 0);
        if (diff !== 0) return diff;
        return String(a.name || "").localeCompare(String(b.name || ""));
      });
    }
    const sliced = items.slice(0, limit).map((x) => {
      const policyScore = Number.isFinite(Number(x.policy_score)) ? Number(x.policy_score) : filePolicyScore(x);
      return {
        ...x,
        ...fileTrustEvidence(x.file_id),
        policy_score: policyScore,
        search_score: Number(x.search_score || 0),
        provider_candidates: providerCandidatesForFile(x),
      };
    });
    sendJson(res, 200, { items: sliced, count: sliced.length });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/files/mine") {
    const instanceId = String(url.searchParams.get("instance_id") || "").trim();
    if (!instanceId) {
      sendJson(res, 400, { error: "instance_id is required" });
      return;
    }
    const limit = normalizeLimit(url.searchParams.get("limit"), 100, 1000);
    const items = [...files.values()].filter((x) => x.owner_instance_id === instanceId).slice(0, limit);
    sendJson(res, 200, { items, count: items.length });
    return;
  }

  const downloadedMatch = url.pathname.match(/^\/v1\/files\/([^/]+)\/downloaded$/);
  if (req.method === "POST" && downloadedMatch) {
    const fileId = decodeURIComponent(downloadedMatch[1]);
    const f = files.get(fileId);
    if (!f) {
      sendJson(res, 404, { error: "file not found" });
      return;
    }
    const body = await readJson(req);
    const downloader = String(body.downloader_instance_id || "").trim();
    f.download_count = Number(f.download_count || 0) + 1;
    if (downloader && !f.providers.includes(downloader)) {
      f.providers.push(downloader);
    }
    f.updated_at = nowIso();
    files.set(fileId, f);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", file_id: fileId, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, item: { ...f, ...fileTrustEvidence(fileId) } });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/messages/send") {
    const body = await readJson(req);
    const fileId = String(body.file_id || "").trim();
    const from = String(body.from_instance_id || "").trim();
    const to = String(body.to_instance_id || "").trim();
    const text = String(body.body || "").trim();
    if (!fileId || !from || !to || !text) {
      sendJson(res, 400, { error: "file_id, from_instance_id, to_instance_id, body are required" });
      return;
    }

    const threadKey = `${fileId}:${from}:${to}`;
    const now = Date.now();
    const lastSentAt = Number(messageSendWindow.get(threadKey) || 0);
    const waitMs = lastSentAt + messageCooldownMs - now;
    if (waitMs > 0) {
      sendJson(res, 429, {
        error: "message cooldown active",
        retry_after_ms: waitMs,
      });
      return;
    }

    const item = {
      message_id: `msg-${randomUUID()}`,
      file_id: fileId,
      from_instance_id: from,
      to_instance_id: to,
      body: text,
      created_at: nowIso(),
    };
    messages.push(item);
    messageSendWindow.set(threadKey, now);
    keepMax(messages, 10_000);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", file_id: fileId, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, item });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/messages/thread") {
    const fileId = String(url.searchParams.get("file_id") || "").trim();
    const a = String(url.searchParams.get("peer_a") || "").trim();
    const b = String(url.searchParams.get("peer_b") || "").trim();
    if (!fileId || !a || !b) {
      sendJson(res, 400, { error: "file_id, peer_a, peer_b are required" });
      return;
    }
    const limit = normalizeLimit(url.searchParams.get("limit"), 100, 1000);
    const items = messages
      .filter(
        (m) =>
          m.file_id === fileId &&
          ((m.from_instance_id === a && m.to_instance_id === b) ||
            (m.from_instance_id === b && m.to_instance_id === a))
      )
      .slice(-limit);
    sendJson(res, 200, { items, count: items.length });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/sessions/create") {
    const body = await readJson(req);
    const s = makeSession(body);
    if (!s.file_id || !s.requester_instance_id) {
      sendJson(res, 400, { error: "file_id and requester_instance_id are required" });
      return;
    }
    const candidates = normalizeSessionCandidates(body, s.file_id);
    s.provider_candidates = candidates;
    s.selected_provider_instance_id = String(candidates[0]?.instance_id || "").trim();
    s.path_state = String(candidates[0]?.path_state || s.path_state || "direct").trim() || "direct";
    s.path_penalty_ema = Number(candidates[0]?.path_penalty_ema || 0);
    sessions.set(s.session_id, s);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", session_id: s.session_id, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, item: s });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/sessions") {
    const requesterInstanceId = String(url.searchParams.get("requester_instance_id") || "").trim();
    const limit = normalizeLimit(url.searchParams.get("limit"), 100, 1000);
    let items = [...sessions.values()];
    if (requesterInstanceId) {
      items = items.filter((x) => x.requester_instance_id === requesterInstanceId);
    }
    items = items
      .sort((a, b) => Date.parse(b.updated_at || 0) - Date.parse(a.updated_at || 0))
      .slice(0, limit);
    sendJson(res, 200, { items, count: items.length });
    return;
  }

  const sessionMatch = url.pathname.match(/^\/v1\/sessions\/([^/]+)$/);
  if (sessionMatch && req.method === "GET") {
    const id = decodeURIComponent(sessionMatch[1]);
    const item = sessions.get(id);
    if (!item) {
      sendJson(res, 404, { error: "session not found" });
      return;
    }
    sendJson(res, 200, { item });
    return;
  }

  if (sessionMatch && req.method === "POST") {
    const id = decodeURIComponent(sessionMatch[1]);
    const item = sessions.get(id);
    if (!item) {
      sendJson(res, 404, { error: "session not found" });
      return;
    }
    const body = await readJson(req);
    item.status = String(body.status || item.status || "active");
    item.path_state = String(body.path_state || item.path_state || "direct").trim() || "direct";
    if (Object.prototype.hasOwnProperty.call(body, "failure_reason")) {
      item.failure_reason = String(body.failure_reason || "").trim();
    }
    if (Object.prototype.hasOwnProperty.call(body, "recovery_progress")) {
      item.recovery_progress = Math.max(0, Math.min(100, Number(body.recovery_progress || 0) || 0));
    }
    if (Object.prototype.hasOwnProperty.call(body, "selected_provider_instance_id")) {
      item.selected_provider_instance_id = String(body.selected_provider_instance_id || "").trim();
    }
    const failed = Boolean(item.failure_reason);
    const penalty = updateSessionPathPenalty(item.selected_provider_instance_id, item.path_state, failed);
    item.path_penalty_ema = Number(penalty || 0);
    item.last_path_update_at = nowIso();
    item.updated_at = nowIso();
    item.expires_at = new Date(Date.now() + ttlSeconds * 1000).toISOString();
    sessions.set(id, item);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", session_id: id, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, item });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/trust/attest") {
    const body = await readJson(req);
    const item = {
      attestation_id: `att-${randomUUID()}`,
      file_id: String(body.file_id || "").trim(),
      reporter_instance_id: String(body.reporter_instance_id || "").trim(),
      verdict: String(body.verdict || "").trim().toLowerCase(),
      reason: String(body.reason || "").trim(),
      created_at: nowIso(),
    };
    if (!item.file_id || !item.reporter_instance_id || !["safe", "unsafe"].includes(item.verdict)) {
      sendJson(res, 400, { error: "file_id, reporter_instance_id, verdict(safe|unsafe) are required" });
      return;
    }
    trustAttestations.push(item);
    keepMax(trustAttestations, 20_000);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", file_id: item.file_id, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, item, ...fileTrustEvidence(item.file_id) });
    return;
  }

  const trustFileMatch = url.pathname.match(/^\/v1\/trust\/file\/([^/]+)$/);
  if (trustFileMatch && req.method === "GET") {
    const fileId = decodeURIComponent(trustFileMatch[1]);
    const items = trustAttestations.filter((x) => x.file_id === fileId).slice(-200);
    sendJson(res, 200, { file_id: fileId, ...fileTrustEvidence(fileId), attestations: items });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/ledger/events") {
    const body = await readJson(req);
    const item = makeLedgerEvent(body);
    if (!item.instance_id || !item.type || !Number.isFinite(item.value)) {
      sendJson(res, 400, { error: "instance_id, type, value are required" });
      return;
    }
    const inserted = upsertLedgerEventIfNew(item);
    if (inserted) {
      processLedgerWorkerBatch(1);
    }
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: inserted ? "ok" : "duplicate", instance_id: item.instance_id, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, duplicate: !inserted, item });
    return;
  }

  if (req.method === "POST" && url.pathname === "/v1/ledger/events/batch") {
    const body = await readJson(req);
    const rawItems = Array.isArray(body.items) ? body.items : [];
    if (rawItems.length === 0) {
      sendJson(res, 400, { error: "items is required" });
      return;
    }
    let inserted = 0;
    let duplicates = 0;
    let invalid = 0;
    for (const raw of rawItems) {
      const item = makeLedgerEvent(raw || {});
      if (!item.instance_id || !item.type || !Number.isFinite(item.value)) {
        invalid += 1;
        continue;
      }
      if (upsertLedgerEventIfNew(item)) {
        inserted += 1;
      } else {
        duplicates += 1;
      }
    }
    processLedgerWorkerBatch(1000);
    scheduleSave();
    addAudit({ method: req.method, path: url.pathname, result: "ok", inserted, duplicates, invalid, ip: clientIp(req) });
    sendJson(res, 200, { ok: true, inserted, duplicates, invalid });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/ledger/summary") {
    const instanceId = String(url.searchParams.get("instance_id") || "").trim();
    if (!instanceId) {
      sendJson(res, 400, { error: "instance_id is required" });
      return;
    }
    processLedgerWorkerBatch(1000);
    const rollup = ledgerRollups.get(instanceId);
    if (!rollup) {
      sendJson(res, 200, {
        instance_id: instanceId,
        total_score: 0,
        event_count: 0,
        by_type: {},
      });
      return;
    }
    sendJson(res, 200, {
      instance_id: instanceId,
      total_score: Number(rollup.score || 0),
      event_count: Number(rollup.event_count || 0),
      by_type: rollup.by_type || {},
      updated_at: rollup.updated_at,
      decay_alpha: ledgerDecayAlpha,
    });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/admin/ledger/rollups") {
    processLedgerWorkerBatch(1000);
    const limit = normalizeLimit(url.searchParams.get("limit"), 200, 5000);
    const items = [...ledgerRollups.values()]
      .sort((a, b) => Number(b.score || 0) - Number(a.score || 0))
      .slice(0, limit);
    sendJson(res, 200, { items, count: items.length, decay_alpha: ledgerDecayAlpha });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/admin/audit") {
    const limit = normalizeLimit(url.searchParams.get("limit"), 200, 2000);
    const pathFilter = String(url.searchParams.get("path") || "").trim();
    const resultFilter = String(url.searchParams.get("result") || "").trim().toLowerCase();
    const sinceMinutes = Number(url.searchParams.get("since_minutes") || 0);
    const cutoff = Number.isFinite(sinceMinutes) && sinceMinutes > 0
      ? Date.now() - Math.floor(sinceMinutes * 60 * 1000)
      : 0;

    let items = [...auditLogs];
    if (pathFilter) {
      items = items.filter((x) => String(x.path || "") === pathFilter);
    }
    if (resultFilter) {
      items = items.filter((x) => String(x.result || "").toLowerCase() === resultFilter);
    }
    if (cutoff > 0) {
      items = items.filter((x) => Date.parse(String(x.at || "")) >= cutoff);
    }
    items = items.slice(-limit);
    sendJson(res, 200, { items, count: items.length });
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/admin/runtime/backends") {
    sendJson(res, 200, runtimeBackendsStatus());
    return;
  }

  if (req.method === "GET" && url.pathname === "/v1/admin/runtime/readiness") {
    sendJson(res, 200, runtimeReadinessStatus());
    return;
  }

  addAudit({ method: req.method, path: url.pathname, result: "not_found", ip: clientIp(req) });
  sendJson(res, 404, { error: "not found" });
});

server.listen(port, host, () => {
  console.log(`[control-plane] listening on ${host}:${port}`);
  console.log(`[control-plane] auth token source: ${authToken === "dev-change-me" ? "default(dev-change-me)" : "env"}`);
  console.log(`[control-plane] data file: ${dataFile}`);
  if (requestedStorageBackend !== effectiveStorageBackend) {
    console.log(`[control-plane] requested storage backend '${requestedStorageBackend}' is not supported; using '${effectiveStorageBackend}'`);
  }
  if (requestedQueueBackend !== effectiveQueueBackend) {
    console.log(`[control-plane] requested queue backend '${requestedQueueBackend}' is not supported; using '${effectiveQueueBackend}'`);
  }
});
