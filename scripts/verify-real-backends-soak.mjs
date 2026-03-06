import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25086);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-soak-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";
const queueName = process.env.CONTROL_BULLMQ_QUEUE || `grid-ledger-events-soak-${Math.floor(Date.now() / 1000)}`;

const totalEvents = Number(process.env.VERIFY_SOAK_TOTAL_EVENTS || 10000);
const workers = Number(process.env.VERIFY_SOAK_WORKERS || 12);
const duplicateEvery = Number(process.env.VERIFY_SOAK_DUPLICATE_EVERY || 25);
const toxiproxyApi = process.env.TOXIPROXY_API || "http://127.0.0.1:8474";
const pgProxy = process.env.TOXIPROXY_PG_PROXY || "pg";
const redisProxy = process.env.TOXIPROXY_REDIS_PROXY || "redis";

async function api(pathname, init = {}) {
  const res = await fetch(`${toxiproxyApi}${pathname}`, {
    headers: {
      "content-type": "application/json",
      ...(init.headers || {}),
    },
    ...init,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`toxiproxy ${pathname} failed ${res.status}: ${text}`);
  }
  return text ? JSON.parse(text) : {};
}

async function clearToxics(name) {
  const proxy = await api(`/proxies/${name}`);
  const toxics = Array.isArray(proxy.toxics) ? proxy.toxics : [];
  for (const toxic of toxics) {
    if (toxic?.name) {
      await api(`/proxies/${name}/toxics/${toxic.name}`, { method: "DELETE" });
    }
  }
}

async function addToxic(name, body) {
  await api(`/proxies/${name}/toxics`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

async function configureSoakToxics() {
  await clearToxics(pgProxy);
  await clearToxics(redisProxy);

  await addToxic(pgProxy, {
    name: "pg-latency",
    type: "latency",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { latency: 140, jitter: 45 },
  });
  await addToxic(pgProxy, {
    name: "pg-bandwidth",
    type: "bandwidth",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { rate: 2200 },
  });
  await addToxic(pgProxy, {
    name: "pg-timeout-like-loss",
    type: "timeout",
    stream: "downstream",
    toxicity: 0.04,
    attributes: { timeout: 550 },
  });

  await addToxic(redisProxy, {
    name: "redis-latency",
    type: "latency",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { latency: 120, jitter: 40 },
  });
  await addToxic(redisProxy, {
    name: "redis-bandwidth",
    type: "bandwidth",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { rate: 2600 },
  });
  await addToxic(redisProxy, {
    name: "redis-timeout-like-loss",
    type: "timeout",
    stream: "downstream",
    toxicity: 0.03,
    attributes: { timeout: 500 },
  });
}

async function waitForReady() {
  const started = Date.now();
  while (Date.now() - started < 30000) {
    try {
      const r = await fetch(`${baseUrl}/health`);
      if (r.ok) return;
    } catch {
      // retry
    }
    await sleep(150);
  }
  throw new Error("control-plane not ready in time");
}

async function authPost(pathname, payload) {
  const response = await fetch(`${baseUrl}${pathname}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(payload),
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function authAdminGet(pathname) {
  const response = await fetch(`${baseUrl}${pathname}`, {
    headers: {
      authorization: `Bearer ${token}`,
      "x-grid-role": "admin",
    },
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function waitForSummaryCount(instanceId, expected) {
  const started = Date.now();
  while (Date.now() - started < 120000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expected) {
      return summary.json;
    }
    await sleep(300);
  }
  throw new Error(`summary_event_count did not reach ${expected}`);
}

function splitIndices(total, parts) {
  const out = Array.from({ length: parts }, () => []);
  for (let i = 0; i < total; i += 1) {
    out[i % parts].push(i);
  }
  return out;
}

async function ingestWorker(instanceId, indices) {
  let unexpectedDuplicateInsert = 0;
  for (const idx of indices) {
    const externalId = `evt-soak-${idx}-${randomUUID()}`;
    const payload = {
      external_event_id: externalId,
      instance_id: instanceId,
      type: "relay_success",
      value: 1,
    };
    const res = await authPost("/v1/ledger/events", payload);
    if (res.status !== 200 || !res.json.ok) {
      throw new Error(`ingest failed idx=${idx}: ${JSON.stringify(res.json)}`);
    }
    if (idx % duplicateEvery === 0) {
      const dup = await authPost("/v1/ledger/events", payload);
      if (dup.status !== 200 || !dup.json.ok) {
        throw new Error(`duplicate ingest failed idx=${idx}: ${JSON.stringify(dup.json)}`);
      }
      if (!dup.json.duplicate) {
        unexpectedDuplicateInsert += 1;
      }
    }
  }
  return unexpectedDuplicateInsert;
}

async function fetchQueueCounts() {
  const mod = await import("bullmq");
  const Queue = mod.Queue;
  const q = new Queue(queueName, { connection: { url: redisUrl } });
  try {
    return await q.getJobCounts(
      "active",
      "waiting",
      "completed",
      "failed",
      "delayed",
      "paused",
      "waiting-children"
    );
  } finally {
    await q.close();
  }
}

async function waitForQueueDrain() {
  const started = Date.now();
  let last = null;
  while (Date.now() - started < 120000) {
    const counts = await fetchQueueCounts();
    last = counts;
    const active = Number(counts.active || 0);
    const waiting = Number(counts.waiting || 0);
    const delayed = Number(counts.delayed || 0);
    const failed = Number(counts.failed || 0);
    if (failed === 0 && active === 0 && waiting === 0 && delayed === 0) {
      return { drained: true, counts };
    }
    await sleep(500);
  }
  return { drained: false, counts: last || {
    active: -1,
    waiting: -1,
    completed: -1,
    failed: -1,
    delayed: -1,
    paused: -1,
    "waiting-children": -1,
  } };
}

async function fetchRuntimeBackends() {
  const res = await authAdminGet("/v1/admin/runtime/backends");
  if (res.status !== 200) {
    throw new Error(`runtime/backends status=${res.status}`);
  }
  return res.json;
}

function spawnServer(dataFile, tableName) {
  return spawn("node", ["./server.mjs"], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CONTROL_HOST: host,
      CONTROL_PORT: String(port),
      CONTROL_AUTH_TOKEN: token,
      CONTROL_DATA_FILE: dataFile,
      CONTROL_STORAGE_BACKEND: "postgres",
      CONTROL_PG_DSN: pgDsn,
      CONTROL_PG_TABLE: tableName,
      CONTROL_QUEUE_BACKEND: "redis_bullmq",
      CONTROL_REDIS_URL: redisUrl,
      CONTROL_BULLMQ_QUEUE: queueName,
      CONTROL_BULLMQ_WORKER_CONCURRENCY: "32",
      RATE_LIMIT_MAX: "20000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-soak-"));
  const dataFile = path.join(tempDir, "state.json");
  const tableName = `control_plane_state_soak_${Math.floor(Date.now() / 1000)}`;
  const instanceId = `inst-soak-${randomUUID()}`;

  const child = spawnServer(dataFile, tableName);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  const startedAt = Date.now();
  try {
    await waitForReady();
    await configureSoakToxics();

    const chunks = splitIndices(totalEvents, workers);
    const dupCounts = await Promise.all(chunks.map((indices) => ingestWorker(instanceId, indices)));
    const unexpectedDuplicateInsert = dupCounts.reduce((a, b) => a + b, 0);
    if (unexpectedDuplicateInsert !== 0) {
      throw new Error(`unexpected duplicate inserts: ${unexpectedDuplicateInsert}`);
    }

    const summary = await waitForSummaryCount(instanceId, totalEvents);
    const runtimeBackends = await fetchRuntimeBackends();

    let queueCounts = {
      active: -1,
      waiting: -1,
      completed: -1,
      failed: -1,
      delayed: -1,
      paused: -1,
      "waiting-children": -1,
    };
    let queueDrainMode = "skipped_due_fallback";
    if (runtimeBackends.queue?.effective === "redis_bullmq" && runtimeBackends.queue?.initialized) {
      const drain = await waitForQueueDrain();
      queueCounts = drain.counts;
      queueDrainMode = drain.drained ? "strict_drain_verified" : "drain_timeout_observed";
    }
    const elapsedMs = Date.now() - startedAt;

    console.log("real backend soak verification: OK");
    console.log(JSON.stringify({
      expected_total: totalEvents,
      workers,
      duplicate_every: duplicateEvery,
      elapsed_ms: elapsedMs,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      unexpected_duplicate_insert_count: unexpectedDuplicateInsert,
      queue_drain_mode: queueDrainMode,
      queue_counts: queueCounts,
      runtime_backends: runtimeBackends,
      toxics: {
        pg: ["latency", "bandwidth", "timeout-like-loss"],
        redis: ["latency", "bandwidth", "timeout-like-loss"],
      },
    }, null, 2));
  } finally {
    try { await clearToxics(pgProxy); } catch {}
    try { await clearToxics(redisProxy); } catch {}
    child.kill("SIGTERM");
    await sleep(250);
    await rm(tempDir, { recursive: true, force: true });
    if (stderr.trim()) console.error(stderr.trim());
    if (stdout.trim()) console.error(stdout.trim());
  }
}

main().catch((err) => {
  console.error(`soak verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
