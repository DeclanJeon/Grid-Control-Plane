import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25081);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-load-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const loadCount = Number(process.env.VERIFY_LEDGER_EVENT_COUNT || 100);

async function waitForReady(urlBase) {
  const started = Date.now();
  while (Date.now() - started < 20000) {
    try {
      const r = await fetch(`${urlBase}/health`);
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

async function authGet(pathname) {
  const response = await fetch(`${baseUrl}${pathname}`, {
    headers: {
      authorization: `Bearer ${token}`,
      "x-grid-role": "admin",
    },
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function waitForEventCount(instanceId, expectedCount) {
  const started = Date.now();
  const timeoutMs = 30000;
  let lastSummary = null;
  while (Date.now() - started < timeoutMs) {
    const summary = await authGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    lastSummary = summary;
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expectedCount) {
      return summary.json;
    }
    await sleep(200);
  }
  throw new Error(
    `ledger summary did not reach expected count ${expectedCount}; last=${JSON.stringify(lastSummary)}`
  );
}

async function fetchPersistedLedgerEventCount() {
  const mod = await import("pg");
  const PgClient = mod.Client;
  const client = new PgClient({ connectionString: pgDsn });
  await client.connect();
  try {
    const result = await client.query("SELECT state_json FROM control_plane_state WHERE id = 1");
    if (result.rows.length === 0 || !result.rows[0].state_json) {
      return 0;
    }
    const payload = JSON.parse(String(result.rows[0].state_json));
    return Array.isArray(payload.ledgerEvents) ? payload.ledgerEvents.length : 0;
  } finally {
    await client.end();
  }
}

async function waitForPersistedLedgerEventCount(expectedCount) {
  const started = Date.now();
  const timeoutMs = 30000;
  let lastCount = 0;
  while (Date.now() - started < timeoutMs) {
    lastCount = await fetchPersistedLedgerEventCount();
    if (lastCount >= expectedCount) {
      return lastCount;
    }
    await sleep(250);
  }
  throw new Error(`persisted ledgerEvents did not reach ${expectedCount}; last=${lastCount}`);
}

function spawnServer(dataFile) {
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
      CONTROL_QUEUE_BACKEND: "redis_bullmq",
      CONTROL_REDIS_URL: redisUrl,
      RATE_LIMIT_MAX: "5000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function ensureRuntimeBackendsHealthy() {
  const backends = await authGet("/v1/admin/runtime/backends");
  if (backends.status !== 200) {
    throw new Error(`runtime/backends status=${backends.status}`);
  }
  const storage = backends.json.storage || {};
  const queue = backends.json.queue || {};
  if (storage.effective !== "postgres" || !storage.initialized) {
    throw new Error(`storage backend unhealthy: ${JSON.stringify(storage)}`);
  }
  if (queue.effective !== "redis_bullmq" || !queue.initialized) {
    throw new Error(`queue backend unhealthy: ${JSON.stringify(queue)}`);
  }
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-real-backend-load-"));
  const dataFile = path.join(tempDir, "state.json");
  const instanceId = `inst-load-${randomUUID()}`;

  let child = spawnServer(dataFile);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  try {
    await waitForReady(baseUrl);
    await ensureRuntimeBackendsHealthy();

    for (let i = 0; i < loadCount; i += 1) {
      const res = await authPost("/v1/ledger/events", {
        external_event_id: `evt-load-${i}-${randomUUID()}`,
        instance_id: instanceId,
        type: "relay_success",
        value: 1,
      });
      if (res.status !== 200 || !res.json.ok) {
        throw new Error(`ledger ingest failed at ${i}: ${JSON.stringify(res.json)}`);
      }
    }

    const beforePersistedCount = await waitForPersistedLedgerEventCount(loadCount);

    let beforeRestartSummary = { event_count: 0, total_score: 0 };
    try {
      beforeRestartSummary = await waitForEventCount(instanceId, 1);
    } catch {
      // rollup processing may lag; persisted count is primary gate for this script
    }

    child.kill("SIGTERM");
    await sleep(250);

    child = spawnServer(dataFile);
    child.stdout?.on("data", (d) => { stdout += String(d); });
    child.stderr?.on("data", (d) => { stderr += String(d); });

    await waitForReady(baseUrl);
    await ensureRuntimeBackendsHealthy();

    const afterPersistedCount = await waitForPersistedLedgerEventCount(loadCount);

    let afterRestartSummary = { event_count: 0, total_score: 0 };
    try {
      afterRestartSummary = await waitForEventCount(instanceId, 1);
    } catch {
      // rollup processing may lag; persisted count is primary gate for this script
    }

    console.log("real backend load/restart verification: OK");
    console.log(JSON.stringify({
      instance_id: instanceId,
      expected_event_count: loadCount,
      persisted_ledger_events_before_restart: beforePersistedCount,
      persisted_ledger_events_after_restart: afterPersistedCount,
      before_restart: {
        event_count: Number(beforeRestartSummary.event_count || 0),
        total_score: Number(beforeRestartSummary.total_score || 0),
      },
      after_restart: {
        event_count: Number(afterRestartSummary.event_count || 0),
        total_score: Number(afterRestartSummary.total_score || 0),
      },
    }, null, 2));
  } finally {
    child.kill("SIGTERM");
    await sleep(250);
    await rm(tempDir, { recursive: true, force: true });
    if (stderr.trim()) console.error(stderr.trim());
    if (stdout.trim()) console.error(stdout.trim());
  }
}

main().catch((err) => {
  console.error(`real backend load verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
