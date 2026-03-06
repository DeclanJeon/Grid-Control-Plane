import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25087);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-restart-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const queueName = process.env.CONTROL_BULLMQ_QUEUE || `grid-ledger-events-restart-${Math.floor(Date.now() / 1000)}`;

const phaseA = Number(process.env.VERIFY_RESTART_PHASE_A || 120);
const phaseB = Number(process.env.VERIFY_RESTART_PHASE_B || 280);
const total = phaseA + phaseB;

async function waitForReady() {
  const started = Date.now();
  while (Date.now() - started < 30000) {
    try {
      const r = await fetch(`${baseUrl}/health`);
      if (r.ok) return;
    } catch {}
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

async function ingestRange(instanceId, start, endExclusive) {
  for (let i = start; i < endExclusive; i += 1) {
    const externalId = `evt-rm-${i}-${randomUUID()}`;
    const payload = {
      external_event_id: externalId,
      instance_id: instanceId,
      type: "relay_success",
      value: 1,
    };
    const res = await authPost("/v1/ledger/events", payload);
    if (res.status !== 200 || !res.json.ok) {
      throw new Error(`ingest failed at ${i}: ${JSON.stringify(res.json)}`);
    }
    if (i % 15 === 0) {
      const dup = await authPost("/v1/ledger/events", payload);
      if (dup.status !== 200 || !dup.json.ok || !dup.json.duplicate) {
        throw new Error(`duplicate check failed at ${i}: ${JSON.stringify(dup.json)}`);
      }
    }
  }
}

async function waitForSummaryCount(instanceId, expected) {
  const started = Date.now();
  let last = null;
  while (Date.now() - started < 180000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    last = summary;
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expected) {
      return summary.json;
    }
    await sleep(250);
  }
  throw new Error(`summary_event_count did not reach ${expected}; last=${JSON.stringify(last)}`);
}

async function fetchPersistedLedgerEventCount(tableName) {
  const mod = await import("pg");
  const PgClient = mod.Client;
  const client = new PgClient({ connectionString: pgDsn });
  await client.connect();
  try {
    const result = await client.query(`SELECT state_json FROM ${tableName} WHERE id = 1`);
    if (result.rows.length === 0 || !result.rows[0].state_json) {
      return 0;
    }
    const payload = JSON.parse(String(result.rows[0].state_json));
    return Array.isArray(payload.ledgerEvents) ? payload.ledgerEvents.length : 0;
  } finally {
    await client.end();
  }
}

async function waitForPersistedCount(tableName, expected) {
  const started = Date.now();
  while (Date.now() - started < 120000) {
    const n = await fetchPersistedLedgerEventCount(tableName);
    if (n >= expected) {
      return n;
    }
    await sleep(300);
  }
  throw new Error(`persisted ledgerEvents did not reach ${expected}`);
}

async function fetchQueueCounts() {
  const mod = await import("bullmq");
  const Queue = mod.Queue;
  const q = new Queue(queueName, { connection: { url: redisUrl } });
  try {
    return await q.getJobCounts("active", "waiting", "completed", "failed", "delayed", "paused", "waiting-children");
  } finally {
    await q.close();
  }
}

async function waitForQueueDrain() {
  const started = Date.now();
  let last = null;
  while (Date.now() - started < 180000) {
    const counts = await fetchQueueCounts();
    last = counts;
    const active = Number(counts.active || 0);
    const waiting = Number(counts.waiting || 0);
    const failed = Number(counts.failed || 0);
    const delayed = Number(counts.delayed || 0);
    if (failed === 0 && active === 0 && waiting === 0 && delayed === 0) {
      return counts;
    }
    await sleep(300);
  }
  throw new Error(`queue did not drain after restart-midrun scenario; last=${JSON.stringify(last)}`);
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
      CONTROL_BULLMQ_WORKER_CONCURRENCY: "8",
      RATE_LIMIT_MAX: "20000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-restart-midrun-"));
  const dataFile = path.join(tempDir, "state.json");
  const tableName = `control_plane_state_rm_${Math.floor(Date.now() / 1000)}`;
  const instanceId = `inst-rm-${randomUUID()}`;

  let child = spawnServer(dataFile, tableName);
  let stdout = "";
  let stderr = "";
  const attachLogs = (proc) => {
    proc.stdout?.on("data", (d) => { stdout += String(d); });
    proc.stderr?.on("data", (d) => { stderr += String(d); });
  };
  attachLogs(child);

  try {
    await waitForReady();
    await ingestRange(instanceId, 0, phaseA);
    await waitForSummaryCount(instanceId, phaseA);
    await waitForPersistedCount(tableName, phaseA);

    child.kill("SIGTERM");
    await sleep(400);

    child = spawnServer(dataFile, tableName);
    attachLogs(child);
    await waitForReady();

    await ingestRange(instanceId, phaseA, total);

    const queueCounts = await waitForQueueDrain();
    const summary = await waitForSummaryCount(instanceId, total);

    console.log("real backend restart-midrun verification: OK");
    console.log(JSON.stringify({
      expected_total: total,
      phase_a: phaseA,
      phase_b: phaseB,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      queue_counts: queueCounts,
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
  console.error(`restart-midrun verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
