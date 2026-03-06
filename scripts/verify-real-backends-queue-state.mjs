import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25085);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-queue-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const queueName = process.env.CONTROL_BULLMQ_QUEUE || `grid-ledger-events-qs-${Math.floor(Date.now() / 1000)}-${randomUUID().slice(0, 8)}`;
const totalEvents = Number(process.env.VERIFY_QUEUE_STATE_TOTAL_EVENTS || 1000);
const workers = Number(process.env.VERIFY_QUEUE_STATE_WORKERS || 8);

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
  while (Date.now() - started < 45000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expected) {
      return summary.json;
    }
    await sleep(250);
  }
  throw new Error(`summary_event_count did not reach ${expected}`);
}

function chunkIndices(total, parts) {
  const out = Array.from({ length: parts }, () => []);
  for (let i = 0; i < total; i += 1) {
    out[i % parts].push(i);
  }
  return out;
}

async function ingestWorker(instanceId, indices) {
  for (const idx of indices) {
    const externalId = `evt-qs-${idx}-${randomUUID()}`;
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
    if (idx % 10 === 0) {
      const dup = await authPost("/v1/ledger/events", payload);
      if (dup.status !== 200 || !dup.json.ok || !dup.json.duplicate) {
        throw new Error(`duplicate handling failed idx=${idx}: ${JSON.stringify(dup.json)}`);
      }
    }
  }
}

async function fetchQueueState() {
  const mod = await import("bullmq");
  const Queue = mod.Queue;
  const queue = new Queue(queueName, { connection: { url: redisUrl } });
  try {
    const counts = await queue.getJobCounts(
      "active",
      "waiting",
      "completed",
      "failed",
      "delayed",
      "paused",
      "waiting-children"
    );
    return counts;
  } finally {
    await queue.close();
  }
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
      RATE_LIMIT_MAX: "10000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-queue-state-"));
  const dataFile = path.join(tempDir, "state.json");
  const tableName = `control_plane_state_qs_${Math.floor(Date.now() / 1000)}`;
  const instanceId = `inst-qs-${randomUUID()}`;

  const child = spawnServer(dataFile, tableName);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  try {
    await waitForReady();

    const chunks = chunkIndices(totalEvents, workers);
    await Promise.all(chunks.map((indices) => ingestWorker(instanceId, indices)));

    const summary = await waitForSummaryCount(instanceId, totalEvents);
    await sleep(1500);
    const queueCounts = await fetchQueueState();
    const failed = Number(queueCounts.failed || 0);
    const active = Number(queueCounts.active || 0);
    const waiting = Number(queueCounts.waiting || 0);
    const delayed = Number(queueCounts.delayed || 0);

    if (failed !== 0) {
      throw new Error(`queue has failed jobs: ${JSON.stringify(queueCounts)}`);
    }
    if (active !== 0 || waiting !== 0 || delayed !== 0) {
      throw new Error(`queue not drained after settle: ${JSON.stringify(queueCounts)}`);
    }

    console.log("real backend queue-state verification: OK");
    console.log(JSON.stringify({
      expected_total: totalEvents,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      workers,
      queue_name: queueName,
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
  console.error(`queue-state verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
