import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25083);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-wan-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";

const totalEvents = Number(process.env.VERIFY_WAN_TOTAL_EVENTS || 1000);
const workers = Number(process.env.VERIFY_WAN_WORKERS || 8);
const jitterMaxMs = Number(process.env.VERIFY_WAN_CLIENT_JITTER_MS || 25);

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
  while (Date.now() - started < 40000) {
    const res = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (res.status === 200 && Number(res.json.event_count || 0) >= expected) {
      return res.json;
    }
    await sleep(200);
  }
  throw new Error(`summary_event_count did not reach ${expected}`);
}

async function workerIngest(instanceId, indices, duplicateEvery) {
  let unexpectedDuplicateInsert = 0;
  for (const idx of indices) {
    const externalId = `evt-wan-${idx}-${randomUUID()}`;
    const event = {
      external_event_id: externalId,
      instance_id: instanceId,
      type: "relay_success",
      value: 1,
    };
    const res = await authPost("/v1/ledger/events", event);
    if (res.status !== 200 || !res.json.ok) {
      throw new Error(`ingest failed idx=${idx} status=${res.status}`);
    }

    if (idx % duplicateEvery === 0) {
      const dup = await authPost("/v1/ledger/events", event);
      if (dup.status !== 200 || !dup.json.ok) {
        throw new Error(`duplicate ingest failed idx=${idx}`);
      }
      if (!dup.json.duplicate) {
        unexpectedDuplicateInsert += 1;
      }
    }

    if (jitterMaxMs > 0) {
      const ms = Math.floor(Math.random() * (jitterMaxMs + 1));
      if (ms > 0) await sleep(ms);
    }
  }
  return unexpectedDuplicateInsert;
}

function splitIndices(total, parts) {
  const out = Array.from({ length: parts }, () => []);
  for (let i = 0; i < total; i += 1) {
    out[i % parts].push(i);
  }
  return out;
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
      RATE_LIMIT_MAX: "10000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-real-backend-wan-"));
  const dataFile = path.join(tempDir, "state.json");
  const instanceId = `inst-wan-${randomUUID()}`;
  const duplicateEvery = 10;

  const child = spawnServer(dataFile);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  try {
    await waitForReady();

    const chunks = splitIndices(totalEvents, workers);
    const dupCounts = await Promise.all(chunks.map((indices) => workerIngest(instanceId, indices, duplicateEvery)));
    const unexpectedDuplicateInsert = dupCounts.reduce((a, b) => a + b, 0);
    if (unexpectedDuplicateInsert !== 0) {
      throw new Error(`unexpected duplicate inserts: ${unexpectedDuplicateInsert}`);
    }

    const summary = await waitForSummaryCount(instanceId, totalEvents);
    const backends = await authAdminGet("/v1/admin/runtime/backends");
    if (backends.status !== 200) {
      throw new Error(`runtime/backends status=${backends.status}`);
    }
    if (backends.json.storage.effective !== "postgres" || !backends.json.storage.initialized) {
      throw new Error(`storage not healthy under wan-like run: ${JSON.stringify(backends.json.storage)}`);
    }
    if (backends.json.queue.effective !== "redis_bullmq" || !backends.json.queue.initialized) {
      throw new Error(`queue not healthy under wan-like run: ${JSON.stringify(backends.json.queue)}`);
    }

    console.log("real backend wan-like verification: OK");
    console.log(JSON.stringify({
      expected_total: totalEvents,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      workers,
      client_jitter_max_ms: jitterMaxMs,
      duplicate_every: duplicateEvery,
      unexpected_duplicate_insert_count: unexpectedDuplicateInsert,
      runtime_backends: backends.json,
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
  console.error(`wan-like verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
