import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25088);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-chaos-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const queueName = process.env.CONTROL_BULLMQ_QUEUE || `grid-ledger-events-chaos-${Math.floor(Date.now() / 1000)}`;
const redisContainer = process.env.VERIFY_REDIS_CONTAINER || "grid-control-redis-e2e";

const phaseA = Number(process.env.VERIFY_CHAOS_PHASE_A || 2000);
const phaseB = Number(process.env.VERIFY_CHAOS_PHASE_B || 2000);
const phaseC = Number(process.env.VERIFY_CHAOS_PHASE_C || 2000);
const total = phaseA + phaseB + phaseC;

async function runDocker(args) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => { out += String(d); });
    child.stderr.on("data", (d) => { err += String(d); });
    child.on("exit", (code) => {
      if (code === 0) resolve({ out, err });
      else reject(new Error(`docker ${args.join(" ")} failed (${code}): ${err || out}`));
    });
  });
}

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

async function ingestRange(instanceId, start, endExclusive, duplicateEvery = 40) {
  let unexpectedDuplicateInsert = 0;
  for (let i = start; i < endExclusive; i += 1) {
    const externalId = `evt-chaos-${i}-${randomUUID()}`;
    const payload = {
      external_event_id: externalId,
      instance_id: instanceId,
      type: "relay_success",
      value: 1,
    };
    const res = await authPost("/v1/ledger/events", payload);
    if (res.status !== 200 || !res.json.ok) {
      throw new Error(`ingest failed idx=${i}: ${JSON.stringify(res.json)}`);
    }
    if (i % duplicateEvery === 0) {
      const dup = await authPost("/v1/ledger/events", payload);
      if (dup.status !== 200 || !dup.json.ok) {
        throw new Error(`duplicate ingest failed idx=${i}: ${JSON.stringify(dup.json)}`);
      }
      if (!dup.json.duplicate) {
        unexpectedDuplicateInsert += 1;
      }
    }
  }
  return unexpectedDuplicateInsert;
}

async function waitForSummaryCount(instanceId, expected) {
  const started = Date.now();
  while (Date.now() - started < 120000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expected) {
      return summary.json;
    }
    await sleep(250);
  }
  throw new Error(`summary_event_count did not reach ${expected}`);
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
  while (Date.now() - started < 120000) {
    const counts = await fetchQueueCounts();
    const active = Number(counts.active || 0);
    const waiting = Number(counts.waiting || 0);
    const delayed = Number(counts.delayed || 0);
    const failed = Number(counts.failed || 0);
    if (failed === 0 && active === 0 && waiting === 0 && delayed === 0) {
      return counts;
    }
    await sleep(400);
  }
  throw new Error("queue did not drain in chaos-combo scenario");
}

async function waitForBackendRestore() {
  const started = Date.now();
  while (Date.now() - started < 60000) {
    const b = await authAdminGet("/v1/admin/runtime/backends");
    if (b.status === 200) {
      const storageOk = b.json.storage?.effective === "postgres" && b.json.storage?.initialized === true;
      const queueOk = b.json.queue?.effective === "redis_bullmq" && b.json.queue?.initialized === true;
      if (storageOk && queueOk) {
        return b.json;
      }
    }
    await sleep(500);
  }
  throw new Error("backend did not restore in time");
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
      CONTROL_BULLMQ_WORKER_CONCURRENCY: "24",
      RATE_LIMIT_MAX: "20000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-chaos-combo-"));
  const dataFile = path.join(tempDir, "state.json");
  const tableName = `control_plane_state_chaos_${Math.floor(Date.now() / 1000)}`;
  const instanceId = `inst-chaos-${randomUUID()}`;

  let child = spawnServer(dataFile, tableName);
  let stdout = "";
  let stderr = "";
  const maxLogChars = 200000;
  const appendLimited = (current, chunk) => {
    const next = current + chunk;
    if (next.length <= maxLogChars) return next;
    return next.slice(next.length - maxLogChars);
  };
  const attachLogs = (proc) => {
    proc.stdout?.on("data", (d) => { stdout = appendLimited(stdout, String(d)); });
    proc.stderr?.on("data", (d) => { stderr = appendLimited(stderr, String(d)); });
  };
  attachLogs(child);

  const startedAt = Date.now();
  try {
    await waitForReady();

    const dupA = await ingestRange(instanceId, 0, phaseA);

    await runDocker(["stop", redisContainer]);
    const dupB = await ingestRange(instanceId, phaseA, phaseA + phaseB);
    await runDocker(["start", redisContainer]);
    await sleep(1200);

    child.kill("SIGKILL");
    await sleep(500);
    child = spawnServer(dataFile, tableName);
    attachLogs(child);
    await waitForReady();

    const dupC = await ingestRange(instanceId, phaseA + phaseB, total);

    const unexpectedDuplicateInsert = dupA + dupB + dupC;
    if (unexpectedDuplicateInsert !== 0) {
      throw new Error(`unexpected duplicate inserts: ${unexpectedDuplicateInsert}`);
    }

    const backends = await waitForBackendRestore();
    const summary = await waitForSummaryCount(instanceId, total);
    const queueCounts = await waitForQueueDrain();
    const elapsedMs = Date.now() - startedAt;

    console.log("real backend chaos-combo verification: OK");
    console.log(JSON.stringify({
      expected_total: total,
      phase_a: phaseA,
      phase_b: phaseB,
      phase_c: phaseC,
      elapsed_ms: elapsedMs,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      unexpected_duplicate_insert_count: unexpectedDuplicateInsert,
      queue_counts: queueCounts,
      runtime_backends: backends,
      injections: {
        redis_hiccup: true,
        process_kill_sigkill: true,
      },
    }, null, 2));
  } finally {
    try { await runDocker(["start", redisContainer]); } catch {}
    child.kill("SIGTERM");
    await sleep(250);
    await rm(tempDir, { recursive: true, force: true });
    if (stderr.trim()) console.error(stderr.trim());
    if (stdout.trim()) console.error(stdout.trim());
  }
}

main().catch((err) => {
  console.error(`chaos-combo verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
