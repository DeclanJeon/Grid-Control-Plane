import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25082);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-fault-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const pgTable = process.env.CONTROL_PG_TABLE || `control_plane_state_fault_${Math.floor(Date.now() / 1000)}`;
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const redisContainer = process.env.VERIFY_REDIS_CONTAINER || "grid-control-redis-e2e";
const postgresContainer = process.env.VERIFY_POSTGRES_CONTAINER || "grid-control-postgres-e2e";

const batchA = Number(process.env.VERIFY_FAULT_BATCH_A || 30);
const batchB = Number(process.env.VERIFY_FAULT_BATCH_B || 30);
const batchC = Number(process.env.VERIFY_FAULT_BATCH_C || 40);
const duplicatePerBatch = Number(process.env.VERIFY_FAULT_DUPLICATE_PER_BATCH || 5);
const expectedTotal = batchA + batchB + batchC;

async function runDocker(args) {
  return new Promise((resolve, reject) => {
    const child = spawn("docker", args, { stdio: ["ignore", "pipe", "pipe"] });
    let out = "";
    let err = "";
    child.stdout.on("data", (d) => { out += String(d); });
    child.stderr.on("data", (d) => { err += String(d); });
    child.on("exit", (code) => {
      if (code === 0) {
        resolve({ out, err });
      } else {
        reject(new Error(`docker ${args.join(" ")} failed (${code}): ${err || out}`));
      }
    });
  });
}

async function waitForReady(urlBase) {
  const started = Date.now();
  while (Date.now() - started < 25000) {
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

async function ingestBatch(instanceId, count, label) {
  let duplicatesAccepted = 0;
  for (let i = 0; i < count; i += 1) {
    const externalId = `evt-${label}-${i}-${randomUUID()}`;
    const res = await authPost("/v1/ledger/events", {
      external_event_id: externalId,
      instance_id: instanceId,
      type: "relay_success",
      value: 1,
    });
    if (res.status !== 200 || !res.json.ok) {
      throw new Error(`ingest failed at ${label}/${i}: ${JSON.stringify(res.json)}`);
    }

    if (i < duplicatePerBatch) {
      const dupRes = await authPost("/v1/ledger/events", {
        external_event_id: externalId,
        instance_id: instanceId,
        type: "relay_success",
        value: 1,
      });
      if (dupRes.status !== 200 || !dupRes.json.ok) {
        throw new Error(`duplicate ingest failed at ${label}/${i}: ${JSON.stringify(dupRes.json)}`);
      }
      if (!dupRes.json.duplicate) {
        duplicatesAccepted += 1;
      }
    }
  }
  return { duplicatesAccepted };
}

async function fetchPersistedLedgerEventCount() {
  const mod = await import("pg");
  const PgClient = mod.Client;
  const client = new PgClient({ connectionString: pgDsn });
  await client.connect();
  try {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(pgTable)) {
      throw new Error(`invalid pg table name: ${pgTable}`);
    }
    const result = await client.query(`SELECT state_json FROM ${pgTable} WHERE id = 1`);
    if (result.rows.length === 0 || !result.rows[0].state_json) {
      return 0;
    }
    const payload = JSON.parse(String(result.rows[0].state_json));
    return Array.isArray(payload.ledgerEvents) ? payload.ledgerEvents.length : 0;
  } finally {
    await client.end();
  }
}

async function waitForPersistedLedgerCount(target) {
  const started = Date.now();
  while (Date.now() - started < 30000) {
    const n = await fetchPersistedLedgerEventCount();
    if (n >= target) {
      return n;
    }
    await sleep(250);
  }
  throw new Error(`persisted ledger count did not reach ${target}`);
}

async function waitForSummaryCount(instanceId, target) {
  const started = Date.now();
  while (Date.now() - started < 30000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= target) {
      return summary.json;
    }
    await sleep(250);
  }
  throw new Error(`summary count did not reach ${target}`);
}

async function fetchRuntimeBackends() {
  const response = await authAdminGet("/v1/admin/runtime/backends");
  if (response.status !== 200) {
    throw new Error(`runtime/backends status=${response.status}`);
  }
  return response.json;
}

async function waitForBackendRestore() {
  const started = Date.now();
  const timeoutMs = 40000;
  let last = null;
  while (Date.now() - started < timeoutMs) {
    last = await fetchRuntimeBackends();
    const storageOk =
      last.storage?.effective === "postgres" &&
      last.storage?.initialized === true;
    const queueOk =
      last.queue?.effective === "redis_bullmq" &&
      last.queue?.initialized === true;
    if (storageOk && queueOk) {
      return last;
    }
    await sleep(500);
  }
  throw new Error(`backend restore timeout; last=${JSON.stringify(last)}`);
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
      CONTROL_PG_TABLE: pgTable,
      CONTROL_QUEUE_BACKEND: "redis_bullmq",
      CONTROL_REDIS_URL: redisUrl,
      RATE_LIMIT_MAX: "5000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-real-backend-faults-"));
  const dataFile = path.join(tempDir, "state.json");
  const instanceId = `inst-fault-${randomUUID()}`;

  let child = spawnServer(dataFile);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  try {
    await waitForReady(baseUrl);

    const dupA = await ingestBatch(instanceId, batchA, "a");

    await runDocker(["stop", redisContainer]);
    const dupB = await ingestBatch(instanceId, batchB, "redis-down");
    await runDocker(["start", redisContainer]);
    await sleep(1000);

    await runDocker(["restart", postgresContainer]);
    await sleep(1000);
    const dupC = await ingestBatch(instanceId, batchC, "pg-restart");

    const duplicateUnexpectedInserts = dupA.duplicatesAccepted + dupB.duplicatesAccepted + dupC.duplicatesAccepted;
    if (duplicateUnexpectedInserts !== 0) {
      throw new Error(`duplicate events were unexpectedly inserted: ${duplicateUnexpectedInserts}`);
    }

    const persistedCount = await waitForPersistedLedgerCount(batchA + batchB);
    const summary = await waitForSummaryCount(instanceId, expectedTotal);
    const backends = await waitForBackendRestore();
    const persistedAfter = await waitForPersistedLedgerCount(expectedTotal);

    console.log("real backend fault-injection verification: OK");
    console.log(JSON.stringify({
      instance_id: instanceId,
      expected_total: expectedTotal,
      persistence_mode_after_injection: `${backends.storage.effective}:${backends.storage.effective_reason}`,
      persisted_ledger_events_before_pg_restart: persistedCount,
      persisted_ledger_events_after_injection: persistedAfter,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      runtime_backends: backends,
      duplicate_injection: {
        duplicate_per_batch: duplicatePerBatch,
        unexpected_insert_count: duplicateUnexpectedInserts,
      },
      injections: {
        redis_stop_start: true,
        postgres_restart: true,
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
  console.error(`fault-injection verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
