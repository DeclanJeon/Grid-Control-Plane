import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25084);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-wan-hard-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";
const pgTable = process.env.CONTROL_PG_TABLE || `control_plane_state_wanhard_${Math.floor(Date.now() / 1000)}`;

const toxiproxyApi = process.env.TOXIPROXY_API || "http://127.0.0.1:8474";
const pgProxyName = process.env.TOXIPROXY_PG_PROXY || "pg";
const redisProxyName = process.env.TOXIPROXY_REDIS_PROXY || "redis";
const cutMode = String(process.env.VERIFY_WAN_HARD_CUT_MODE || "both").trim().toLowerCase();

const phaseA = Number(process.env.VERIFY_WAN_HARD_BATCH_A || 300);
const phaseB = Number(process.env.VERIFY_WAN_HARD_BATCH_B || 300);
const phaseC = Number(process.env.VERIFY_WAN_HARD_BATCH_C || 400);
const totalEvents = phaseA + phaseB + phaseC;
const workers = Number(process.env.VERIFY_WAN_HARD_WORKERS || 8);

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

async function waitForBackendRestore() {
  const started = Date.now();
  while (Date.now() - started < 50000) {
    const status = await authAdminGet("/v1/admin/runtime/backends");
    if (status.status === 200) {
      const storageOk = status.json.storage?.effective === "postgres" && status.json.storage?.initialized === true;
      const queueOk = status.json.queue?.effective === "redis_bullmq" && status.json.queue?.initialized === true;
      if (storageOk && queueOk) {
        return status.json;
      }
    }
    await sleep(500);
  }
  throw new Error("backend re-promote did not converge in time");
}

async function waitForSummaryCount(instanceId, expected) {
  const started = Date.now();
  while (Date.now() - started < 50000) {
    const summary = await authAdminGet(`/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    if (summary.status === 200 && Number(summary.json.event_count || 0) >= expected) {
      return summary.json;
    }
    await sleep(250);
  }
  throw new Error(`summary_event_count did not reach ${expected}`);
}

async function fetchPersistedLedgerEventCount() {
  const mod = await import("pg");
  const PgClient = mod.Client;
  const client = new PgClient({ connectionString: pgDsn });
  await client.connect();
  try {
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

async function waitForPersistedCount(expected) {
  const started = Date.now();
  while (Date.now() - started < 50000) {
    const count = await fetchPersistedLedgerEventCount();
    if (count >= expected) {
      return count;
    }
    await sleep(250);
  }
  throw new Error(`persisted ledgerEvents count did not reach ${expected}`);
}

async function setProxyEnabled(name, enabled) {
  const response = await fetch(`${toxiproxyApi}/proxies/${name}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ enabled }),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(`setProxyEnabled(${name}, ${enabled}) failed: ${response.status} ${text}`);
  }
}

async function applyCut(enabled) {
  if (cutMode === "both") {
    await setProxyEnabled(pgProxyName, enabled);
    await setProxyEnabled(redisProxyName, enabled);
    return;
  }
  if (cutMode === "pg") {
    await setProxyEnabled(pgProxyName, enabled);
    return;
  }
  if (cutMode === "redis") {
    await setProxyEnabled(redisProxyName, enabled);
    return;
  }
  throw new Error(`unsupported VERIFY_WAN_HARD_CUT_MODE: ${cutMode}`);
}

async function ingestRange(instanceId, start, endExclusive, duplicateEvery = 15) {
  let unexpectedDuplicateInsert = 0;
  const indices = [];
  for (let i = start; i < endExclusive; i += 1) {
    indices.push(i);
  }
  const chunks = Array.from({ length: workers }, () => []);
  for (let i = 0; i < indices.length; i += 1) {
    chunks[i % workers].push(indices[i]);
  }

  const dupCounts = await Promise.all(chunks.map(async (chunk) => {
    let localDup = 0;
    for (const idx of chunk) {
      const externalId = `evt-wanhard-${idx}-${randomUUID()}`;
      const payload = {
        external_event_id: externalId,
        instance_id: instanceId,
        type: "relay_success",
        value: 1,
      };
      const res = await authPost("/v1/ledger/events", payload);
      if (res.status !== 200 || !res.json.ok) {
        throw new Error(`ingest failed at idx=${idx}: ${JSON.stringify(res.json)}`);
      }
      if (idx % duplicateEvery === 0) {
        const dup = await authPost("/v1/ledger/events", payload);
        if (dup.status !== 200 || !dup.json.ok) {
          throw new Error(`duplicate ingest failed at idx=${idx}: ${JSON.stringify(dup.json)}`);
        }
        if (!dup.json.duplicate) {
          localDup += 1;
        }
      }
    }
    return localDup;
  }));

  for (const n of dupCounts) {
    unexpectedDuplicateInsert += n;
  }
  return unexpectedDuplicateInsert;
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
      RATE_LIMIT_MAX: "10000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-wanhard-"));
  const dataFile = path.join(tempDir, "state.json");
  const instanceId = `inst-wanhard-${randomUUID()}`;

  const child = spawnServer(dataFile);
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => { stdout += String(d); });
  child.stderr?.on("data", (d) => { stderr += String(d); });

  try {
    await waitForReady();

    const dupA = await ingestRange(instanceId, 0, phaseA);

    await applyCut(false);
    const dupB = await ingestRange(instanceId, phaseA, phaseA + phaseB);

    await applyCut(true);
    await sleep(2000);

    const dupC = await ingestRange(instanceId, phaseA + phaseB, totalEvents);

    const unexpectedDuplicateInsert = dupA + dupB + dupC;
    if (unexpectedDuplicateInsert !== 0) {
      throw new Error(`unexpected duplicate inserts: ${unexpectedDuplicateInsert}`);
    }

    const backends = await waitForBackendRestore();
    const persistedCount = await waitForPersistedCount(totalEvents);
    const summary = await waitForSummaryCount(instanceId, totalEvents);

    console.log("real backend wan hard-failure verification: OK");
    console.log(JSON.stringify({
      expected_total: totalEvents,
      summary_event_count: Number(summary.event_count || 0),
      summary_total_score: Number(summary.total_score || 0),
      persisted_ledger_events: persistedCount,
      unexpected_duplicate_insert_count: unexpectedDuplicateInsert,
      proxies_cut_phase: {
        mode: cutMode,
        pg: pgProxyName,
        redis: redisProxyName,
      },
      runtime_backends: backends,
    }, null, 2));
  } finally {
    try { await setProxyEnabled(pgProxyName, true); } catch {}
    try { await setProxyEnabled(redisProxyName, true); } catch {}
    child.kill("SIGTERM");
    await sleep(250);
    await rm(tempDir, { recursive: true, force: true });
    if (stderr.trim()) console.error(stderr.trim());
    if (stdout.trim()) console.error(stdout.trim());
  }
}

main().catch((err) => {
  console.error(`wan-hardfail verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
