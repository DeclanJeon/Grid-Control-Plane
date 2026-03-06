import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";
import { setTimeout as sleep } from "node:timers/promises";

const host = process.env.CONTROL_HOST || "127.0.0.1";
const port = Number(process.env.CONTROL_PORT || 25080);
const token = process.env.CONTROL_AUTH_TOKEN || `verify-${randomUUID()}`;
const baseUrl = `http://${host}:${port}`;

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:6379";

async function waitForReady(urlBase) {
  const started = Date.now();
  while (Date.now() - started < 15000) {
    try {
      const r = await fetch(`${urlBase}/health`);
      if (r.ok) {
        return;
      }
    } catch {
      // retry
    }
    await sleep(150);
  }
  throw new Error("control-plane not ready in time");
}

async function authAdminGet(urlBase, reqToken, pathname) {
  const response = await fetch(`${urlBase}${pathname}`, {
    headers: {
      authorization: `Bearer ${reqToken}`,
      "x-grid-role": "admin",
    },
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function main() {
  const tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-real-backend-"));
  const dataFile = path.join(tempDir, "state.json");

  const child = spawn("node", ["./server.mjs"], {
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
      RATE_LIMIT_MAX: "1000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (d) => {
    stdout += String(d);
  });
  child.stderr?.on("data", (d) => {
    stderr += String(d);
  });

  try {
    await waitForReady(baseUrl);
    const backends = await authAdminGet(baseUrl, token, "/v1/admin/runtime/backends");
    if (backends.status !== 200) {
      throw new Error(`runtime/backends status=${backends.status}`);
    }
    const readiness = await authAdminGet(baseUrl, token, "/v1/admin/runtime/readiness");
    if (readiness.status !== 200) {
      throw new Error(`runtime/readiness status=${readiness.status}`);
    }

    if (backends.json.storage.effective !== "postgres") {
      throw new Error(`storage backend not postgres: ${backends.json.storage.effective}`);
    }
    if (backends.json.queue.effective !== "redis_bullmq") {
      throw new Error(`queue backend not redis_bullmq: ${backends.json.queue.effective}`);
    }
    if (!backends.json.storage.initialized) {
      throw new Error(`storage backend not initialized: ${backends.json.storage.runtime_error || "unknown"}`);
    }
    if (!backends.json.queue.initialized) {
      throw new Error(`queue backend not initialized: ${backends.json.queue.runtime_error || "unknown"}`);
    }

    console.log("real backend verification: OK");
    console.log(JSON.stringify({
      storage: backends.json.storage,
      queue: backends.json.queue,
      ready_for_production: readiness.json.ready_for_production,
      blockers: readiness.json.blockers,
    }, null, 2));
  } finally {
    child.kill("SIGTERM");
    await sleep(200);
    await rm(tempDir, { recursive: true, force: true });
    if (stderr.trim()) {
      console.error(stderr.trim());
    }
    if (stdout.trim()) {
      console.error(stdout.trim());
    }
  }
}

main().catch((err) => {
  console.error(`real backend verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
