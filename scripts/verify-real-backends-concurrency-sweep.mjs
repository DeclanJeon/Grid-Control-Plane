import { spawn } from "node:child_process";

const profile = [4, 16, 32, 64];
const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55432/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";

function runSoak(concurrency, index) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      CONTROL_PG_DSN: pgDsn,
      CONTROL_REDIS_URL: redisUrl,
      CONTROL_BULLMQ_WORKER_CONCURRENCY: String(concurrency),
      VERIFY_SOAK_TOTAL_EVENTS: process.env.VERIFY_SOAK_TOTAL_EVENTS || "4000",
      VERIFY_SOAK_WORKERS: process.env.VERIFY_SOAK_WORKERS || "4",
      VERIFY_SOAK_DUPLICATE_EVERY: process.env.VERIFY_SOAK_DUPLICATE_EVERY || "80",
      CONTROL_PORT: String(25300 + index),
      CONTROL_AUTH_TOKEN: `sweep-${concurrency}-${index}`,
      CONTROL_BULLMQ_QUEUE: `grid-ledger-events-sweep-${concurrency}-${Date.now()}`,
      CONTROL_PG_TABLE: `control_plane_state_sweep_${concurrency}_${Date.now()}`,
      TOXIPROXY_API: process.env.TOXIPROXY_API || "http://127.0.0.1:8474",
      TOXIPROXY_PG_PROXY: process.env.TOXIPROXY_PG_PROXY || "pg",
      TOXIPROXY_REDIS_PROXY: process.env.TOXIPROXY_REDIS_PROXY || "redis",
    };
    const child = spawn("node", ["./scripts/verify-real-backends-soak.mjs"], {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let out = "";
    let err = "";
    child.stdout.on("data", (d) => { out += String(d); });
    child.stderr.on("data", (d) => { err += String(d); });
    child.on("exit", (code) => {
      if (code === 0) {
        resolve({ out, err });
      } else {
        reject(new Error(`concurrency=${concurrency} failed (${code})\n${out}\n${err}`));
      }
    });
  });
}

async function main() {
  const results = [];
  let idx = 1;
  for (const concurrency of profile) {
    const started = Date.now();
    await runSoak(concurrency, idx);
    const elapsed = Date.now() - started;
    results.push({ concurrency, elapsed_ms: elapsed, status: "pass" });
    console.log(`concurrency ${concurrency}: OK (${elapsed}ms)`);
    idx += 1;
  }

  console.log("real backend concurrency sweep verification: OK");
  console.log(JSON.stringify({
    profile,
    pass: results.length,
    fail: 0,
    results,
  }, null, 2));
}

main().catch((err) => {
  console.error(`concurrency sweep verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
