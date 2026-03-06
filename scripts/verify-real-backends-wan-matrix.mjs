import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";

const toxiproxyApi = process.env.TOXIPROXY_API || "http://127.0.0.1:8474";
const runs = Number(process.env.VERIFY_WAN_MATRIX_RUNS || 10);
const pgProxy = process.env.TOXIPROXY_PG_PROXY || "pg";
const redisProxy = process.env.TOXIPROXY_REDIS_PROXY || "redis";

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

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

async function ensureProxy(name, listen, upstream) {
  try {
    await api(`/proxies/${name}`);
  } catch {
    await api("/proxies", {
      method: "POST",
      body: JSON.stringify({ name, listen, upstream }),
    });
  }
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

async function addLatencyToxic(name, latency, jitter) {
  await api(`/proxies/${name}/toxics`, {
    method: "POST",
    body: JSON.stringify({
      name: `${name}-latency-${latency}-${jitter}`,
      type: "latency",
      stream: "downstream",
      toxicity: 1.0,
      attributes: { latency, jitter },
    }),
  });
}

async function addBandwidthToxic(name, rateKbps) {
  await api(`/proxies/${name}/toxics`, {
    method: "POST",
    body: JSON.stringify({
      name: `${name}-bandwidth-${rateKbps}`,
      type: "bandwidth",
      stream: "downstream",
      toxicity: 1.0,
      attributes: { rate: rateKbps },
    }),
  });
}

function runHardfailOnce(runIndex, profile) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      CONTROL_PG_DSN: pgDsn,
      CONTROL_REDIS_URL: redisUrl,
      VERIFY_WAN_HARD_BATCH_A: String(200),
      VERIFY_WAN_HARD_BATCH_B: String(200),
      VERIFY_WAN_HARD_BATCH_C: String(200),
      TOXIPROXY_API: toxiproxyApi,
      TOXIPROXY_PG_PROXY: pgProxy,
      TOXIPROXY_REDIS_PROXY: redisProxy,
      CONTROL_PG_TABLE: `control_plane_state_wanmatrix_${Date.now()}_${runIndex}`,
      CONTROL_PORT: String(25100 + runIndex),
      CONTROL_AUTH_TOKEN: `wan-matrix-${runIndex}-${randomUUID()}`,
    };

    const child = spawn("node", ["./scripts/verify-real-backends-wan-hardfail.mjs"], {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (d) => { stdout += String(d); });
    child.stderr.on("data", (d) => { stderr += String(d); });

    child.on("exit", (code) => {
      if (code === 0) {
        resolve({ ok: true, stdout, stderr, profile });
      } else {
        reject(new Error(`run ${runIndex} failed (${code})\n${stdout}\n${stderr}`));
      }
    });
  });
}

async function main() {
  await ensureProxy(pgProxy, "127.0.0.1:55433", "127.0.0.1:55432");
  await ensureProxy(redisProxy, "127.0.0.1:56380", "127.0.0.1:56379");

  const results = [];

  for (let i = 1; i <= runs; i += 1) {
    const profile = {
      pgLatency: randInt(80, 220),
      pgJitter: randInt(15, 80),
      pgBandwidth: randInt(800, 5000),
      redisLatency: randInt(60, 200),
      redisJitter: randInt(10, 70),
      redisBandwidth: randInt(1000, 6000),
    };

    await clearToxics(pgProxy);
    await clearToxics(redisProxy);
    await addLatencyToxic(pgProxy, profile.pgLatency, profile.pgJitter);
    await addBandwidthToxic(pgProxy, profile.pgBandwidth);
    await addLatencyToxic(redisProxy, profile.redisLatency, profile.redisJitter);
    await addBandwidthToxic(redisProxy, profile.redisBandwidth);

    const startedAt = Date.now();
    const runResult = await runHardfailOnce(i, profile);
    const elapsedMs = Date.now() - startedAt;
    results.push({ run: i, elapsed_ms: elapsedMs, profile: runResult.profile });
    console.log(`matrix run ${i}/${runs}: OK (${elapsedMs}ms)`);
  }

  await clearToxics(pgProxy);
  await clearToxics(redisProxy);

  const elapsedList = results.map((x) => x.elapsed_ms).sort((a, b) => a - b);
  const p50 = elapsedList[Math.floor(elapsedList.length * 0.5)] || 0;
  const p95 = elapsedList[Math.min(elapsedList.length - 1, Math.floor(elapsedList.length * 0.95))] || 0;

  console.log("real backend wan matrix verification: OK");
  console.log(JSON.stringify({
    runs,
    pass: results.length,
    fail: 0,
    elapsed_ms: {
      min: elapsedList[0] || 0,
      p50,
      p95,
      max: elapsedList[elapsedList.length - 1] || 0,
    },
    profiles: results,
  }, null, 2));
}

main().catch((err) => {
  console.error(`wan matrix verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
