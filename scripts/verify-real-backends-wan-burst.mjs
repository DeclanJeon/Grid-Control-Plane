import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";

const toxiproxyApi = process.env.TOXIPROXY_API || "http://127.0.0.1:8474";
const pgProxy = process.env.TOXIPROXY_PG_PROXY || "pg";
const redisProxy = process.env.TOXIPROXY_REDIS_PROXY || "redis";

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";
const runs = Number(process.env.VERIFY_WAN_BURST_RUNS || 8);
const runMaxRetries = Number(process.env.VERIFY_WAN_BURST_RUN_MAX_RETRIES || 3);

function randInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomCutMode() {
  const modes = ["both", "pg", "redis"];
  return modes[randInt(0, modes.length - 1)];
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

async function clearToxics(name) {
  const proxy = await api(`/proxies/${name}`);
  const toxics = Array.isArray(proxy.toxics) ? proxy.toxics : [];
  for (const toxic of toxics) {
    if (toxic?.name) {
      await api(`/proxies/${name}/toxics/${toxic.name}`, { method: "DELETE" });
    }
  }
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

async function addToxic(proxy, body) {
  await api(`/proxies/${proxy}/toxics`, {
    method: "POST",
    body: JSON.stringify(body),
  });
}

async function applyBurstProfile(profile) {
  await clearToxics(pgProxy);
  await clearToxics(redisProxy);

  await addToxic(pgProxy, {
    name: `pg-lat-${profile.pg.latency}`,
    type: "latency",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { latency: profile.pg.latency, jitter: profile.pg.jitter },
  });
  await addToxic(pgProxy, {
    name: `pg-band-${profile.pg.bandwidth}`,
    type: "bandwidth",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { rate: profile.pg.bandwidth },
  });
  await addToxic(pgProxy, {
    name: `pg-timeout-${profile.pg.timeoutMs}`,
    type: "timeout",
    stream: "downstream",
    toxicity: profile.pg.timeoutToxicity,
    attributes: { timeout: profile.pg.timeoutMs },
  });
  await addToxic(pgProxy, {
    name: `pg-slicer-${profile.pg.sliceAverage}`,
    type: "slicer",
    stream: "downstream",
    toxicity: profile.pg.slicerToxicity,
    attributes: {
      average_size: profile.pg.sliceAverage,
      size_variation: profile.pg.sliceVariation,
      delay: profile.pg.sliceDelay,
    },
  });

  await addToxic(redisProxy, {
    name: `redis-lat-${profile.redis.latency}`,
    type: "latency",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { latency: profile.redis.latency, jitter: profile.redis.jitter },
  });
  await addToxic(redisProxy, {
    name: `redis-band-${profile.redis.bandwidth}`,
    type: "bandwidth",
    stream: "downstream",
    toxicity: 1.0,
    attributes: { rate: profile.redis.bandwidth },
  });
  await addToxic(redisProxy, {
    name: `redis-timeout-${profile.redis.timeoutMs}`,
    type: "timeout",
    stream: "downstream",
    toxicity: profile.redis.timeoutToxicity,
    attributes: { timeout: profile.redis.timeoutMs },
  });
  await addToxic(redisProxy, {
    name: `redis-slicer-${profile.redis.sliceAverage}`,
    type: "slicer",
    stream: "downstream",
    toxicity: profile.redis.slicerToxicity,
    attributes: {
      average_size: profile.redis.sliceAverage,
      size_variation: profile.redis.sliceVariation,
      delay: profile.redis.sliceDelay,
    },
  });
}

function runHardfail(runIndex, cutMode) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      CONTROL_PG_DSN: pgDsn,
      CONTROL_REDIS_URL: redisUrl,
      TOXIPROXY_API: toxiproxyApi,
      TOXIPROXY_PG_PROXY: pgProxy,
      TOXIPROXY_REDIS_PROXY: redisProxy,
      VERIFY_WAN_HARD_CUT_MODE: cutMode,
      VERIFY_WAN_HARD_BATCH_A: "250",
      VERIFY_WAN_HARD_BATCH_B: "250",
      VERIFY_WAN_HARD_BATCH_C: "250",
      VERIFY_WAN_HARD_WORKERS: "10",
      CONTROL_PORT: String(25700 + runIndex),
      CONTROL_AUTH_TOKEN: `wan-burst-${runIndex}-${randomUUID()}`,
      CONTROL_PG_TABLE: `control_plane_state_wanburst_${Date.now()}_${runIndex}`,
    };

    const child = spawn("node", ["./scripts/verify-real-backends-wan-hardfail.mjs"], {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    const maxChars = 250000;
    const append = (cur, chunk) => {
      const next = cur + chunk;
      return next.length <= maxChars ? next : next.slice(next.length - maxChars);
    };
    child.stdout.on("data", (d) => { stdout = append(stdout, String(d)); });
    child.stderr.on("data", (d) => { stderr = append(stderr, String(d)); });

    child.on("exit", (code) => {
      if (code === 0) {
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`wan burst run ${runIndex} failed (${code})\n${stdout}\n${stderr}`));
      }
    });
  });
}

function makeProfile() {
  return {
    pg: {
      latency: randInt(100, 260),
      jitter: randInt(15, 70),
      bandwidth: randInt(1100, 3000),
      timeoutMs: randInt(450, 900),
      timeoutToxicity: Number((randInt(12, 30) / 100).toFixed(2)),
      sliceAverage: randInt(100, 240),
      sliceVariation: randInt(20, 80),
      sliceDelay: randInt(5, 25),
      slicerToxicity: Number((randInt(18, 35) / 100).toFixed(2)),
    },
    redis: {
      latency: randInt(90, 230),
      jitter: randInt(12, 65),
      bandwidth: randInt(1200, 3400),
      timeoutMs: randInt(400, 850),
      timeoutToxicity: Number((randInt(10, 28) / 100).toFixed(2)),
      sliceAverage: randInt(110, 260),
      sliceVariation: randInt(20, 90),
      sliceDelay: randInt(5, 22),
      slicerToxicity: Number((randInt(16, 32) / 100).toFixed(2)),
    },
  };
}

async function main() {
  if (!Number.isFinite(runs) || runs < 1) {
    throw new Error(`invalid VERIFY_WAN_BURST_RUNS: ${runs}`);
  }

  await ensureProxy(pgProxy, "127.0.0.1:55433", "127.0.0.1:55432");
  await ensureProxy(redisProxy, "127.0.0.1:56380", "127.0.0.1:56379");

  const results = [];
  for (let i = 1; i <= runs; i += 1) {
    let attempt = 0;
    let runOk = false;
    let elapsedMs = 0;
    let profile = null;
    let cutMode = "both";
    let lastError = "";

    while (!runOk && attempt < Math.max(1, runMaxRetries)) {
      attempt += 1;
      profile = makeProfile();
      cutMode = randomCutMode();
      try {
        await applyBurstProfile(profile);
        const started = Date.now();
        await runHardfail(i, cutMode);
        elapsedMs = Date.now() - started;
        runOk = true;
      } catch (err) {
        lastError = String(err && err.message ? err.message : err);
      }
    }

    if (!runOk || !profile) {
      throw new Error(`wan burst run ${i} exhausted retries (${runMaxRetries}): ${lastError}`);
    }

    results.push({
      run: i,
      cut_mode: cutMode,
      elapsed_ms: elapsedMs,
      retries_used: attempt,
      profile,
    });
    console.log(`wan burst run ${i}/${runs}: OK (${elapsedMs}ms, cut=${cutMode}, retries=${attempt})`);
  }

  await clearToxics(pgProxy);
  await clearToxics(redisProxy);

  const elapsed = results.map((r) => r.elapsed_ms).sort((a, b) => a - b);
  const totalElapsed = elapsed.reduce((sum, v) => sum + v, 0);
  const p50 = elapsed[Math.floor(elapsed.length * 0.5)] || 0;
  const p95 = elapsed[Math.min(elapsed.length - 1, Math.floor(elapsed.length * 0.95))] || 0;

  console.log("real backend wan burst-pathology verification: OK");
  console.log(JSON.stringify({
    runs,
    pass: results.length,
    fail: 0,
    elapsed_ms: {
      min: elapsed[0] || 0,
      p50,
      p95,
      max: elapsed[elapsed.length - 1] || 0,
      avg: Math.round(totalElapsed / Math.max(1, results.length)),
    },
    results,
  }, null, 2));
}

main().catch(async (err) => {
  try { await clearToxics(pgProxy); } catch {}
  try { await clearToxics(redisProxy); } catch {}
  console.error(`wan burst verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
