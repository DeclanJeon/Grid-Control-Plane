import { spawn } from "node:child_process";

const toxiproxyApi = process.env.TOXIPROXY_API || "http://127.0.0.1:8474";
const pgProxy = process.env.TOXIPROXY_PG_PROXY || "pg";
const redisProxy = process.env.TOXIPROXY_REDIS_PROXY || "redis";

const pgDsn = process.env.CONTROL_PG_DSN || "postgres://grid:grid@127.0.0.1:55433/grid";
const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56380";
const scenarioMaxRetries = Number(process.env.VERIFY_WAN_LOSS_SCENARIO_RETRIES || 2);

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

async function addLatency(name, latency, jitter) {
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

async function addBandwidth(name, rate) {
  await api(`/proxies/${name}/toxics`, {
    method: "POST",
    body: JSON.stringify({
      name: `${name}-bandwidth-${rate}`,
      type: "bandwidth",
      stream: "downstream",
      toxicity: 1.0,
      attributes: { rate },
    }),
  });
}

async function addTimeout(name, timeoutMs, toxicity) {
  await api(`/proxies/${name}/toxics`, {
    method: "POST",
    body: JSON.stringify({
      name: `${name}-timeout-${timeoutMs}`,
      type: "timeout",
      stream: "downstream",
      toxicity,
      attributes: { timeout: timeoutMs },
    }),
  });
}

function runHardfailScenario(scenario, runIndex) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      CONTROL_PG_DSN: pgDsn,
      CONTROL_REDIS_URL: redisUrl,
      TOXIPROXY_API: toxiproxyApi,
      TOXIPROXY_PG_PROXY: pgProxy,
      TOXIPROXY_REDIS_PROXY: redisProxy,
      VERIFY_WAN_HARD_CUT_MODE: scenario.cutMode,
      VERIFY_WAN_HARD_BATCH_A: "200",
      VERIFY_WAN_HARD_BATCH_B: "200",
      VERIFY_WAN_HARD_BATCH_C: "200",
      CONTROL_PORT: String(25200 + runIndex),
      CONTROL_AUTH_TOKEN: `wan-loss-${runIndex}`,
      CONTROL_PG_TABLE: `control_plane_state_wanloss_${Date.now()}_${runIndex}`,
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
        resolve({ stdout, stderr });
      } else {
        reject(new Error(`scenario ${scenario.name} failed (${code})\n${stdout}\n${stderr}`));
      }
    });
  });
}

async function main() {
  const scenarios = [
    {
      name: "both-cut-with-timeout-toxics",
      cutMode: "both",
      pg: { latency: 150, jitter: 35, bandwidth: 2400, timeout: 550, toxicity: 0.22 },
      redis: { latency: 140, jitter: 30, bandwidth: 2800, timeout: 500, toxicity: 0.20 },
    },
    {
      name: "pg-only-partition",
      cutMode: "pg",
      pg: { latency: 220, jitter: 70, bandwidth: 1600, timeout: 900, toxicity: 0.4 },
      redis: { latency: 120, jitter: 30, bandwidth: 3500, timeout: 500, toxicity: 0.2 },
    },
    {
      name: "redis-only-partition",
      cutMode: "redis",
      pg: { latency: 120, jitter: 30, bandwidth: 3200, timeout: 500, toxicity: 0.2 },
      redis: { latency: 210, jitter: 60, bandwidth: 1700, timeout: 850, toxicity: 0.4 },
    },
  ];

  const results = [];
  let idx = 1;
  for (const scenario of scenarios) {
    let attempt = 0;
    let success = false;
    let elapsedMs = 0;
    let lastError = "";

    while (!success && attempt < Math.max(1, scenarioMaxRetries)) {
      attempt += 1;
      await clearToxics(pgProxy);
      await clearToxics(redisProxy);

      await addLatency(pgProxy, scenario.pg.latency, scenario.pg.jitter);
      await addBandwidth(pgProxy, scenario.pg.bandwidth);
      await addTimeout(pgProxy, scenario.pg.timeout, scenario.pg.toxicity);

      await addLatency(redisProxy, scenario.redis.latency, scenario.redis.jitter);
      await addBandwidth(redisProxy, scenario.redis.bandwidth);
      await addTimeout(redisProxy, scenario.redis.timeout, scenario.redis.toxicity);

      try {
        const startedAt = Date.now();
        await runHardfailScenario(scenario, idx);
        elapsedMs = Date.now() - startedAt;
        success = true;
      } catch (err) {
        lastError = String(err && err.message ? err.message : err);
      }
    }

    if (!success) {
      throw new Error(`scenario ${scenario.name} exhausted retries (${scenarioMaxRetries}): ${lastError}`);
    }

    results.push({ name: scenario.name, cut_mode: scenario.cutMode, elapsed_ms: elapsedMs, retries_used: attempt });
    idx += 1;
  }

  await clearToxics(pgProxy);
  await clearToxics(redisProxy);

  console.log("real backend wan loss/partition verification: OK");
  console.log(JSON.stringify({
    scenarios: results,
    pass: results.length,
    fail: 0,
  }, null, 2));
}

main().catch((err) => {
  console.error(`wan loss verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
