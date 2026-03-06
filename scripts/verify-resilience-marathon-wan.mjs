import { spawn } from "node:child_process";

const durationMs = Number(process.env.VERIFY_MARATHON_WAN_DURATION_MS || 300000);
const chaosRuns = Number(process.env.VERIFY_MARATHON_WAN_CHAOS_RUNS || 1);
const wanBurstRuns = Number(process.env.VERIFY_MARATHON_WAN_BURST_RUNS || 2);
const phaseMin = Number(process.env.VERIFY_MARATHON_WAN_PHASE_MIN || 1200);
const phaseMax = Number(process.env.VERIFY_MARATHON_WAN_PHASE_MAX || 2200);
const stepMaxRetries = Number(process.env.VERIFY_MARATHON_WAN_STEP_RETRIES || 3);

function parseJsonBlock(output) {
  const start = output.indexOf("{");
  const end = output.lastIndexOf("}");
  if (start < 0 || end < start) {
    throw new Error(`unable to parse JSON payload\n${output}`);
  }
  return JSON.parse(output.slice(start, end + 1));
}

function runScript(scriptPath, envOverrides) {
  return new Promise((resolve, reject) => {
    const child = spawn("node", [scriptPath], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ...envOverrides,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    const maxChars = 450000;
    let out = "";
    let err = "";
    const appendLimited = (current, chunk) => {
      const next = current + chunk;
      return next.length <= maxChars ? next : next.slice(next.length - maxChars);
    };

    child.stdout.on("data", (d) => {
      out = appendLimited(out, String(d));
    });
    child.stderr.on("data", (d) => {
      err = appendLimited(err, String(d));
    });

    child.on("exit", (code) => {
      if (code !== 0) {
        reject(new Error(`${scriptPath} failed (${code})\n${out}\n${err}`));
        return;
      }
      resolve({ out, err });
    });
  });
}

function validateConfig() {
  if (!Number.isFinite(durationMs) || durationMs < 120000) {
    throw new Error(`invalid VERIFY_MARATHON_WAN_DURATION_MS: ${durationMs}`);
  }
  if (!Number.isFinite(chaosRuns) || chaosRuns < 1) {
    throw new Error(`invalid VERIFY_MARATHON_WAN_CHAOS_RUNS: ${chaosRuns}`);
  }
  if (!Number.isFinite(wanBurstRuns) || wanBurstRuns < 1) {
    throw new Error(`invalid VERIFY_MARATHON_WAN_BURST_RUNS: ${wanBurstRuns}`);
  }
  if (!Number.isFinite(phaseMin) || !Number.isFinite(phaseMax) || phaseMin < 1 || phaseMax < phaseMin) {
    throw new Error(`invalid phase bounds: min=${phaseMin}, max=${phaseMax}`);
  }
  if (!Number.isFinite(stepMaxRetries) || stepMaxRetries < 1) {
    throw new Error(`invalid VERIFY_MARATHON_WAN_STEP_RETRIES: ${stepMaxRetries}`);
  }
}

async function runWithRetries(scriptPath, envOverrides) {
  let attempt = 0;
  let lastError = "";
  while (attempt < stepMaxRetries) {
    attempt += 1;
    try {
      const result = await runScript(scriptPath, envOverrides);
      return { result, retriesUsed: attempt };
    } catch (err) {
      lastError = String(err && err.message ? err.message : err);
    }
  }
  throw new Error(`${scriptPath} exhausted retries (${stepMaxRetries}): ${lastError}`);
}

async function main() {
  validateConfig();

  const startedAt = Date.now();
  const deadline = startedAt + durationMs;

  let cycle = 0;
  let totalChaosEvents = 0;
  let totalChaosRuns = 0;
  let totalBoundaryStalled = 0;
  let totalWanBurstRuns = 0;
  let retryBoundaryPassCycles = 0;
  let requeueBoundaryPassCycles = 0;

  const cycleResults = [];

  while (Date.now() < deadline) {
    cycle += 1;

    const chaosStarted = Date.now();
    const chaosRun = await runWithRetries("./scripts/verify-real-backends-chaos-campaign.mjs", {
      VERIFY_CHAOS_CAMPAIGN_RUNS: String(chaosRuns),
      VERIFY_CHAOS_CAMPAIGN_PHASE_MIN: String(phaseMin),
      VERIFY_CHAOS_CAMPAIGN_PHASE_MAX: String(phaseMax),
      VERIFY_CHAOS_CAMPAIGN_BASE_PORT: String(25800 + cycle * 20),
    });
    const chaosPayload = parseJsonBlock(chaosRun.result.out);
    const chaosElapsed = Date.now() - chaosStarted;

    const boundaryStarted = Date.now();
    const boundaryRun = await runWithRetries("./scripts/verify-bullmq-job-state-boundaries.mjs", {});
    const boundaryPayload = parseJsonBlock(boundaryRun.result.out);
    const boundaryElapsed = Date.now() - boundaryStarted;

    const wanStarted = Date.now();
    const wanRun = await runWithRetries("./scripts/verify-real-backends-wan-burst.mjs", {
      VERIFY_WAN_BURST_RUNS: String(wanBurstRuns),
      VERIFY_WAN_BURST_RUN_MAX_RETRIES: "3",
    });
    const wanPayload = parseJsonBlock(wanRun.result.out);
    const wanElapsed = Date.now() - wanStarted;

    const stalled = Number(boundaryPayload.event_counts?.stalled || 0);
    const retryState = String(boundaryPayload.retry_job?.final_state || "");
    const requeueState = String(boundaryPayload.requeue_job?.final_state || "");
    const requeueActiveOnB = Boolean(boundaryPayload.requeue_job?.became_active_on_worker_b);

    totalChaosEvents += Number(chaosPayload.total_events || 0);
    totalChaosRuns += Number(chaosPayload.runs || 0);
    totalBoundaryStalled += stalled;
    totalWanBurstRuns += Number(wanPayload.runs || 0);

    if (retryState === "completed") {
      retryBoundaryPassCycles += 1;
    }
    if (requeueState === "completed" && requeueActiveOnB) {
      requeueBoundaryPassCycles += 1;
    }

    cycleResults.push({
      cycle,
      chaos_runs: Number(chaosPayload.runs || 0),
      chaos_events: Number(chaosPayload.total_events || 0),
      chaos_elapsed_ms: chaosElapsed,
      chaos_step_retries_used: chaosRun.retriesUsed,
      boundary_stalled_events: stalled,
      boundary_retry_final_state: retryState,
      boundary_requeue_final_state: requeueState,
      boundary_requeue_active_on_worker_b: requeueActiveOnB,
      boundary_elapsed_ms: boundaryElapsed,
      boundary_step_retries_used: boundaryRun.retriesUsed,
      wan_burst_runs: Number(wanPayload.runs || 0),
      wan_burst_avg_elapsed_ms: Number(wanPayload.elapsed_ms?.avg || 0),
      wan_elapsed_ms: wanElapsed,
      wan_step_retries_used: wanRun.retriesUsed,
    });

    console.log(`resilience-wan marathon cycle ${cycle}: OK (chaos ${chaosElapsed}ms, boundary ${boundaryElapsed}ms, wan ${wanElapsed}ms)`);
  }

  const elapsedMs = Date.now() - startedAt;
  if (cycle < 1) {
    throw new Error("no cycle completed");
  }

  console.log("resilience wan marathon verification: OK");
  console.log(JSON.stringify({
    duration_target_ms: durationMs,
    elapsed_ms: elapsedMs,
    cycles: cycle,
    total_chaos_runs: totalChaosRuns,
    total_chaos_events: totalChaosEvents,
    total_boundary_stalled_events: totalBoundaryStalled,
    retry_boundary_pass_cycles: retryBoundaryPassCycles,
    requeue_boundary_pass_cycles: requeueBoundaryPassCycles,
    total_wan_burst_runs: totalWanBurstRuns,
    cycle_results: cycleResults,
  }, null, 2));
}

main().catch((err) => {
  console.error(`resilience wan marathon verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
