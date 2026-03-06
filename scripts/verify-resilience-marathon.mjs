import { spawn } from "node:child_process";

const durationMs = Number(process.env.VERIFY_MARATHON_DURATION_MS || 300000);
const perCycleChaosRuns = Number(process.env.VERIFY_MARATHON_CHAOS_RUNS || 2);
const phaseMin = Number(process.env.VERIFY_MARATHON_PHASE_MIN || 1200);
const phaseMax = Number(process.env.VERIFY_MARATHON_PHASE_MAX || 2600);

function parseJsonBlock(output) {
  const start = output.indexOf("{");
  const end = output.lastIndexOf("}");
  if (start < 0 || end < start) {
    throw new Error(`JSON payload parse failed\n${output}`);
  }
  return JSON.parse(output.slice(start, end + 1));
}

function runNodeScript(scriptPath, envOverrides) {
  return new Promise((resolve, reject) => {
    const child = spawn("node", [scriptPath], {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ...envOverrides,
      },
      stdio: ["ignore", "pipe", "pipe"],
    });

    const maxLogChars = 400000;
    let out = "";
    let err = "";
    const appendLimited = (current, chunk) => {
      const next = current + chunk;
      if (next.length <= maxLogChars) {
        return next;
      }
      return next.slice(next.length - maxLogChars);
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

function ensureValidConfig() {
  if (!Number.isFinite(durationMs) || durationMs < 60000) {
    throw new Error(`invalid VERIFY_MARATHON_DURATION_MS: ${durationMs}`);
  }
  if (!Number.isFinite(perCycleChaosRuns) || perCycleChaosRuns < 1) {
    throw new Error(`invalid VERIFY_MARATHON_CHAOS_RUNS: ${perCycleChaosRuns}`);
  }
  if (!Number.isFinite(phaseMin) || !Number.isFinite(phaseMax) || phaseMin < 1 || phaseMax < phaseMin) {
    throw new Error(`invalid phase bounds: min=${phaseMin}, max=${phaseMax}`);
  }
}

async function main() {
  ensureValidConfig();

  const startedAt = Date.now();
  const deadline = startedAt + durationMs;

  let cycle = 0;
  let totalChaosEvents = 0;
  let totalChaosRuns = 0;
  let totalStalledEvents = 0;
  let retryBoundaryPass = 0;
  let requeueBoundaryPass = 0;

  const cycleResults = [];

  while (Date.now() < deadline) {
    cycle += 1;

    const chaosStart = Date.now();
    const chaos = await runNodeScript("./scripts/verify-real-backends-chaos-campaign.mjs", {
      VERIFY_CHAOS_CAMPAIGN_RUNS: String(perCycleChaosRuns),
      VERIFY_CHAOS_CAMPAIGN_PHASE_MIN: String(phaseMin),
      VERIFY_CHAOS_CAMPAIGN_PHASE_MAX: String(phaseMax),
      VERIFY_CHAOS_CAMPAIGN_BASE_PORT: String(25600 + cycle * 10),
    });
    const chaosPayload = parseJsonBlock(chaos.out);
    const chaosElapsed = Date.now() - chaosStart;

    const boundaryStart = Date.now();
    const boundary = await runNodeScript("./scripts/verify-bullmq-job-state-boundaries.mjs", {});
    const boundaryPayload = parseJsonBlock(boundary.out);
    const boundaryElapsed = Date.now() - boundaryStart;

    const stalled = Number(boundaryPayload.event_counts?.stalled || 0);
    const retryState = String(boundaryPayload.retry_job?.final_state || "");
    const requeueState = String(boundaryPayload.requeue_job?.final_state || "");
    const requeueActiveOnB = Boolean(boundaryPayload.requeue_job?.became_active_on_worker_b);

    totalChaosEvents += Number(chaosPayload.total_events || 0);
    totalChaosRuns += Number(chaosPayload.runs || 0);
    totalStalledEvents += stalled;
    if (retryState === "completed") {
      retryBoundaryPass += 1;
    }
    if (requeueState === "completed" && requeueActiveOnB) {
      requeueBoundaryPass += 1;
    }

    cycleResults.push({
      cycle,
      chaos_runs: Number(chaosPayload.runs || 0),
      chaos_total_events: Number(chaosPayload.total_events || 0),
      chaos_elapsed_ms: chaosElapsed,
      boundary_stalled_events: stalled,
      boundary_retry_final_state: retryState,
      boundary_requeue_final_state: requeueState,
      boundary_requeue_active_on_worker_b: requeueActiveOnB,
      boundary_elapsed_ms: boundaryElapsed,
    });

    console.log(`resilience marathon cycle ${cycle}: OK (chaos ${chaosElapsed}ms + boundary ${boundaryElapsed}ms)`);
  }

  const totalElapsedMs = Date.now() - startedAt;
  if (cycle < 1) {
    throw new Error("no marathon cycle executed");
  }

  console.log("resilience marathon verification: OK");
  console.log(JSON.stringify({
    duration_target_ms: durationMs,
    elapsed_ms: totalElapsedMs,
    cycles: cycle,
    total_chaos_runs: totalChaosRuns,
    total_chaos_events: totalChaosEvents,
    total_stalled_events_observed: totalStalledEvents,
    retry_boundary_pass_cycles: retryBoundaryPass,
    requeue_boundary_pass_cycles: requeueBoundaryPass,
    cycle_results: cycleResults,
  }, null, 2));
}

main().catch((err) => {
  console.error(`resilience marathon verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
