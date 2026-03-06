import { spawn } from "node:child_process";

const runs = Number(process.env.VERIFY_CHAOS_CAMPAIGN_RUNS || 5);
const phaseMin = Number(process.env.VERIFY_CHAOS_CAMPAIGN_PHASE_MIN || 1200);
const phaseMax = Number(process.env.VERIFY_CHAOS_CAMPAIGN_PHASE_MAX || 2600);
const basePort = Number(process.env.VERIFY_CHAOS_CAMPAIGN_BASE_PORT || 25500);

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function extractJsonBlock(output) {
  const start = output.indexOf("{");
  const end = output.lastIndexOf("}");
  if (start < 0 || end < start) {
    throw new Error(`failed to parse verifier JSON payload\n${output}`);
  }
  return JSON.parse(output.slice(start, end + 1));
}

function runChaosCombo(index, phases) {
  return new Promise((resolve, reject) => {
    const env = {
      ...process.env,
      CONTROL_PORT: String(basePort + index),
      CONTROL_AUTH_TOKEN: `campaign-${index}-${Date.now()}`,
      CONTROL_BULLMQ_QUEUE: `grid-ledger-events-chaos-campaign-${index}-${Date.now()}`,
      CONTROL_PG_TABLE: `control_plane_state_chaos_campaign_${index}_${Date.now()}`,
      VERIFY_CHAOS_PHASE_A: String(phases.phaseA),
      VERIFY_CHAOS_PHASE_B: String(phases.phaseB),
      VERIFY_CHAOS_PHASE_C: String(phases.phaseC),
    };

    const child = spawn("node", ["./scripts/verify-real-backends-chaos-combo.mjs"], {
      cwd: process.cwd(),
      env,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const maxLogChars = 300000;
    let out = "";
    let err = "";
    const appendLimited = (current, chunk) => {
      const next = current + chunk;
      if (next.length <= maxLogChars) return next;
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
        reject(new Error(`chaos run ${index} failed (${code})\n${out}\n${err}`));
        return;
      }

      try {
        const payload = extractJsonBlock(out);
        resolve({ payload, out, err });
      } catch (parseErr) {
        reject(new Error(`chaos run ${index} parse error: ${parseErr.message}\n${out}\n${err}`));
      }
    });
  });
}

async function main() {
  if (!Number.isFinite(runs) || runs < 1) {
    throw new Error(`invalid VERIFY_CHAOS_CAMPAIGN_RUNS: ${runs}`);
  }
  if (!Number.isFinite(phaseMin) || !Number.isFinite(phaseMax) || phaseMin < 1 || phaseMax < phaseMin) {
    throw new Error(`invalid phase bounds: min=${phaseMin}, max=${phaseMax}`);
  }

  const details = [];
  for (let i = 1; i <= runs; i += 1) {
    const phases = {
      phaseA: randomInt(phaseMin, phaseMax),
      phaseB: randomInt(phaseMin, phaseMax),
      phaseC: randomInt(phaseMin, phaseMax),
    };
    const started = Date.now();
    const result = await runChaosCombo(i, phases);
    const elapsedMs = Date.now() - started;

    const expectedTotal = Number(result.payload.expected_total || 0);
    const summaryCount = Number(result.payload.summary_event_count || 0);
    const duplicateCount = Number(result.payload.unexpected_duplicate_insert_count || 0);
    const queueCounts = result.payload.queue_counts || {};
    const queueHealthy = Number(queueCounts.active || 0) === 0
      && Number(queueCounts.waiting || 0) === 0
      && Number(queueCounts.failed || 0) === 0
      && Number(queueCounts.delayed || 0) === 0;

    if (summaryCount !== expectedTotal) {
      throw new Error(`run ${i} summary mismatch: expected=${expectedTotal}, got=${summaryCount}`);
    }
    if (duplicateCount !== 0) {
      throw new Error(`run ${i} duplicate mismatch: ${duplicateCount}`);
    }
    if (!queueHealthy) {
      throw new Error(`run ${i} queue not drained: ${JSON.stringify(queueCounts)}`);
    }

    details.push({
      run: i,
      phase_a: phases.phaseA,
      phase_b: phases.phaseB,
      phase_c: phases.phaseC,
      expected_total: expectedTotal,
      summary_event_count: summaryCount,
      elapsed_ms: elapsedMs,
      queue_counts: queueCounts,
      runtime_backends: result.payload.runtime_backends,
    });

    console.log(`chaos campaign run ${i}/${runs}: OK (${elapsedMs}ms)`);
  }

  const elapsedValues = details.map((d) => d.elapsed_ms);
  const totalEvents = details.reduce((sum, d) => sum + d.expected_total, 0);
  const totalElapsedMs = elapsedValues.reduce((sum, v) => sum + v, 0);

  console.log("real backend chaos-campaign verification: OK");
  console.log(JSON.stringify({
    runs,
    phase_min: phaseMin,
    phase_max: phaseMax,
    total_events: totalEvents,
    total_elapsed_ms: totalElapsedMs,
    min_elapsed_ms: Math.min(...elapsedValues),
    max_elapsed_ms: Math.max(...elapsedValues),
    avg_elapsed_ms: Math.round(totalElapsedMs / runs),
    details,
  }, null, 2));
}

main().catch((err) => {
  console.error(`chaos-campaign verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
