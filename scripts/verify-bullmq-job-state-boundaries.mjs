import { randomUUID } from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";

const redisUrl = process.env.CONTROL_REDIS_URL || "redis://127.0.0.1:56379";
const queueName = process.env.VERIFY_BULLMQ_QUEUE || `grid-bullmq-boundary-${Math.floor(Date.now() / 1000)}-${randomUUID().slice(0, 8)}`;

async function waitFor(condition, timeoutMs, message) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const value = await condition();
    if (value) {
      return value;
    }
    await sleep(150);
  }
  throw new Error(message);
}

async function main() {
  const mod = await import("bullmq");
  const Queue = mod.Queue;
  const Worker = mod.Worker;
  const QueueEvents = mod.QueueEvents;

  if (!Queue || !Worker || !QueueEvents) {
    throw new Error("bullmq Queue/Worker/QueueEvents are required");
  }

  const connection = { url: redisUrl };
  const queue = new Queue(queueName, { connection });
  const queueEvents = new QueueEvents(queueName, { connection });
  let workerA = null;
  let workerB = null;

  try {
    await queueEvents.waitUntilReady();

    const eventCounts = {
      waiting: 0,
      active: 0,
      completed: 0,
      failed: 0,
      stalled: 0,
    };

    queueEvents.on("waiting", () => { eventCounts.waiting += 1; });
    queueEvents.on("active", () => { eventCounts.active += 1; });
    queueEvents.on("completed", () => { eventCounts.completed += 1; });
    queueEvents.on("failed", () => { eventCounts.failed += 1; });
    queueEvents.on("stalled", () => { eventCounts.stalled += 1; });

    const attemptCounter = new Map();
    let requeueJobId = "";
    let requeueBecameActive = false;

    workerA = new Worker(
      queueName,
      async (job) => {
        const key = String(job.id || job.name || randomUUID());
        const next = Number(attemptCounter.get(key) || 0) + 1;
        attemptCounter.set(key, next);

        if (job.name === "retry-once" && next === 1) {
          throw new Error("intentional retry-once failure");
        }
        if (job.name === "always-fail") {
          throw new Error("intentional always-fail failure");
        }
        if (job.name === "requeue-after-worker-stop") {
          await sleep(15000);
        }

        return {
          worker: "A",
          attempts_seen: next,
        };
      },
      {
        connection,
        concurrency: 1,
        lockDuration: 2000,
        stalledInterval: 1000,
        maxStalledCount: 2,
      }
    );

  const retryJob = await queue.add("retry-once", { marker: "retry" }, {
    attempts: 2,
    backoff: { type: "fixed", delay: 200 },
    removeOnComplete: 100,
    removeOnFail: 100,
    jobId: `retry-${randomUUID()}`,
  });

  const failedJob = await queue.add("always-fail", { marker: "fail" }, {
    attempts: 2,
    backoff: { type: "fixed", delay: 200 },
    removeOnComplete: 100,
    removeOnFail: 100,
    jobId: `fail-${randomUUID()}`,
  });

  const requeueJob = await queue.add("requeue-after-worker-stop", { marker: "requeue" }, {
    attempts: 3,
    backoff: { type: "fixed", delay: 200 },
    removeOnComplete: 100,
    removeOnFail: 100,
    jobId: `requeue-${randomUUID()}`,
  });
  requeueJobId = String(requeueJob.id || "");

  await waitFor(async () => {
    const active = await requeueJob.getState();
    return active === "active";
  }, 15000, "requeue job did not become active on worker A");

    await workerA.close(true);
    workerA = null;

    workerB = new Worker(
      queueName,
      async (job) => {
        const key = String(job.id || job.name || randomUUID());
        const next = Number(attemptCounter.get(key) || 0) + 1;
        attemptCounter.set(key, next);

        if (job.name === "retry-once" && next === 1) {
          throw new Error("intentional retry-once failure");
        }
        if (job.name === "always-fail") {
          throw new Error("intentional always-fail failure");
        }
        if (String(job.id || "") === requeueJobId) {
          requeueBecameActive = true;
        }
        return {
          worker: "B",
          attempts_seen: next,
        };
      },
      {
        connection,
        concurrency: 2,
        lockDuration: 2000,
        stalledInterval: 1000,
        maxStalledCount: 2,
      }
    );

  await waitFor(async () => {
    const j = await queue.getJob(String(retryJob.id));
    if (!j) return false;
    const state = await j.getState();
    return state === "completed";
  }, 30000, "retry-once job did not complete");

  await waitFor(async () => {
    const j = await queue.getJob(String(failedJob.id));
    if (!j) return false;
    const state = await j.getState();
    return state === "failed";
  }, 30000, "always-fail job did not move to failed");

  await waitFor(async () => {
    const j = await queue.getJob(String(requeueJob.id));
    if (!j) return false;
    const state = await j.getState();
    return state === "completed";
  }, 40000, "requeue job did not complete after worker replacement");

  const retryFinal = await queue.getJob(String(retryJob.id));
  const failedFinal = await queue.getJob(String(failedJob.id));
  const requeueFinal = await queue.getJob(String(requeueJob.id));

  const retryAttemptsMade = Number(retryFinal?.attemptsMade || 0);
  const failedAttemptsMade = Number(failedFinal?.attemptsMade || 0);
  const requeueAttemptsSeen = Number(attemptCounter.get(requeueJobId) || 0);

  if (retryAttemptsMade < 1) {
    throw new Error(`retry-once attemptsMade unexpected: ${retryAttemptsMade}`);
  }
  if (failedAttemptsMade < 2) {
    throw new Error(`always-fail attemptsMade unexpected: ${failedAttemptsMade}`);
  }
  if (!requeueBecameActive || requeueAttemptsSeen < 2) {
    throw new Error(`requeue boundary not satisfied: active_on_worker_b=${requeueBecameActive}, attempts_seen=${requeueAttemptsSeen}`);
  }

  const queueCounts = await queue.getJobCounts(
    "active",
    "waiting",
    "completed",
    "failed",
    "delayed",
    "paused",
    "waiting-children"
  );

  const active = Number(queueCounts.active || 0);
  const waiting = Number(queueCounts.waiting || 0);
  const delayed = Number(queueCounts.delayed || 0);
  if (active !== 0 || waiting !== 0 || delayed !== 0) {
    throw new Error(`queue did not settle: ${JSON.stringify(queueCounts)}`);
  }

    console.log("bullmq job-state boundaries verification: OK");
    console.log(JSON.stringify({
      queue_name: queueName,
      retry_job: {
        id: String(retryJob.id),
        attempts_made: retryAttemptsMade,
        final_state: await retryFinal?.getState(),
      },
      failed_job: {
        id: String(failedJob.id),
        attempts_made: failedAttemptsMade,
        final_state: await failedFinal?.getState(),
      },
      requeue_job: {
        id: String(requeueJob.id),
        attempts_seen: requeueAttemptsSeen,
        became_active_on_worker_b: requeueBecameActive,
        final_state: await requeueFinal?.getState(),
      },
      event_counts: eventCounts,
      queue_counts: queueCounts,
    }, null, 2));
  } finally {
    if (workerA) {
      try { await workerA.close(true); } catch {}
    }
    if (workerB) {
      try { await workerB.close(); } catch {}
    }
    try { await queueEvents.close(); } catch {}
    try { await queue.close(); } catch {}
  }
}

main().catch((err) => {
  console.error(`bullmq boundary verification failed: ${err && err.message ? err.message : String(err)}`);
  process.exitCode = 1;
});
