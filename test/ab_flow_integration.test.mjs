import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { after, before, test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";

const token = "test-token";
const host = "127.0.0.1";
const port = 20000 + Math.floor(Math.random() * 10000);
const baseUrl = `http://${host}:${port}`;

let child = null;
let tempDir = "";

async function waitForServerReady() {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < 8000) {
    try {
      const response = await fetch(`${baseUrl}/health`);
      if (response.ok) {
        return;
      }
    } catch (err) {
      lastError = err;
    }
    await sleep(100);
  }
  const reason = lastError ? `: ${String(lastError)}` : "";
  throw new Error(`control-plane did not become ready in time${reason}`);
}

async function waitForServerReadyAt(urlBase) {
  const startedAt = Date.now();
  let lastError = null;
  while (Date.now() - startedAt < 8000) {
    try {
      const response = await fetch(`${urlBase}/health`);
      if (response.ok) {
        return;
      }
    } catch (err) {
      lastError = err;
    }
    await sleep(100);
  }
  const reason = lastError ? `: ${String(lastError)}` : "";
  throw new Error(`control-plane did not become ready in time${reason}`);
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

async function authGet(pathname) {
  const response = await fetch(`${baseUrl}${pathname}`, {
    headers: {
      authorization: `Bearer ${token}`,
    },
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

async function authAdminGetAt(urlBase, reqToken, pathname) {
  const response = await fetch(`${urlBase}${pathname}`, {
    headers: {
      authorization: `Bearer ${reqToken}`,
      "x-grid-role": "admin",
    },
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function authPostAt(urlBase, reqToken, pathname, payload) {
  const response = await fetch(`${urlBase}${pathname}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${reqToken}`,
    },
    body: JSON.stringify(payload),
  });
  const json = await response.json();
  return { status: response.status, json };
}

async function authGetAt(urlBase, reqToken, pathname) {
  const response = await fetch(`${urlBase}${pathname}`, {
    headers: {
      authorization: `Bearer ${reqToken}`,
    },
  });
  const json = await response.json();
  return { status: response.status, json };
}

before(async () => {
  tempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-test-"));
  const dataFile = path.join(tempDir, "state.json");
  child = spawn("node", ["./server.mjs"], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CONTROL_HOST: host,
      CONTROL_PORT: String(port),
      CONTROL_AUTH_TOKEN: token,
      CONTROL_DATA_FILE: dataFile,
      RATE_LIMIT_MAX: "1000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  await waitForServerReady();
});

after(async () => {
  if (child) {
    child.kill("SIGTERM");
    await sleep(200);
  }
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
  }
});

test("A/B flow increments download count", async () => {
  const owner = `inst-a-${randomUUID()}`;
  const downloader = `inst-b-${randomUUID()}`;

  const registerRes = await authPost("/v1/files/register", {
    owner_instance_id: owner,
    name: "sample-file.bin",
    size: 1024,
    sha256: "abc123",
    piece_size: 256,
    piece_count: 4,
  });

  assert.equal(registerRes.status, 200);
  assert.equal(registerRes.json.ok, true);
  assert.ok(registerRes.json.item.file_id);
  const fileId = registerRes.json.item.file_id;

  const listBefore = await authGet(`/v1/files?limit=100&query=${encodeURIComponent(fileId)}`);
  assert.equal(listBefore.status, 200);
  assert.equal(listBefore.json.count, 1);
  assert.equal(listBefore.json.items[0].download_count, 0);

  const downloadedRes = await authPost(`/v1/files/${encodeURIComponent(fileId)}/downloaded`, {
    downloader_instance_id: downloader,
  });
  assert.equal(downloadedRes.status, 200);
  assert.equal(downloadedRes.json.ok, true);
  assert.equal(downloadedRes.json.item.download_count, 1);

  const listAfter = await authGet(`/v1/files?limit=100&query=${encodeURIComponent(fileId)}`);
  assert.equal(listAfter.status, 200);
  assert.equal(listAfter.json.count, 1);
  assert.equal(listAfter.json.items[0].download_count, 1);
  assert.ok(Array.isArray(listAfter.json.items[0].providers));
  assert.ok(listAfter.json.items[0].providers.includes(downloader));
});

test("query ranking uses prefix and trigram search score", async () => {
  const owner = `inst-q-${randomUUID()}`;

  const samples = [
    "alpha-video.mkv",
    "alpine-guide.pdf",
    "zeta.bin",
  ];

  for (const name of samples) {
    const registerRes = await authPost("/v1/files/register", {
      owner_instance_id: owner,
      name,
      size: 4096,
      sha256: "def456",
      piece_size: 512,
      piece_count: 8,
    });
    assert.equal(registerRes.status, 200);
    assert.equal(registerRes.json.ok, true);
  }

  const ranked = await authGet("/v1/files?limit=100&ranking_mode=policy&query=alp");
  assert.equal(ranked.status, 200);
  assert.equal(ranked.json.count, 2);
  assert.ok(Array.isArray(ranked.json.items));
  assert.equal(ranked.json.items[0].name, "alpha-video.mkv");
  assert.equal(ranked.json.items[1].name, "alpine-guide.pdf");
  assert.ok(Number(ranked.json.items[0].search_score) >= Number(ranked.json.items[1].search_score));
});

test("file list exposes provider availability_score components", async () => {
  const owner = `inst-p-${randomUUID()}`;

  const presenceRes = await authPost("/v1/presence/sync", {
    instance_id: owner,
    host: "local",
    app: "desktop-tauri",
    nodes: [
      {
        role: "peer",
        mode: "runtime",
        broker_url: "http://127.0.0.1:18080",
        active: true,
      },
    ],
  });
  assert.equal(presenceRes.status, 200);

  const eventRes = await authPost("/v1/ledger/events", {
    external_event_id: `evt-${randomUUID()}`,
    instance_id: owner,
    type: "relay_success",
    value: 2,
  });
  assert.equal(eventRes.status, 200);
  assert.equal(eventRes.json.ok, true);

  const registerRes = await authPost("/v1/files/register", {
    owner_instance_id: owner,
    name: "provider-score.bin",
    size: 2048,
    sha256: "feedface",
    piece_size: 512,
    piece_count: 4,
  });
  assert.equal(registerRes.status, 200);
  const fileId = registerRes.json.item.file_id;

  const listRes = await authGet(`/v1/files?limit=200&query=${encodeURIComponent("provider-score.bin")}`);
  assert.equal(listRes.status, 200);
  const item = listRes.json.items.find((x) => x.file_id === fileId);
  assert.ok(item);
  assert.ok(Array.isArray(item.provider_candidates));
  assert.equal(item.provider_candidates.length, 1);

  const provider = item.provider_candidates[0];
  assert.equal(provider.instance_id, owner);
  assert.ok(Number(provider.uptime_weight) > 0);
  assert.ok(Number(provider.relay_success_weight) > 0);
  assert.ok(Number(provider.trust_weight) >= 0);
  assert.ok(Number(provider.availability_score) > 0);
});

test("session assignment applies failed-path EMA penalty", async () => {
  const providerA = `inst-s-a-${randomUUID()}`;
  const providerB = `inst-s-b-${randomUUID()}`;
  const requester = `inst-s-r-${randomUUID()}`;

  const registerRes = await authPost("/v1/files/register", {
    owner_instance_id: providerA,
    name: "session-ema.bin",
    size: 1024,
    sha256: "cafebabe",
    piece_size: 256,
    piece_count: 4,
  });
  assert.equal(registerRes.status, 200);
  const fileId = registerRes.json.item.file_id;

  const firstSession = await authPost("/v1/sessions/create", {
    file_id: fileId,
    requester_instance_id: requester,
    provider_candidates: [providerA, providerB],
  });
  assert.equal(firstSession.status, 200);
  const sessionId = firstSession.json.item.session_id;
  assert.equal(firstSession.json.item.selected_provider_instance_id, providerA);

  const failUpdate = await authPost(`/v1/sessions/${encodeURIComponent(sessionId)}`, {
    selected_provider_instance_id: providerA,
    path_state: "direct",
    status: "failed",
    failure_reason: "timeout",
  });
  assert.equal(failUpdate.status, 200);
  assert.ok(Number(failUpdate.json.item.path_penalty_ema) > 0);

  const secondSession = await authPost("/v1/sessions/create", {
    file_id: fileId,
    requester_instance_id: `${requester}-2`,
    provider_candidates: [providerA, providerB],
  });
  assert.equal(secondSession.status, 200);
  const candidates = secondSession.json.item.provider_candidates;
  assert.equal(candidates.length, 2);
  const a = candidates.find((x) => x.instance_id === providerA);
  const b = candidates.find((x) => x.instance_id === providerB);
  assert.ok(a);
  assert.ok(b);
  assert.ok(Number(a.path_penalty_ema) > 0);
  assert.ok(Number(a.assignment_score) < Number(a.availability_score));
  assert.equal(Number(b.path_penalty_ema), 0);
});

test("admin audit endpoint includes mutation logs", async () => {
  const owner = `inst-audit-${randomUUID()}`;
  const registerRes = await authPost("/v1/files/register", {
    owner_instance_id: owner,
    name: "audit-sample.bin",
    size: 1024,
    sha256: "abcd",
    piece_size: 256,
    piece_count: 4,
  });
  assert.equal(registerRes.status, 200);
  assert.equal(registerRes.json.ok, true);

  const auditRes = await authAdminGet("/v1/admin/audit?limit=200");
  assert.equal(auditRes.status, 200);
  assert.ok(Array.isArray(auditRes.json.items));
  const found = auditRes.json.items.some(
    (x) => x.path === "/v1/files/register" && x.result === "ok"
  );
  assert.equal(found, true);

  const filteredRes = await authAdminGet(
    `/v1/admin/audit?limit=200&path=${encodeURIComponent("/v1/files/register")}&result=OK&since_minutes=5`
  );
  assert.equal(filteredRes.status, 200);
  assert.ok(Array.isArray(filteredRes.json.items));
  assert.ok(filteredRes.json.items.length >= 1);
  assert.ok(filteredRes.json.items.every((x) => x.path === "/v1/files/register"));
  assert.ok(filteredRes.json.items.every((x) => String(x.result || "").toLowerCase() === "ok"));
});

test("admin runtime backend endpoint exposes requested and effective backends", async () => {
  const backendRes = await authAdminGet("/v1/admin/runtime/backends");
  assert.equal(backendRes.status, 200);
  assert.ok(backendRes.json.storage);
  assert.ok(backendRes.json.queue);

  assert.equal(backendRes.json.storage.requested, "file");
  assert.equal(backendRes.json.storage.effective, "file");
  assert.ok(Array.isArray(backendRes.json.storage.supported));
  assert.ok(backendRes.json.storage.supported.includes("file"));
  assert.equal(backendRes.json.storage.migration_ready, true);
  assert.equal(backendRes.json.storage.effective_reason, "configured_supported");

  assert.equal(backendRes.json.queue.requested, "inline");
  assert.equal(backendRes.json.queue.effective, "inline");
  assert.ok(Array.isArray(backendRes.json.queue.supported));
  assert.ok(backendRes.json.queue.supported.includes("inline"));
  assert.equal(backendRes.json.queue.migration_ready, true);
  assert.equal(backendRes.json.queue.effective_reason, "configured_supported");
  assert.equal(backendRes.json.queue.mode, "process_local");
});

test("admin runtime readiness endpoint exposes blockers", async () => {
  const readinessRes = await authAdminGet("/v1/admin/runtime/readiness");
  assert.equal(readinessRes.status, 200);
  assert.ok(Array.isArray(readinessRes.json.blockers));
  assert.equal(readinessRes.json.ready_for_production, false);
  assert.ok(Number(readinessRes.json.blocker_count) >= 2);
  assert.ok(readinessRes.json.blockers.includes("storage_not_externalized"));
  assert.ok(readinessRes.json.blockers.includes("queue_not_externalized"));
  assert.ok(readinessRes.json.backends);
  assert.equal(readinessRes.json.backends.storage.effective, "file");
  assert.equal(readinessRes.json.backends.queue.effective, "inline");
});

test("file_journal queue backend replays ledger events after state file removal", async () => {
  const localHost = "127.0.0.1";
  const localPort = 22000 + Math.floor(Math.random() * 10000);
  const localBase = `http://${localHost}:${localPort}`;
  const localToken = `tok-${randomUUID()}`;
  const localTempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-journal-test-"));
  const localDataFile = path.join(localTempDir, "state.json");
  const localJournalFile = path.join(localTempDir, "ledger.journal");
  const instanceId = `inst-j-${randomUUID()}`;

  const spawnServer = () => spawn("node", ["./server.mjs"], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CONTROL_HOST: localHost,
      CONTROL_PORT: String(localPort),
      CONTROL_AUTH_TOKEN: localToken,
      CONTROL_DATA_FILE: localDataFile,
      CONTROL_QUEUE_BACKEND: "file_journal",
      CONTROL_QUEUE_JOURNAL_FILE: localJournalFile,
      RATE_LIMIT_MAX: "1000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  let localChild = spawnServer();
  try {
    await waitForServerReadyAt(localBase);

    const readinessRes = await authAdminGetAt(localBase, localToken, "/v1/admin/runtime/readiness");
    assert.equal(readinessRes.status, 200);
    assert.equal(readinessRes.json.backends.queue.effective, "file_journal");

    const ingestRes = await authPostAt(localBase, localToken, "/v1/ledger/events", {
      external_event_id: `evt-j-${randomUUID()}`,
      instance_id: instanceId,
      type: "relay_success",
      value: 2,
    });
    assert.equal(ingestRes.status, 200);
    assert.equal(ingestRes.json.ok, true);

    const beforeRestart = await authGetAt(localBase, localToken, `/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    assert.equal(beforeRestart.status, 200);
    assert.equal(beforeRestart.json.event_count, 1);

    localChild.kill("SIGTERM");
    await sleep(200);

    await rm(localDataFile, { force: true });

    localChild = spawnServer();
    await waitForServerReadyAt(localBase);

    const afterRestart = await authGetAt(localBase, localToken, `/v1/ledger/summary?instance_id=${encodeURIComponent(instanceId)}`);
    assert.equal(afterRestart.status, 200);
    assert.equal(afterRestart.json.event_count, 1);
    assert.ok(Number(afterRestart.json.total_score) > 0);
  } finally {
    if (localChild) {
      localChild.kill("SIGTERM");
      await sleep(200);
    }
    await rm(localTempDir, { recursive: true, force: true });
  }
});

test("runtime readiness reports config-missing fallback for postgres and redis_bullmq", async () => {
  const localHost = "127.0.0.1";
  const localPort = 23000 + Math.floor(Math.random() * 10000);
  const localBase = `http://${localHost}:${localPort}`;
  const localToken = `tok-${randomUUID()}`;
  const localTempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-backend-missing-"));
  const localDataFile = path.join(localTempDir, "state.json");

  let localChild = spawn("node", ["./server.mjs"], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CONTROL_HOST: localHost,
      CONTROL_PORT: String(localPort),
      CONTROL_AUTH_TOKEN: localToken,
      CONTROL_DATA_FILE: localDataFile,
      CONTROL_STORAGE_BACKEND: "postgres",
      CONTROL_QUEUE_BACKEND: "redis_bullmq",
      RATE_LIMIT_MAX: "1000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    await waitForServerReadyAt(localBase);
    const readinessRes = await authAdminGetAt(localBase, localToken, "/v1/admin/runtime/readiness");
    assert.equal(readinessRes.status, 200);
    assert.equal(readinessRes.json.backends.storage.requested, "postgres");
    assert.equal(readinessRes.json.backends.storage.effective, "file");
    assert.equal(readinessRes.json.backends.storage.effective_reason, "missing_config_fallback");
    assert.equal(readinessRes.json.backends.queue.requested, "redis_bullmq");
    assert.equal(readinessRes.json.backends.queue.effective, "inline");
    assert.equal(readinessRes.json.backends.queue.effective_reason, "missing_config_fallback");
    assert.ok(readinessRes.json.blockers.includes("storage_backend_config_missing"));
    assert.ok(readinessRes.json.blockers.includes("queue_backend_config_missing"));
  } finally {
    if (localChild) {
      localChild.kill("SIGTERM");
      await sleep(200);
    }
    await rm(localTempDir, { recursive: true, force: true });
  }
});

test("runtime readiness reports runtime_init_fallback when postgres/redis init fails", async () => {
  const localHost = "127.0.0.1";
  const localPort = 24000 + Math.floor(Math.random() * 10000);
  const localBase = `http://${localHost}:${localPort}`;
  const localToken = `tok-${randomUUID()}`;
  const localTempDir = await mkdtemp(path.join(tmpdir(), "grid-control-plane-backend-init-fail-"));
  const localDataFile = path.join(localTempDir, "state.json");

  let localChild = spawn("node", ["./server.mjs"], {
    cwd: path.resolve("."),
    env: {
      ...process.env,
      CONTROL_HOST: localHost,
      CONTROL_PORT: String(localPort),
      CONTROL_AUTH_TOKEN: localToken,
      CONTROL_DATA_FILE: localDataFile,
      CONTROL_STORAGE_BACKEND: "postgres",
      CONTROL_PG_DSN: "postgres://invalid:invalid@127.0.0.1:1/none",
      CONTROL_QUEUE_BACKEND: "redis_bullmq",
      CONTROL_REDIS_URL: "redis://127.0.0.1:1",
      RATE_LIMIT_MAX: "1000",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  try {
    await waitForServerReadyAt(localBase);
    const readinessRes = await authAdminGetAt(localBase, localToken, "/v1/admin/runtime/readiness");
    assert.equal(readinessRes.status, 200);
    assert.equal(readinessRes.json.backends.storage.requested, "postgres");
    assert.equal(readinessRes.json.backends.storage.effective, "file");
    assert.equal(readinessRes.json.backends.storage.effective_reason, "runtime_init_fallback");
    assert.equal(readinessRes.json.backends.queue.requested, "redis_bullmq");
    assert.equal(readinessRes.json.backends.queue.effective, "inline");
    assert.equal(readinessRes.json.backends.queue.effective_reason, "runtime_init_fallback");
  } finally {
    if (localChild) {
      localChild.kill("SIGTERM");
      await sleep(200);
    }
    await rm(localTempDir, { recursive: true, force: true });
  }
});
