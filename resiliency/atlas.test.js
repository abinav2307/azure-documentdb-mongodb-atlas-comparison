import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import {
  ATLAS_AGENT, ATLAS_REGIONS, AtlasApiError, AtlasClient, atlasResourceIds,
  validateAtlasManifest
} from "./atlas.js";
import { REGIONS, manifest, plannedDocument, prepare } from "./campaign.js";
import { buildInconclusiveCandidate, validateManifest } from "./campaign-coordinator.js";
import { buildFinalDocument } from "./finalize-run.js";
import { validate } from "./validate-run-document.js";

const PROJECT = "0123456789abcdef01234567";
const ENV = { ATLAS_CLIENT_ID: "test-client", ATLAS_CLIENT_SECRET: "test-client-secret" };
const schema = JSON.parse(await readFile(new URL("./run-document.schema.json", import.meta.url), "utf8"));

function fixture(initial = "M10", target = "M20") {
  const created = new Date("2026-01-01T00:00:00Z");
  const cluster = "abinav-resiliency-agent-eus2-abc12";
  const value = manifest("run-1", cluster, created, "eastus2", initial, target, PROJECT);
  const document = plannedDocument("run-1", cluster, created, "eastus2", initial, target, PROJECT);
  value.approvals.executionApproved = true;
  value.approvals.cleanupPreapproved = true;
  return { value, document };
}

function apiMock(value) {
  const [clusterPath, userPath] = atlasResourceIds(value);
  const resources = new Map();
  const calls = [];
  let tokens = 0;
  let ambiguousCreate = false;
  const fetchImpl = async (url, options) => {
    if (url.endsWith("/api/oauth/token")) {
      tokens += 1;
      assert.equal(options.headers.Authorization, `Basic ${Buffer.from("test-client:test-client-secret").toString("base64")}`);
      assert.equal(options.body, "grant_type=client_credentials");
      return Response.json({ access_token: "test-access-token", expires_in: 3600 });
    }
    assert.equal(options.headers.Authorization, "Bearer test-access-token");
    const path = new URL(url).pathname.replace("/api/atlas/v2", "");
    assert.equal(options.headers.Accept, path.includes("databaseUsers")
      ? "application/vnd.atlas.2023-01-01+json" : "application/vnd.atlas.2024-08-05+json");
    const body = options.body ? JSON.parse(options.body) : undefined;
    calls.push({ method: options.method, path, body });
    if (options.method === "GET") {
      return resources.has(path) ? Response.json(resources.get(path)) : Response.json({}, { status: 404 });
    }
    if (options.method === "POST" && path.endsWith("/clusters")) {
      resources.set(clusterPath, {
        ...body, groupId: PROJECT, stateName: "IDLE",
        connectionStrings: { standardSrv: "mongodb+srv://test.example.mongodb.net" }
      });
      resources.get(clusterPath).replicationSpecs[0].id = "replica-set-id";
      if (ambiguousCreate) {
        ambiguousCreate = false;
        throw new TypeError("fetch failed");
      }
    } else if (options.method === "POST" && path.endsWith("/databaseUsers")) {
      resources.set(userPath, { ...body, groupId: PROJECT });
    } else if (options.method === "PATCH") {
      resources.set(path, { ...resources.get(path), ...body });
    } else if (options.method === "DELETE") {
      resources.delete(path);
    } else assert.fail(`Unexpected request ${options.method} ${path}`);
    return Response.json({});
  };
  return {
    resources, calls, fetchImpl, tokens: () => tokens,
    ambiguousCreate: () => { ambiguousCreate = true; }
  };
}

test("Atlas applies eastus2 only, one-step tiers and 32 GB while preserving workload settings", () => {
  assert.deepEqual(REGIONS.map((region) => ATLAS_REGIONS[region]),
    ["US_EAST_2"]);
  for (const [initial, target] of [["M10", "M20"], ["M20", "M30"]]) {
    const { value, document } = fixture(initial, target);
    assert.equal(value.experiment.baselineSeconds, 120);
    assert.equal(value.experiment.postScaleSeconds, 120);
    assert.equal(value.experiment.timeoutSeconds, 5);
    assert.equal(value.experiment.maxDataPlaneRetries, 0);
    assert.equal(value.experiment.operationsPerSecondCeiling, 0);
    assert.equal(value.experiment.storageSizeGb, 32);
    assert.equal(value.experiment.highAvailability, "AtlasReplicaSet");
    assert.equal(value.atlas.regionName, "US_EAST_2");
    assert.equal(document.agentName, ATLAS_AGENT);
    assert.equal(document.product, "MongoDB Atlas");
    assert.equal(value.schemaVersion, 3);
    assert.equal(document.provider, "atlas");
    assert.deepEqual(validate(document, schema), []);
    validateAtlasManifest(value, { env: {} });
  }
});

test("Atlas manifests reject project mismatches, resource tampering and missing approval", () => {
  for (const change of [
    (value) => { value.atlas.projectId = "invalid"; },
    (value) => { value.atlas.regionName = "US_EAST"; },
    (value) => { value.atlas.replicaSetNodeCount = 1; },
    (value) => { value.atlas.username = "someone-else"; },
    (value) => { value.ownedResourceIds = ["/groups/another-project/clusters/other"]; },
    (value) => { value.plannedResourceIds.reverse(); },
    (value) => { value.approvals.cleanupPreapproved = false; },
    (value) => { value.experiment.workloadThreads = 100; }
  ]) {
    const { value } = fixture();
    change(value);
    assert.throws(() => validateAtlasManifest(value, { env: {} }));
  }
  assert.throws(() => validateAtlasManifest(fixture().value, {
    env: { ATLAS_PROJECT_ID: "ffffffffffffffffffffffff" }
  }), /does not match/);
});

test("Atlas provisioning and scaling reject out-of-policy tiers before any API request", async () => {
  const client = new AtlasClient({
    env: ENV, fetchImpl: async () => assert.fail("Out-of-policy tiers must not reach the Atlas API")
  });
  for (const [initial, target] of [["M50", "M60"], ["M40", "M60"], ["M20", "M200"], ["M30", "M40"], ["M10", "M30"], ["M20", "M40"]]) {
    const { value } = fixture();
    Object.assign(value.experiment, { initialComputeTier: initial, targetComputeTier: target });
    await assert.rejects(client.provision(value), /Unsupported initial|Unapproved SKU transition/);
    await assert.rejects(client.scaleBody(value), /Unsupported initial|Unapproved SKU transition/);
  }
});

test("Atlas is the only campaign provider and preparation writes schema-valid plans", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-prepare-"));
  try {
    await mkdir(resolve(root, "resiliency"));
    await writeFile(resolve(root, "resiliency", "run-document.schema.json"), JSON.stringify(schema));
    const path = await prepare(root, 1, () => ["eastus2", "M10", "M20"], {
      atlasProjectId: PROJECT, clusterLabel: "eus2"
    });
    const plan = JSON.parse(await readFile(path, "utf8"));
    assert.equal(plan.provider, "atlas");
    assert.equal(plan.atlasProjectId, PROJECT);
    assert.equal(plan.approved, false);
    const manifestPath = resolve(root, plan.manifests[0]);
    const value = JSON.parse(await readFile(manifestPath, "utf8"));
    value.approvals.executionApproved = true;
    value.approvals.cleanupPreapproved = true;
    await writeFile(manifestPath, JSON.stringify(value));
    await validateManifest(manifestPath, { env: {} });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("generation and Atlas requests enforce region and storage policy before network access", async () => {
  const client = new AtlasClient({
    env: ENV, fetchImpl: async () => assert.fail("Invalid configurations must not reach Atlas")
  });
  const created = new Date("2026-01-01T00:00:00Z");
  for (const region of REGIONS) {
    for (const [initial, target] of [["M10", "M20"], ["M20", "M30"]]) {
      const value = manifest("run-1", "abinav-resiliency-agent-rnd-abc12", created, region, initial, target, PROJECT);
      value.approvals.executionApproved = true;
      value.approvals.cleanupPreapproved = true;
      validateAtlasManifest(value, { env: {} });
      for (const tier of [initial, target]) {
        const config = client.clusterBody(value, tier).replicationSpecs[0].regionConfigs[0];
        assert.equal(config.regionName, ATLAS_REGIONS[region]);
        assert.deepEqual(config.electableSpecs, { instanceSize: tier, nodeCount: 3, diskSizeGB: 32 });
        assert.equal(config.autoScaling.diskGB.enabled, false);
        assert.equal(config.autoScaling.compute.enabled, false);
      }
      const document = plannedDocument("run-1", "cluster", created, region, initial, target, PROJECT);
      assert.deepEqual(validate(document, schema), []);
    }
  }
  for (const region of ["westus2", "eastus", "northcentralus", "westeurope", "westus", "centralus", "invalid"]) {
    assert.throws(() => manifest("run-1", "cluster", created, region, "M10", "M20", PROJECT),
      /Unsupported Atlas Azure region/);
    assert.throws(() => plannedDocument("run-1", "cluster", created, region, "M10", "M20", PROJECT),
      /Unsupported Atlas Azure region/);
  }
  for (const storage of [0, 31, 33, 128, 1024, "32"]) {
    assert.throws(() => manifest("run-1", "cluster", created, "eastus2", "M10", "M20", PROJECT, storage),
      /Unapproved storage size/);
    assert.throws(() => plannedDocument("run-1", "cluster", created, "eastus2", "M10", "M20", PROJECT, storage),
      /Unapproved storage size/);
  }
  const mutations = [
    ...["westus2", "eastus", "northcentralus", "westeurope"].map((region) => (value) => {
      value.location = region;
      value.atlas.regionName = ATLAS_REGIONS[region];
    }),
    ...[31, 33, 128, 1024, "32"].map((storage) => (value) => { value.experiment.storageSizeGb = storage; })
  ];
  const root = await mkdtemp(resolve(tmpdir(), "atlas-policy-"));
  try {
    for (const mutate of mutations) {
      const { value } = fixture();
      mutate(value);
      assert.throws(() => validateAtlasManifest(value, { env: {} }), /Unapproved region|Unapproved storage/);
      assert.throws(() => client.clusterBody(value, "M10"), /Unapproved region|Unapproved storage/);
      await assert.rejects(client.provision(value), /Unapproved region|Unapproved storage/);
      await assert.rejects(client.scaleBody(value), /Unapproved region|Unapproved storage/);
      const path = resolve(root, "manifest.json");
      await writeFile(path, JSON.stringify(value));
      await assert.rejects(validateManifest(path, { env: {} }), /Unapproved region|Unapproved storage/);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("result validation rejects larger disks, other regions and non-adjacent tiers", () => {
  for (const change of [
    (document) => { document.initialComputeTier = "M30"; document.targetComputeTier = "M40"; },
    (document) => { document.targetComputeTier = "M30"; },
    (document) => { document.storageSizeGb = 128; },
    (document) => { document.storageSizeGb = "32"; },
    ...["westus2", "eastus", "northcentralus", "westeurope"].map((region) => (document) => { document.region = region; })
  ]) {
    const { document } = fixture();
    change(document);
    assert.notDeepEqual(validate(document, schema), []);
  }
});

test("ownership-verified cleanup remains available for legacy disks, tiers and regions", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-legacy-cleanup-"));
  try {
    const { value } = fixture();
    value.location = "westeurope";
    value.atlas.regionName = "EUROPE_WEST";
    value.clusterName = "abinav-resiliency-agent-rnd-abc12";
    Object.assign(value.experiment, {
      initialComputeTier: "M30", targetComputeTier: "M50", storageSizeGb: 1024, workloadThreads: 2
    });
    value.plannedResourceIds = atlasResourceIds(value);
    const [clusterPath] = value.plannedResourceIds;
    value.ownedResourceIds = [clusterPath];
    const path = resolve(root, "manifest.json");
    await writeFile(path, JSON.stringify(value));
    await assert.rejects(validateManifest(path, { env: {} }), /Unsupported initial/);
    await validateManifest(path, { env: {}, cleanupOnly: true });
    const mock = apiMock(value);
    mock.resources.set(clusterPath, {
      name: value.clusterName, groupId: PROJECT, stateName: "IDLE",
      tags: [{ key: "runId", value: value.runId }, { key: "purpose", value: "atlas-resiliency" }]
    });
    const client = new AtlasClient({ env: ENV, fetchImpl: mock.fetchImpl, wait: async () => {} });
    const evidence = await client.cleanup(value, path, resolve(root, "cleanup.json"));
    assert.ok(evidence.cleanupCompletedUtc);
    assert.equal(mock.resources.size, 0);
    assert.deepEqual(mock.calls.filter((call) => call.method === "DELETE").map((call) => call.path), [clusterPath]);
    value.ownedResourceIds = ["/groups/another-project/clusters/other"];
    await assert.rejects(client.cleanup(value, path, resolve(root, "cleanup.json")), /outside the approved run/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas mocked create, exact tier scale and verified cleanup use only run-owned resources", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-lifecycle-"));
  try {
    const { value } = fixture();
    const mock = apiMock(value);
    const client = new AtlasClient({ env: ENV, fetchImpl: mock.fetchImpl, wait: async () => {} });
    const events = [];
    const writer = { write: async (event, details) => events.push({ event, ...details }) };
    const path = resolve(root, "manifest.json");
    const resource = await client.provision(value, path, "test-db-password", writer, () => false);
    assert.equal(events.at(-1).provisioning_state, "Succeeded");
    const created = mock.calls.find((call) => call.method === "POST" && call.path.endsWith("/clusters")).body;
    assert.equal(created.diskSizeGB, undefined);
    assert.deepEqual(created.replicationSpecs[0].regionConfigs[0].electableSpecs, {
      instanceSize: "M10", nodeCount: 3, diskSizeGB: 32
    });
    assert.equal(created.replicationSpecs[0].regionConfigs[0].autoScaling.compute.enabled, false);
    const user = mock.calls.find((call) => call.method === "POST" && call.path.endsWith("/databaseUsers")).body;
    assert.deepEqual(user.roles, [{ roleName: "readWrite", databaseName: "resiliencyTestDB" }]);
    assert.deepEqual(user.scopes, [{ name: value.clusterName, type: "CLUSTER" }]);
    const uri = client.connectionString(resource, value, "test@password");
    assert.match(uri, /test%40password@/);
    assert.match(uri, /retryWrites=false&retryReads=false/);
    assert.doesNotMatch(await readFile(path, "utf8"), /test-db-password|test-client-secret|test-access-token/);
    const [clusterPath] = atlasResourceIds(value);
    const scale = await client.scaleBody(value);
    assert.deepEqual(scale.replicationSpecs[0].regionConfigs[0].electableSpecs, {
      instanceSize: "M20", nodeCount: 3, diskSizeGB: 32
    });
    assert.equal(scale.replicationSpecs[0].id, "replica-set-id");
    assert.equal(scale.name, undefined);
    await client.request("PATCH", clusterPath, { body: scale });
    await client.waitCluster(value, writer, "scale-up", "M20");
    value.experiment.cleanupPolicy = "retain";
    const evidence = await client.cleanup(value, path, resolve(root, "cleanup.json"));
    assert.ok(evidence.cleanupCompletedUtc);
    assert.equal(mock.resources.size, 0);
    assert.equal(mock.tokens(), 1);
    assert.deepEqual(mock.calls.filter((call) => call.method === "DELETE").map((call) => call.path),
      [...atlasResourceIds(value)].reverse());
    assert.ok(mock.calls.every((call) => call.path.startsWith(`/groups/${PROJECT}/`)));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas resumed cleanup tolerates already-absent resources and skips a repeated delete", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-cleanup-resume-"));
  try {
    const { value } = fixture();
    const mock = apiMock(value);
    value.ownedResourceIds = atlasResourceIds(value);
    const client = new AtlasClient({ env: ENV, fetchImpl: mock.fetchImpl });
    const result = await client.cleanup(value, resolve(root, "manifest.json"), resolve(root, "cleanup.json"));
    assert.ok(result.cleanupCompletedUtc);
    assert.equal(mock.calls.some((call) => call.method === "DELETE"), false);
    value.ownedResourceIds = [];
    await assert.rejects(client.cleanup(value, resolve(root, "manifest.json"), resolve(root, "cleanup.json")), /No Atlas resources/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas retries ambiguous creates without duplicating or adopting foreign resources", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-retry-"));
  try {
    const { value } = fixture();
    const mock = apiMock(value);
    const client = new AtlasClient({ env: ENV, fetchImpl: mock.fetchImpl, wait: async () => {} });
    const writer = { write: async () => {} };
    mock.ambiguousCreate();
    await assert.rejects(client.provision(value, resolve(root, "manifest.json"), "test-password", writer), /fetch failed/);
    assert.deepEqual(value.ownedResourceIds, atlasResourceIds(value));
    await client.provision(value, resolve(root, "manifest.json"), "test-password", writer);
    assert.equal(mock.calls.filter((call) => call.method === "POST" && call.path.endsWith("/clusters")).length, 1);
    value.ownedResourceIds = [];
    await assert.rejects(client.provision(value, resolve(root, "manifest.json"), "test-password", writer), /refusing to adopt/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas rejects coerced storage and failed workload; waits past a stale IDLE tier", async () => {
  const { value } = fixture();
  const mock = apiMock(value);
  const client = new AtlasClient({ env: ENV, fetchImpl: mock.fetchImpl, wait: async () => {
    const cluster = mock.resources.get(atlasResourceIds(value)[0]);
    cluster.replicationSpecs[0].regionConfigs[0].electableSpecs.instanceSize = "M20";
  } });
  const resource = { ...client.clusterBody(value, "M10"), stateName: "IDLE", groupId: PROJECT };
  mock.resources.set(atlasResourceIds(value)[0], resource);
  const writer = { write: async () => {} };
  await client.waitCluster(value, writer, "scale-up", "M20");
  resource.replicationSpecs[0].regionConfigs[0].electableSpecs.diskSizeGB = 256;
  await assert.rejects(client.waitCluster(value, writer, "provision", "M20"), /exact planned/);
  await assert.rejects(client.waitCluster(value, writer, "scale-up", "M20", {
    exitCode: 1, signalCode: null
  }), /Workload exited/);
});

test("Atlas errors exclude response secrets and tokens refresh before expiry", async () => {
  let tokens = 0;
  const client = new AtlasClient({ env: ENV, fetchImpl: async (url) => {
    if (url.endsWith("/api/oauth/token")) {
      tokens += 1;
      return Response.json({ access_token: "token", expires_in: 3600 });
    }
    return Response.json({ errorCode: "INVALID_DISK_SIZE", detail: "secret-password", password: "secret" }, {
      status: 400, headers: { "x-mongodb-request-id": "atlas-request-123" }
    });
  } });
  const path = `/groups/${PROJECT}/clusters/test`;
  await assert.rejects(client.request("POST", path, { body: { password: "secret" } }), (error) => {
    assert.ok(error instanceof AtlasApiError);
    assert.equal(error.code, "INVALID_DISK_SIZE");
    assert.equal(error.controlPlaneOperationId, "atlas-request-123");
    assert.doesNotMatch(error.stack, /secret-password/);
    return true;
  });
  await assert.rejects(client.request("GET", path), AtlasApiError);
  assert.equal(tokens, 1);
  client.tokenExpires = 0;
  await client.accessToken();
  assert.equal(tokens, 2);
  await assert.rejects(client.request("GET", "/groups/invalid/clusters/test"), /Invalid Atlas/);
  assert.throws(() => new AtlasClient({ env: {} }), /ATLAS_CLIENT_ID/);
});

test("Atlas SRV URLs preserve provider options but explicitly disable both driver retries", () => {
  const { value } = fixture();
  const client = new AtlasClient({ env: ENV });
  const uri = new URL(client.connectionString({
    connectionStrings: { standardSrv: "mongodb+srv://test.example.mongodb.net/?w=majority&retryReads=true&retryWrites=true" }
  }, value, "test@password"));
  assert.equal(uri.searchParams.get("w"), "majority");
  assert.equal(uri.searchParams.get("retryReads"), "false");
  assert.equal(uri.searchParams.get("retryWrites"), "false");
  assert.equal(decodeURIComponent(uri.password), "test@password");
  assert.throws(() => client.connectionString({
    connectionStrings: { standardSrv: "mongodb://invalid.example" }
  }, value, "password"), /valid SRV/);
});

test("Atlas cleanup rejects unowned resources and still deletes the other owned resource", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-cleanup-"));
  try {
    const { value } = fixture();
    const mock = apiMock(value);
    const notifications = [];
    const client = new AtlasClient({
      env: ENV, fetchImpl: mock.fetchImpl, wait: async () => {},
      notify: async (failure) => notifications.push(failure)
    });
    await client.provision(value, resolve(root, "manifest.json"), "password", { write: async () => {} });
    const [clusterPath, userPath] = atlasResourceIds(value);
    mock.resources.get(userPath).scopes = [{ name: "foreign-cluster", type: "CLUSTER" }];
    await assert.rejects(client.cleanup(value, resolve(root, "manifest.json"), resolve(root, "cleanup.json")), /ownership/);
    assert.equal(mock.resources.has(clusterPath), false);
    assert.equal(mock.resources.has(userPath), true);
    assert.equal(notifications[0].provider, "atlas");
    const evidence = JSON.parse(await readFile(resolve(root, "cleanup.json"), "utf8"));
    assert.equal(evidence.cleanupCompletedUtc, null);
    assert.equal(evidence.failure.operation, "cleanup");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas result schema requires the exact root product value", () => {
  const { document } = fixture();
  delete document.product;
  assert.ok(validate(document, schema).some((error) => error.includes("product")));
  for (const product of ["atlas", "MongoDB", "", null, 123]) {
    document.product = product;
    assert.ok(validate(document, schema).some((error) => error.includes("/product")));
  }
  document.product = "MongoDB Atlas";
  assert.deepEqual(validate(document, schema), []);
});

test("Atlas final and inconclusive results retain provider identity and existing metrics", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-finalize-"));
  try {
    const { value, document: planned } = fixture();
    const control = [
      { event: "run_start", timestamp_utc: "2026-01-01T00:00:00.000Z" },
      { event: "provision_attempt_start", timestamp_utc: "2026-01-01T00:00:01.000Z" },
      {
        event: "state_transition", operation: "provision", provisioning_state: "Succeeded",
        cluster_status: "Ready", timestamp_utc: "2026-01-01T00:00:30.000Z"
      },
      { event: "provision_attempt_end", success: true, timestamp_utc: "2026-01-01T00:00:30.000Z" },
      { event: "scale_submitted", timestamp_utc: "2026-01-01T00:01:00.000Z" },
      { event: "scale_terminal", elapsed_seconds: 60, timestamp_utc: "2026-01-01T00:02:00.000Z" },
      { event: "run_complete", timestamp_utc: "2026-01-01T00:03:00.000Z" }
    ];
    const operations = [{
      kind: "data_plane_operation", run_id: value.runId, thread_id: 1,
      phase: "scale-up", operation: "insert", success: false, error_type: "ConnectionFailure",
      started_utc: "2026-01-01T00:01:01.000Z", ended_utc: "2026-01-01T00:01:02.000Z", latency_ms: 1000
    }];
    for (const [name, events] of [["control-plane", control], ["operations", operations]]) {
      await writeFile(resolve(root, `${name}.jsonl`), events.map((event) => JSON.stringify(event)).join("\n") + "\n");
    }
    const { document } = await buildFinalDocument(root, value, { endUtc: "2026-01-01T00:03:00.000Z" });
    assert.equal(document.agentName, ATLAS_AGENT);
    assert.equal(document.product, "MongoDB Atlas");
    assert.equal(document.atlasProjectId, PROJECT);
    assert.equal(document.aggregateMetrics.allOperations.failed, 1);
    assert.deepEqual(document.availabilityDips[0].errorCategoryCounts, { ConnectionFailure: 1 });
    assert.equal(document.aggregateMetrics.perThread[0].allOperations.failed, 1);
    assert.deepEqual(validate(document, schema), []);
    const failed = await buildInconclusiveCandidate({
      planned, manifest: value, resultDirectory: root, cleanup: null, error: new Error("test")
    });
    assert.equal(failed.provider, "atlas");
    assert.equal(failed.product, "MongoDB Atlas");
    assert.equal(failed.aggregateMetrics.allOperations.failed, 1);
    assert.deepEqual(failed.availabilityDips[0].errorCategoryCounts, { ConnectionFailure: 1 });
    assert.deepEqual(validate(failed, schema), []);
    delete document.atlasProjectId;
    assert.ok(validate(document, schema).some((error) => error.includes("Atlas result identity")));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
