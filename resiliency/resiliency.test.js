import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import test from "node:test";
import { BSON, MongoClient, MongoServerError } from "mongodb";
import {
  DEFAULT_ITERATIONS, INITIAL_SKUS, REGIONS, higherSkus, latestAtlasPlan,
  main as campaign, manifest, plannedDocument, prepare, randomConfiguration, storageSizeForInitialTier
} from "./campaign.js";
import { validateManifest } from "./campaign-coordinator.js";
import { main as orchestrate, workloadEnvironment } from "./orchestrate.js";
import { main as cleanup } from "./cleanup-run.js";
import { ResultSink } from "./result-sink.js";
import {
  aggregateRequestMetrics, buildExperienceReport, buildFinalDocument, bucketDips,
  controlPlaneFailures, phaseMetrics
} from "./finalize-run.js";
import { driverMetadata } from "./driver-info.js";
import {
  CONTROL_PLANE_FAILURE_RECIPIENT, sendControlPlaneFailureEmail
} from "./email-notifier.js";
import { WORKLOAD_THREADS_BY_SKU, validateComputeTransition, workloadThreads } from "./sku-config.js";
import { buildSummary, phaseSummary } from "./summarize.js";
import { validate } from "./validate-run-document.js";
import {
  WorkloadValidationError, createOneKibDocument, loadConfig, runOperation
} from "./workload.js";

const PROJECT = "0123456789abcdef01234567";

test("campaign defaults and SKU mappings are preserved", () => {
  assert.equal(DEFAULT_ITERATIONS, 200);
  assert.deepEqual(WORKLOAD_THREADS_BY_SKU, {
    M10: 1, M20: 1, M30: 2, M40: 4, M50: 8, M60: 16, M80: 32
  });
  assert.deepEqual(higherSkus("M10"), ["M20"]);
  assert.deepEqual(higherSkus("M20"), ["M30"]);
  assert.equal(workloadThreads("M80"), 32);
  assert.equal(new Set(REGIONS).size, REGIONS.length);
  assert.deepEqual(INITIAL_SKUS, ["M10", "M20"]);
  assert.deepEqual(REGIONS, ["eastus2", "westus2", "eastus", "northcentralus"]);
});

test("every randomized configuration uses the four regions and exactly one tier step from M10 or M20", (t) => {
  for (const [initialIndex, initial] of INITIAL_SKUS.entries()) {
    for (const [regionIndex, region] of REGIONS.entries()) {
      const target = higherSkus(initial)[0];
      const samples = [(initialIndex + 0.5) / INITIAL_SKUS.length, (regionIndex + 0.5) / REGIONS.length, 0.99];
      const random = t.mock.method(Math, "random", () => samples.shift());
      assert.deepEqual(randomConfiguration(), [region, initial, target]);
      assert.equal(samples.length, 0);
      random.mock.restore();
    }
  }
  for (const initial of ["M30", "M40", "M50", "M60", "M80", "M200", "invalid"]) {
    assert.throws(() => higherSkus(initial), /Unsupported initial/);
    assert.throws(() => validateComputeTransition(initial, "M50"), /Unsupported initial/);
  }
  for (const target of ["M10", "M20", "M60", "M80", "M200", "invalid"]) {
    assert.throws(() => validateComputeTransition("M20", target), /Unapproved SKU transition/);
  }
  for (const target of ["M10", "M30", "M40", "M50", "M60", "invalid"]) {
    assert.throws(() => validateComputeTransition("M10", target), /Unapproved SKU transition/);
  }
});

test("manifest generation and coordinator validation reject out-of-policy SKU transitions", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "campaign-sku-policy-"));
  try {
    const create = (initial, target) => manifest(
      "run-1", "abinav-resiliency-agent-rnd-abc12",
      new Date("2026-01-01T00:00:00Z"), "eastus2", initial, target, PROJECT
    );
    const path = resolve(root, "manifest.json");
    for (const [initial, target] of [["M50", "M60"], ["M40", "M60"], ["M20", "M200"], ["M30", "M40"], ["M10", "M30"], ["M20", "M40"]]) {
      assert.throws(() => create(initial, target), /Unsupported initial|Unapproved SKU transition/);
      const value = create("M20", "M30");
      Object.assign(value.experiment, { initialComputeTier: initial, targetComputeTier: target });
      value.approvals.executionApproved = true;
      value.approvals.cleanupPreapproved = true;
      await writeFile(path, JSON.stringify(value));
      await assert.rejects(validateManifest(path, { env: {} }), /Unsupported initial|Unapproved SKU transition/);
      await assert.rejects(orchestrate([path]), /Unsupported initial|Unapproved SKU transition/);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas uses exactly 32 GB and a replica set for both allowed initial tiers", async () => {
  assert.equal(storageSizeForInitialTier("M10"), 32);
  assert.equal(storageSizeForInitialTier("M20"), 32);
  assert.throws(() => storageSizeForInitialTier("M30"), /Unsupported initial/);
  assert.throws(() => storageSizeForInitialTier("M10", 128), /Unapproved storage size/);
  const root = await mkdtemp(resolve(tmpdir(), "atlas-manifest-"));
  try {
    const value = manifest(
      "run-1", "abinav-resiliency-agent-rnd-abc12",
      new Date("2026-01-01T00:00:00Z"), "eastus2", "M20", "M30", PROJECT
    );
    assert.equal(value.experiment.highAvailability, "AtlasReplicaSet");
    assert.equal(value.experiment.storageSizeGb, 32);
    assert.equal(value.experiment.workloadThreads, 1);
    value.approvals.executionApproved = true;
    value.approvals.cleanupPreapproved = true;
    const path = resolve(root, "manifest.json");
    await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
    await validateManifest(path, { env: {} });
    value.experiment.highAvailability = "Disabled";
    await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
    await assert.rejects(validateManifest(path, { env: {} }), /Invalid Atlas experiment/);
    value.experiment.highAvailability = "AtlasReplicaSet";
    value.experiment.storageSizeGb = 1024;
    await writeFile(path, `${JSON.stringify(value, null, 2)}\n`);
    await assert.rejects(validateManifest(path, { env: {} }), /Unapproved storage size/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("driver metadata uses the pinned MongoDB Node.js driver", () => {
  assert.deepEqual(driverMetadata(), { driver: "Node.js", driverVersion: "mongodb-7.6.0" });
});

test("control-plane failure email includes Atlas request and stack details", async () => {
  let sentMessage;
  const result = await sendControlPlaneFailureEmail({
    runId: "run-1",
    operation: "scaling",
    timestampUtc: "2026-01-01T00:00:00.000Z",
    errorCode: "OperationFailed",
    errorMessage: "Scale failed.",
    controlPlaneOperationId: "11111111-2222-4333-8444-555555555555",
    retryCount: 2,
    finalOutcome: "succeeded",
    stackTrace: "AtlasApiError: Scale failed.\n    at scale (orchestrate.js:1:1)"
  }, {
    env: {
      AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING: "endpoint=https://example.invalid/;accesskey=test",
      AZURE_COMMUNICATION_EMAIL_FROM_ADDRESS: "resiliency@example.com"
    },
    createClient: () => ({
      beginSend: async (message) => {
        sentMessage = message;
        return { pollUntilDone: async () => ({ status: "Succeeded", id: "email-1" }) };
      }
    })
  });
  assert.equal(result.status, "Succeeded");
  assert.equal(sentMessage.senderAddress, "resiliency@example.com");
  assert.equal(sentMessage.recipients.to[0].address, CONTROL_PLANE_FAILURE_RECIPIENT);
  assert.match(sentMessage.content.plainText, /Control-plane OperationId: 11111111-2222-4333-8444-555555555555/);
  assert.match(sentMessage.content.subject, /^\[Atlas resiliency\]/);
  assert.match(sentMessage.content.plainText, /Retry count: 2/);
  assert.match(sentMessage.content.plainText, /Final outcome: succeeded/);
  assert.match(sentMessage.content.plainText, /at scale \(orchestrate\.js:1:1\)/);
});

test("control-plane failure email requires environment configuration", async () => {
  await assert.rejects(
    sendControlPlaneFailureEmail({}, { env: {} }),
    /AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING is required/
  );
});

test("workload document is exactly one kibibyte", () => {
  assert.equal(BSON.serialize(createOneKibDocument("run-1", 42)).length, 1024);
});

test("summary tracks operation and time availability", () => {
  const operations = [
    { kind: "data_plane_operation", phase: "scale-up", started_utc: "2026-01-01T00:00:00.000Z", ended_utc: "2026-01-01T00:00:00.100Z", success: true },
    { kind: "data_plane_operation", phase: "scale-up", started_utc: "2026-01-01T00:00:01.000Z", ended_utc: "2026-01-01T00:00:02.000Z", success: false },
    { kind: "data_plane_operation", phase: "scale-up", started_utc: "2026-01-01T00:00:03.000Z", ended_utc: "2026-01-01T00:00:03.100Z", success: true }
  ];
  const metric = buildSummary(operations).availability_metric;
  assert.equal(metric.attempted, 3);
  assert.equal(metric.success_rate_percent, 66.667);
  assert.equal(metric.disrupted_seconds, 2.1);
  assert.equal(metric.disruptions.length, 1);
});

test("aggregate metrics sum configured threads and retain zero-volume minutes", () => {
  const operations = [
    { thread_id: 1, phase: "baseline", operation: "health", started_utc: "2026-01-01T00:00:10.000Z", ended_utc: "2026-01-01T00:00:10.100Z", success: true },
    { thread_id: 2, phase: "baseline", operation: "health", started_utc: "2026-01-01T00:00:20.000Z", ended_utc: "2026-01-01T00:00:20.100Z", success: false, error_type: "ConnectionFailure" },
    { thread_id: 1, phase: "post-scale", operation: "health", started_utc: "2026-01-01T00:02:10.000Z", ended_utc: "2026-01-01T00:02:10.100Z", success: true }
  ];
  const metrics = aggregateRequestMetrics(
    operations, 2, Date.parse("2026-01-01T00:01:00Z"), Date.parse("2026-01-01T00:02:00Z")
  );
  assert.equal(metrics.allOperations.attempted, 3);
  assert.deepEqual(metrics.requestVolumePerMinute, [
    { startTimestamp: "2026-01-01T00:00:00.000Z", requestCount: 2 },
    { startTimestamp: "2026-01-01T00:01:00.000Z", requestCount: 0 },
    { startTimestamp: "2026-01-01T00:02:00.000Z", requestCount: 1 }
  ]);
});

test("phase metrics handle campaign-scale operation volumes", () => {
  const events = Array.from({ length: 150_000 }, (_, index) => ({
    thread_id: 1,
    phase: "baseline",
    started_utc: new Date(index * 1000).toISOString(),
    ended_utc: new Date(index * 1000 + 100).toISOString(),
    success: true
  }));
  const metrics = phaseMetrics(events);
  assert.equal(metrics.attempted, 150_000);
  assert.equal(metrics.observedSeconds, 149_999.1);
  const aggregate = aggregateRequestMetrics(events, 1);
  assert.equal(aggregate.allOperations.attempted, 150_000);
  assert.equal(aggregate.requestVolumePerMinute.length, 2_500);
  const summary = phaseSummary(events);
  assert.equal(summary.attempted, 150_000);
  assert.equal(summary.observed_seconds, 149_999.1);
});

test("availability dips are scoped by thread", () => {
  const operations = [];
  for (let second = 0; second < 5; second += 1) {
    for (const threadId of [1, 2]) {
      const failed = threadId === 2 && second === 1;
      operations.push({
        thread_id: threadId, phase: "scale-up", operation: "health",
        started_utc: `2026-01-01T00:00:0${second}.000Z`,
        ended_utc: `2026-01-01T00:00:0${second}.100Z`,
        success: !failed, ...(failed ? { error_type: "ConnectionFailure" } : {})
      });
    }
  }
  const metrics = aggregateRequestMetrics(
    operations, 2, Date.parse("2026-01-01T00:00:00Z"), Date.parse("2026-01-01T00:00:04Z")
  );
  assert.deepEqual(metrics.perThread[0].availabilityDips, []);
  assert.equal(metrics.perThread[1].availabilityDips[0].failedOperations, 1);
  assert.deepEqual(metrics.perThread[1].availabilityDips[0].errorCategoryCounts, { ConnectionFailure: 1 });
});

test("dip error counts cover every failure category, exclude successes and retain the merge rule", () => {
  const start = Date.parse("2026-01-01T00:00:00Z");
  const event = (second, success, errorType, threadId = 1) => ({
    thread_id: threadId, phase: "scale-up", operation: "point_read", success,
    started_utc: new Date(start + second * 1000).toISOString(),
    ended_utc: new Date(start + second * 1000 + 100).toISOString(),
    ...(errorType === undefined ? {} : { error_type: errorType })
  });
  const expected = { NetworkTimeout: 10, NotPrimaryError: 7, ServerSelectionTimeoutError: 5, WorkloadValidationError: 3 };
  const events = Object.entries(expected).flatMap(([category, count]) =>
    Array.from({ length: count }, () => event(0, false, category)));
  events.push(event(0, true, "IgnoredSuccess"), ...[1, 2, 3].map((second) => event(second, true)));
  const [raw, merged] = bucketDips(events, start, start + 3100);
  for (const dip of [raw[0], merged[0]]) {
    assert.equal(dip.failedOperations, 25);
    assert.equal(dip.durationSeconds, 1);
    assert.deepEqual(dip.errorCategoryCounts, expected);
    assert.deepEqual(dip.errorCategories, Object.keys(expected).sort());
  }
  const gapEvents = [
    event(0, false, "NetworkTimeout"), event(1, true), event(2, false, "NotPrimaryError"),
    ...[3, 4, 5].map((second) => event(second, true))
  ];
  const [gapRaw, gapMerged] = bucketDips(gapEvents, start, start + 5100);
  assert.equal(gapRaw.length, 2);
  assert.equal(gapMerged.length, 1);
  assert.deepEqual(gapMerged[0].errorCategoryCounts, { NetworkTimeout: 1, NotPrimaryError: 1 });
  assert.equal(gapMerged[0].durationSeconds, 3);
  const unknown = bucketDips([event(0, false), event(0, false, "__proto__")], start, start + 500)[1][0];
  assert.deepEqual(unknown.errorCategoryCounts, JSON.parse('{"UnknownError":1,"__proto__":1}'));
  const empty = bucketDips([], start, start + 93)[1][0];
  assert.equal(empty.failedOperations, 0);
  assert.deepEqual(empty.errorCategories, []);
  assert.deepEqual(empty.errorCategoryCounts, {});
});

test("schema and invariants enforce error-category counts for root and per-thread dips", async () => {
  const schema = JSON.parse(await readFile(resolve("resiliency/run-document.schema.json"), "utf8"));
  const start = Date.parse("2026-01-01T00:00:00Z");
  const dip = bucketDips([{
    phase: "scale-up", operation: "point_read", success: false, error_type: "NetworkTimeout",
    started_utc: new Date(start).toISOString()
  }], start, start + 1000)[1][0];
  for (const perThread of [false, true]) {
    const document = plannedDocument("run-1", "cluster", new Date(start), "eastus2", "M10", "M20", PROJECT);
    const dips = perThread ? document.aggregateMetrics.perThread[0].availabilityDips : document.availabilityDips;
    dips.push(structuredClone(dip));
    assert.deepEqual(validate(document, schema), []);
    dips[0].errorCategoryCounts.NetworkTimeout = 2;
    assert.ok(validate(document, schema).some((error) => error.includes("does not sum to failedOperations")));
    dips[0].errorCategoryCounts = { NotPrimaryError: 1 };
    assert.ok(validate(document, schema).some((error) => error.includes("keys differ from errorCategories")));
    dips[0].errorCategoryCounts = { NetworkTimeout: 0 };
    assert.ok(validate(document, schema).some((error) => error.includes("must be >= 1")));
    delete dips[0].errorCategoryCounts;
    assert.ok(validate(document, schema).some((error) => error.includes("errorCategoryCounts")));
  }
});

test("operation failures are recorded without escaping the loop", async () => {
  const events = [];
  const writer = { write: async (event) => events.push(event) };
  const result = await runOperation({
    writer,
    config: { runId: "run-1", maxRetries: 0, phaseFile: null },
    threadId: 2, operation: "insert", logicalId: "run-1:0",
    action: async () => { throw new WorkloadValidationError("expected"); }
  });
  assert.equal(result, false);
  const terminal = events.filter((event) => event.kind === "data_plane_operation");
  assert.equal(terminal.length, 1);
  assert.equal(terminal[0].error_type, "WorkloadValidationError");
});

test("planned Atlas document passes the schema", async () => {
  const schema = JSON.parse(await readFile(resolve("resiliency/run-document.schema.json"), "utf8"));
  const document = plannedDocument(
    "run-1", "cluster", new Date("2026-01-01T00:00:00Z"),
    "eastus2", "M20", "M30", PROJECT
  );
  assert.equal("exceptions" in document, false);
  assert.equal("exceptions" in schema.properties, false);
  assert.deepEqual(document.controlPlaneFailures, []);
  assert.deepEqual(validate(document, schema), []);
  document.controlPlaneFailures.push({
    operation: "provisioning",
    timestampUtc: "2026-01-01T00:00:00.000Z",
    errorCode: "RegionNotAvailable",
    errorMessage: "The region has no capacity.",
    controlPlaneOperationId: "a7a568c1-606e-4001-82da-1101c37e85db",
    retryCount: 1,
    finalOutcome: "succeeded",
    stackTrace: "AtlasApiError: provisioning failed"
  });
  assert.deepEqual(validate(document, schema), []);
});

test("finalized documents aggregate failures without persisting exceptions", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-finalize-"));
  try {
    const operations = [{
      kind: "data_plane_operation", run_id: "run-1", thread_id: 1,
      phase: "scale-up", operation: "insert",
      started_utc: "2026-01-01T00:01:01.000Z",
      ended_utc: "2026-01-01T00:01:02.000Z",
      latency_ms: 1000, success: false, error_type: "ConnectionFailure"
    }];
    const control = [
      { event: "run_start", timestamp_utc: "2026-01-01T00:00:00.000Z" },
      { event: "provision_attempt_start", timestamp_utc: "2026-01-01T00:00:01.000Z" },
      {
        event: "operation_terminal_error", operation: "provision",
        error_code: "RegionNotAvailable", error_detail: "The region has no capacity.",
        control_plane_operation_id: "11111111-2222-4333-8444-555555555555",
        retry_count: 1, final_outcome: "succeeded",
        stack_trace: "AtlasApiError: provisioning failed\n    at provision (orchestrate.js:1:1)",
        timestamp_utc: "2026-01-01T00:00:20.000Z"
      },
      {
        event: "state_transition", operation: "provision",
        provisioning_state: "Succeeded", cluster_status: "Ready",
        timestamp_utc: "2026-01-01T00:00:30.000Z"
      },
      {
        event: "provision_attempt_end", success: true,
        timestamp_utc: "2026-01-01T00:00:30.000Z"
      },
      { event: "scale_submitted", timestamp_utc: "2026-01-01T00:01:00.000Z" },
      {
        event: "scale_terminal", elapsed_seconds: 60,
        timestamp_utc: "2026-01-01T00:02:00.000Z"
      },
      { event: "run_complete", timestamp_utc: "2026-01-01T00:03:00.000Z" }
    ];
    await writeFile(
      resolve(root, "operations.jsonl"),
      `${operations.map((item) => JSON.stringify(item)).join("\n")}\n`
    );
    await writeFile(
      resolve(root, "control-plane.jsonl"),
      `${control.map((item) => JSON.stringify(item)).join("\n")}\n`
    );
    const runManifest = {
      runId: "run-1", provider: "atlas", atlas: { projectId: PROJECT },
      clusterName: "cluster", location: "eastus2",
      experiment: {
        initialComputeTier: "M10", targetComputeTier: "M20", storageSizeGb: 32,
        timeoutSeconds: 5, maxDataPlaneRetries: 0, workloadThreads: 1
      }
    };
    const { document } = await buildFinalDocument(root, runManifest, {
      endUtc: "2026-01-01T00:03:00.000Z"
    });
    assert.equal("exceptions" in document, false);
    assert.equal(document.aggregateMetrics.allOperations.failed, 1);
    assert.deepEqual(document.availabilityDips[0].errorCategories, ["ConnectionFailure"]);
    assert.deepEqual(document.controlPlaneFailures, [{
      operation: "provisioning", timestampUtc: "2026-01-01T00:00:20.000Z",
      errorCode: "RegionNotAvailable", errorMessage: "The region has no capacity.",
      controlPlaneOperationId: "11111111-2222-4333-8444-555555555555",
      retryCount: 1, finalOutcome: "succeeded",
      stackTrace: "AtlasApiError: provisioning failed\n    at provision (orchestrate.js:1:1)"
    }]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("Atlas cleanup failures are included in control-plane failures", () => {
  const failure = {
    operation: "cleanup", timestampUtc: "2026-01-01T00:04:00.000Z",
    errorCode: "AuthorizationFailed", errorMessage: "Deletion was denied.",
    controlPlaneOperationId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
    retryCount: 0, finalOutcome: "failed",
    stackTrace: "AtlasApiError: cleanup failed\n    at cleanup (cleanup-run.js:1:1)"
  };
  assert.deepEqual(controlPlaneFailures([], { failure }), [failure]);
});

test("Atlas provisioning retries consolidate into one failure", () => {
  const events = [
    {
      event: "operation_terminal_error", operation: "provision",
      timestamp_utc: "2026-01-01T00:00:10.000Z",
      error_code: "ATLAS_REQUEST_FAILED", error_detail: "First attempt failed.",
      control_plane_operation_id: "first-request", stack_trace: "first stack"
    },
    {
      event: "operation_terminal_error", operation: "provisioning",
      timestamp_utc: "2026-01-01T00:00:20.000Z",
      error_code: "RegionNotAvailable", error_detail: "Second attempt failed.",
      control_plane_operation_id: "second-request", stack_trace: "second stack"
    },
    {
      event: "provision_attempt_end", success: true,
      timestamp_utc: "2026-01-01T00:00:30.000Z"
    }
  ];
  assert.deepEqual(controlPlaneFailures(events), [{
    operation: "provisioning",
    timestampUtc: "2026-01-01T00:00:20.000Z",
    errorCode: "RegionNotAvailable",
    errorMessage: "Second attempt failed.",
    controlPlaneOperationId: "second-request",
    retryCount: 1,
    finalOutcome: "succeeded",
    stackTrace: "second stack"
  }]);
});

test("campaign preparation materializes Atlas identity and Node.js metadata", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-node-"));
  try {
    await mkdir(resolve(root, "resiliency"), { recursive: true });
    const schema = await readFile(resolve("resiliency/run-document.schema.json"));
    await import("node:fs/promises").then(({ writeFile }) =>
      writeFile(resolve(root, "resiliency/run-document.schema.json"), schema));
    const planPath = await prepare(root, 1, () => ["eastus2", "M20", "M30"], { atlasProjectId: PROJECT });
    const plan = JSON.parse(await readFile(planPath, "utf8"));
    const manifestPath = resolve(root, plan.manifests[0]);
    const document = JSON.parse(await readFile(resolve(manifestPath, "../planned-run-document.json"), "utf8"));
    assert.equal(document.driver, "Node.js");
    assert.equal(document.driverVersion, "mongodb-7.6.0");
    assert.equal(document.atlasProjectId, PROJECT);
    assert.equal(plan.provider, "atlas");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("fixed profile preparation uses the approved low-cost configuration", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-fixed-profile-"));
  try {
    await mkdir(resolve(root, "resiliency"), { recursive: true });
    const schema = await readFile(resolve("resiliency/run-document.schema.json"));
    await import("node:fs/promises").then(({ writeFile }) =>
      writeFile(resolve(root, "resiliency/run-document.schema.json"), schema));
    const planPath = await prepare(
      root, 1, () => ["eastus2", "M10", "M20"],
      { clusterLabel: "eus2", storageSizeGb: 32, atlasProjectId: PROJECT }
    );
    const plan = JSON.parse(await readFile(planPath, "utf8"));
    const manifestPath = resolve(root, plan.manifests[0]);
    const runManifest = JSON.parse(await readFile(manifestPath, "utf8"));
    const document = JSON.parse(
      await readFile(resolve(manifestPath, "../planned-run-document.json"), "utf8")
    );
    assert.match(runManifest.clusterName, /^abinav-resiliency-agent-eus2-[a-z0-9]{5}$/);
    assert.equal(runManifest.location, "eastus2");
    assert.deepEqual(runManifest.experiment, {
      initialComputeTier: "M10", targetComputeTier: "M20", storageSizeGb: 32,
      shardCount: 1, highAvailability: "AtlasReplicaSet",
      baselineSeconds: 120, postScaleSeconds: 120,
      requestPacing: "unthrottled-1-threaded",
      workloadThreads: 1, operationsPerSecondCeiling: 0,
      sampleIntervalSeconds: 1, timeoutSeconds: 5, maxDataPlaneRetries: 0,
      maxProvisioningRetries: 3, cleanupPolicy: "always-delete"
    });
    assert.equal(document.storageSizeGb, 32);
    runManifest.approvals.executionApproved = true;
    runManifest.approvals.cleanupPreapproved = true;
    await import("node:fs/promises").then(({ writeFile }) =>
      writeFile(manifestPath, `${JSON.stringify(runManifest, null, 2)}\n`));
    await validateManifest(manifestPath, { env: {} });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("resume selects only current Atlas plans and rejects archived manifests before execution or cleanup", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-archives-"));
  try {
    const current = resolve(root, "current.json");
    await writeFile(current, JSON.stringify({ provider: "atlas", schemaVersion: 3 }));
    await writeFile(resolve(root, "archived.json"), JSON.stringify({ schemaVersion: 2, approved: true }));
    await writeFile(resolve(root, "old-atlas.json"), JSON.stringify({ provider: "atlas", schemaVersion: 2 }));
    assert.equal(await latestAtlasPlan(root), current);
    const archived = resolve(root, "archived-manifest.json");
    const value = manifest(
      "run-1", "abinav-resiliency-agent-rnd-abc12", new Date("2026-01-01T00:00:00Z"),
      "eastus2", "M10", "M20", PROJECT
    );
    value.schemaVersion = 2;
    await writeFile(archived, JSON.stringify(value));
    await assert.rejects(orchestrate([archived]), /Invalid Atlas/);
    await assert.rejects(cleanup([archived, "--evidence", resolve(root, "cleanup.json")]), /Invalid Atlas/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("campaign CLI defaults to Atlas and can prepare without approval or cloud credentials", async () => {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-cli-"));
  const previous = process.env.ATLAS_PROJECT_ID;
  try {
    process.env.ATLAS_PROJECT_ID = PROJECT;
    await mkdir(resolve(root, "resiliency"));
    await writeFile(resolve(root, "resiliency", "run-document.schema.json"),
      await readFile(resolve("resiliency", "run-document.schema.json")));
    assert.equal(await campaign(["--iterations", "1", "--fixed-profile", "--prepare-only"], { root }), 0);
    const path = await latestAtlasPlan(resolve(root, "resiliency", "campaigns"));
    const plan = JSON.parse(await readFile(path, "utf8"));
    assert.equal(plan.provider, "atlas");
    assert.equal(plan.approved, false);
    assert.equal(plan.schemaVersion, 3);
    await assert.rejects(campaign(["--provider", "unsupported", "--prepare-only"], { root }), /--provider must be atlas/);
  } finally {
    if (previous === undefined) delete process.env.ATLAS_PROJECT_ID;
    else process.env.ATLAS_PROJECT_ID = previous;
    await rm(root, { recursive: true, force: true });
  }
});

test("campaign CLI prepares exactly 200 runs by default and with an explicit count", async () => {
  const previous = process.env.ATLAS_PROJECT_ID;
  try {
    process.env.ATLAS_PROJECT_ID = PROJECT;
    for (const args of [[], ["--iterations", "200"]]) {
      const root = await mkdtemp(resolve(tmpdir(), "atlas-200-cli-"));
      try {
        await mkdir(resolve(root, "resiliency"));
        await writeFile(resolve(root, "resiliency", "run-document.schema.json"),
          await readFile(resolve("resiliency", "run-document.schema.json")));
        assert.equal(await campaign([...args, "--prepare-only"], { root }), 0);
        const path = await latestAtlasPlan(resolve(root, "resiliency", "campaigns"));
        const plan = JSON.parse(await readFile(path, "utf8"));
        assert.equal(plan.iterations, 200);
        assert.equal(plan.manifests.length, 200);
        assert.equal(new Set(plan.manifests).size, 200);
        assert.equal(plan.approved, false);
        assert.equal(plan.cleanupPolicy, "always-delete");
        const manifests = await Promise.all(plan.manifests.map(async (relative) =>
          JSON.parse(await readFile(resolve(root, relative), "utf8"))));
        assert.equal(new Set(manifests.map((value) => value.clusterName)).size, 200);
        for (const value of manifests) {
          assert.equal(value.experiment.cleanupPolicy, "always-delete");
          assert.equal(value.experiment.storageSizeGb, 32);
          assert.ok(REGIONS.includes(value.location));
          assert.ok(INITIAL_SKUS.includes(value.experiment.initialComputeTier));
          assert.equal(value.approvals.executionApproved, false);
          validateComputeTransition(value.experiment.initialComputeTier, value.experiment.targetComputeTier);
        }
      } finally {
        await rm(root, { recursive: true, force: true });
      }
    }
  } finally {
    if (previous === undefined) delete process.env.ATLAS_PROJECT_ID;
    else process.env.ATLAS_PROJECT_ID = previous;
  }
});

test("campaign CLI rejects counts outside 1 through 200 and preserves the single-run fixed profile", async () => {
  for (const count of ["0", "-1", "201", "1.5"]) {
    await assert.rejects(campaign(["--iterations", count, "--prepare-only"]),
      /--iterations must be an integer between 1 and 200/);
  }
  await assert.rejects(campaign(["--iterations", "200", "--fixed-profile", "--prepare-only"]),
    /--fixed-profile requires --iterations 1/);
});

test("workload uses Atlas credentials without inheriting management, result or email secrets", () => {
  const connection = "mongodb+srv://user:password@test.example.mongodb.net/";
  const env = workloadEnvironment({
    ATLAS_CLIENT_ID: "client", ATLAS_CLIENT_SECRET: "secret", ATLAS_RESULTS_URI: "results-secret",
    AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING: "email-secret", PATH: "keep-path"
  }, connection, "run-1", "phase.txt", "stop");
  assert.equal(env.ATLAS_CLIENT_ID, undefined);
  assert.equal(env.ATLAS_CLIENT_SECRET, undefined);
  assert.equal(env.ATLAS_RESULTS_URI, undefined);
  assert.equal(env.AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING, undefined);
  assert.equal(env.PATH, "keep-path");
  const args = {
    durationSeconds: 60, operationsPerSecond: 0, maxRetries: 0, timeoutMs: 5000,
    threads: 1, database: "resiliencyTestDB", collection: "resiliencyCollection"
  };
  assert.equal(loadConfig(args, env).connectionString, connection);
  assert.throws(() => loadConfig(args, { RESILIENCY_RUN_ID: "run-1" }), /ATLAS_CONNECTION_STRING is required/);
});

test("result sink connects to resiliencyResultsDB.atlasRuns", async (t) => {
  const selected = [];
  const collection = {};
  t.mock.method(MongoClient.prototype, "db", (name) => {
    selected.push(name);
    if (name === "admin") return { command: async (command) => assert.deepEqual(command, { ping: 1 }) };
    assert.equal(name, "resiliencyResultsDB");
    return {
      collection: (collectionName) => {
        assert.equal(collectionName, "atlasRuns");
        return collection;
      }
    };
  });
  const uriEnv = "ATLAS_TEST_RESULTS_URI";
  const previous = process.env[uriEnv];
  process.env[uriEnv] = "mongodb://localhost:27017";
  let sink;
  try {
    sink = await ResultSink.connect({ uriEnv });
    assert.equal(sink.collection, collection);
    assert.deepEqual(selected, ["admin", "resiliencyResultsDB"]);
  } finally {
    await sink?.close();
    if (previous === undefined) delete process.env[uriEnv];
    else process.env[uriEnv] = previous;
  }
});

test("Atlas results retain exact persistence, readback and project identity checks", async () => {
  let stored = null;
  const sink = new ResultSink({
    insertOne: async (document) => {
      if (stored) throw new MongoServerError({ code: 11000, message: "duplicate" });
      stored = structuredClone(document);
    },
    findOne: async () => structuredClone(stored),
    replaceOne: async (_, document) => {
      if (!stored) return { matchedCount: 0 };
      stored = structuredClone(document);
      return { matchedCount: 1 };
    }
  });
  const document = plannedDocument("run-1", "cluster", new Date(), "eastus2", "M10", "M20", PROJECT);
  await sink.ensureInitial(document);
  await sink.ensureInitial(document);
  assert.deepEqual(await sink.ensureResumeBase(document), document);
  await assert.rejects(sink.ensureResumeBase({ ...document, atlasProjectId: "ffffffffffffffffffffffff" }), /conflicting run identity/);
  const final = { ...document, reportMarkdown: "Final Atlas report" };
  await sink.replaceFinalAndVerify(final, { retrySeconds: 0 });
  await sink.verify(final);
  await assert.rejects(sink.verify(document), /readback differs/);
});

test("scaling request failures remain in Atlas aggregate results", () => {
  const failures = controlPlaneFailures([{
    event: "operation_terminal_error", operation: "scaling",
    timestamp_utc: "2026-01-01T00:00:00.000Z", error_code: "ATLAS_REQUEST_FAILED",
    error_detail: "Scale failed", control_plane_operation_id: "atlas-request-id",
    retry_count: 0, final_outcome: "failed", stack_trace: "AtlasApiError: Scale failed"
  }]);
  assert.equal(failures.length, 1);
  assert.equal(failures[0].operation, "scaling");
  assert.equal(failures[0].controlPlaneOperationId, "atlas-request-id");
});

test("experience report retains developer-facing conclusions", () => {
  const metrics = {
    allOperations: { attempted: 1000, failed: 20 },
    scalingInProgressPhase: { disruptedSeconds: 35 },
    perThread: [{ allOperations: { failed: 20 } }, { allOperations: { failed: 0 } }]
  };
  const report = buildExperienceReport({
    manifest: { runId: "run-1", experiment: { initialComputeTier: "M30", targetComputeTier: "M40" } },
    aggregateMetrics: metrics, provisioningSeconds: 700, scaleSeconds: 1200,
    availabilityDips: [{ durationSeconds: 35 }], cleanupComplete: false,
    inconclusiveErrorCategory: "Error"
  });
  assert.match(report, /longer waiting to start/);
  assert.match(report, /more disruption than I expected/);
  assert.match(report, /INCONCLUSIVE/);
});
