import { mkdir, readdir, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline/promises";
import { driverMetadata } from "./driver-info.js";
import { INITIAL_SKUS, higherSkus, validateComputeTransition, workloadThreads } from "./sku-config.js";
import { isMain, parseArgs, randomSuffix, readJson, readSecret, runNode, utcNow, writeJson } from "./common.js";
import { validate } from "./validate-run-document.js";
import { ATLAS_AGENT, ATLAS_REGIONS, atlasResourceIds, requireAtlasProjectId, validateAtlasManifest } from "./atlas.js";

export { INITIAL_SKUS, SKU_TIERS, higherSkus } from "./sku-config.js";
export const DEFAULT_ITERATIONS = 200;
export const REGIONS = Object.freeze(Object.keys(ATLAS_REGIONS));

export function storageSizeForInitialTier(initialTier, requested = 1024) {
  return ["M10", "M20"].includes(initialTier) ? 128 : requested;
}

function choose(values) {
  return values[Math.floor(Math.random() * values.length)];
}

export function randomConfiguration() {
  const initialTier = choose(INITIAL_SKUS);
  return [choose(REGIONS), initialTier, choose(higherSkus(initialTier))];
}

function compactUtc(date) {
  return date.toISOString().replaceAll("-", "").replaceAll(":", "").replace(/\.\d{3}Z$/, "Z");
}

export function plannedDocument(
  runId, cluster, created, region, initialTier, targetTier, projectId = process.env.ATLAS_PROJECT_ID,
  storageSizeGb = 1024
) {
  requireAtlasProjectId(projectId);
  validateComputeTransition(initialTier, targetTier);
  const emptyMetrics = () => ({
    attempted: 0, succeeded: 0, failed: 0, availabilityPercent: null, observedSeconds: 0
  });
  const perThread = Array.from({ length: workloadThreads(initialTier) }, (_, index) => ({
    threadId: index + 1, allOperations: emptyMetrics(), preScalingPhase: emptyMetrics(),
    scalingInProgressPhase: emptyMetrics(), postScalePhase: emptyMetrics(), availabilityDips: []
  }));
  return {
    _id: runId, date: created.toISOString().slice(0, 10),
    provider: "atlas", product: "MongoDB Atlas", agentName: ATLAS_AGENT,
    atlasProjectId: projectId, atlasTopology: "3-node replica set",
    ...driverMetadata(),
    clientObservedAvailabilityPerc: null, totalRuntimeSeconds: null,
    provisioningTimeSeconds: null, scaleUpTimeSeconds: null, cleanupTimeSeconds: null,
    startTimeUTC: null, endTimeUTC: null, clusterName: cluster, region,
    initialComputeTier: initialTier, targetComputeTier: targetTier,
    storageSizeGb: storageSizeForInitialTier(initialTier, storageSizeGb),
    parameters: { connectionTimeoutSeconds: 5, operationTimeoutSeconds: 5, maxDataPlaneRetries: 0 },
    controlPlaneTimeline: {
      provisioningSubmittedUtc: null, provisioningCompletedUtc: null, scaleSubmittedUtc: null,
      scaleCompleted: null, scaleResult: null, cleanupSubmittedUtc: null, cleanupCompletedUtc: null
    },
    controlPlaneFailures: [],
    aggregateMetrics: {
      allOperations: emptyMetrics(), preScalingPhase: null, scalingInProgressPhase: null,
      postScalePhase: null, perThread, requestVolumePerMinute: []
    },
    availabilityDips: [],
    reportMarkdown: "# MongoDB Atlas resiliency run\n\nApproved campaign run has not started."
  };
}

export function manifest(
  runId, cluster, created, region, initialTier, targetTier, projectId = process.env.ATLAS_PROJECT_ID,
  storageSizeGb = 1024
) {
  requireAtlasProjectId(projectId);
  validateComputeTransition(initialTier, targetTier);
  if (!ATLAS_REGIONS[region]) throw new Error(`Unsupported Atlas Azure region: ${region}`);
  const expiresAt = new Date(created.getTime() + 24 * 3_600_000).toISOString();
  const threadCount = workloadThreads(initialTier);
  const value = {
    schemaVersion: 3, provider: "atlas", runId, status: "planned-awaiting-approval",
    clusterName: cluster, location: region, apiVersion: "2024-08-05", expiresAt,
    atlas: {
      projectId, providerName: "AZURE", regionName: ATLAS_REGIONS[region],
      username: `resiliency-${runId}`, replicaSetNodeCount: 3
    },
    tags: { purpose: "atlas-resiliency", runId, createdBy: "copilot", expiresAt },
    experiment: {
      initialComputeTier: initialTier, targetComputeTier: targetTier,
      storageSizeGb: storageSizeForInitialTier(initialTier, storageSizeGb),
      shardCount: 1, highAvailability: "AtlasReplicaSet",
      baselineSeconds: 120, postScaleSeconds: 120,
      requestPacing: `unthrottled-${threadCount}-threaded`, workloadThreads: threadCount,
      operationsPerSecondCeiling: 0, sampleIntervalSeconds: 1, timeoutSeconds: 5,
      maxDataPlaneRetries: 0, maxProvisioningRetries: 3,
      cleanupPolicy: "always-delete"
    },
    plannedResourceIds: [], ownedResourceIds: [],
    securityLimitations: [
      "Atlas dedicated clusters use a three-node replica set.",
      "The existing Atlas project IP access list must allow the workload host; the campaign does not modify it.",
      "Atlas must accept the exact planned Azure region, tier and disk size; unsupported configurations fail without substitution."
    ],
    approvals: { executionApproved: false, cleanupPreapproved: false, approvedAtlasProjectId: projectId }
  };
  value.plannedResourceIds = atlasResourceIds(value);
  return value;
}

export async function prepare(
  root, iterations, configuration = randomConfiguration,
  { clusterLabel = "rnd", storageSizeGb = 1024, atlasProjectId = process.env.ATLAS_PROJECT_ID } = {}
) {
  requireAtlasProjectId(atlasProjectId);
  const created = new Date();
  const campaignId = `${compactUtc(created)}-${randomSuffix()}`;
  const runPaths = [];
  const used = new Set();
  const schema = await readJson(resolve(root, "resiliency", "run-document.schema.json"));
  for (let index = 0; index < iterations; index += 1) {
    let suffix;
    do suffix = randomSuffix(); while (used.has(suffix));
    used.add(suffix);
    const runTime = new Date(created.getTime() + index * 1000);
    const runId = `${compactUtc(runTime)}-${suffix}`;
    const [region, initialTier, targetTier] = configuration();
    const cluster = `abinav-resiliency-agent-${clusterLabel}-${suffix}`;
    const runManifest = manifest(runId, cluster, runTime, region, initialTier, targetTier, atlasProjectId, storageSizeGb);
    const document = plannedDocument(runId, cluster, runTime, region, initialTier, targetTier, atlasProjectId, storageSizeGb);
    const errors = validate(document, schema);
    if (errors.length) throw new Error(`Generated run document is invalid: ${errors.join("; ")}`);
    const runDirectory = resolve(root, "resiliency", "runs", runId);
    try {
      await stat(runDirectory);
      throw new Error(`Generated run directory already exists: ${runId}`);
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
    await mkdir(runDirectory, { recursive: true });
    await writeJson(resolve(runDirectory, "manifest.json"), runManifest);
    await writeJson(resolve(runDirectory, "planned-run-document.json"), document);
    runPaths.push(`resiliency/runs/${runId}/manifest.json`);
  }
  const planPath = resolve(root, "resiliency", "campaigns", `${campaignId}.json`);
  await mkdir(dirname(planPath), { recursive: true });
  await writeJson(planPath, {
    schemaVersion: 3, campaignId, provider: "atlas", atlasProjectId,
    cleanupPolicy: "always-delete",
    createdUtc: created.toISOString(), iterations, approved: false, manifests: runPaths
  });
  return planPath;
}

export async function approve(root, planPath, { acceptBillableCharges = false, input } = {}) {
  const plan = await readJson(planPath);
  if (plan.provider !== "atlas" || plan.schemaVersion !== 3) throw new Error("Only current Atlas campaign plans can be approved");
  console.log(`Campaign ${plan.campaignId}: ${plan.iterations} strictly sequential runs`);
  console.log(`Atlas project: ${plan.atlasProjectId} | Cloud: AZURE | Topology: 3-node replica set`);
  console.log("Existing project IP access list is unchanged; exact region, tier and disk size required.");
  console.log("Run-owned clusters and scoped database users are always cleaned up, including after failures.");
  for (const [index, relative] of plan.manifests.entries()) {
    const value = await readJson(resolve(root, relative));
    validateAtlasManifest(value, { approved: false });
    validateComputeTransition(value.experiment.initialComputeTier, value.experiment.targetComputeTier);
    if (value.atlas.projectId !== plan.atlasProjectId) throw new Error("Campaign plan and manifest Atlas projects differ");
    const experiment = value.experiment;
    console.log(`${index + 1}. ${value.runId} | ${value.clusterName} | ${value.location} | ${experiment.initialComputeTier} -> ${experiment.targetComputeTier} | ${experiment.storageSizeGb} GB | ${experiment.highAvailability}`);
  }
  const expected = `APPROVE ${plan.campaignId}`;
  if (!acceptBillableCharges) {
    let answer;
    if (input) answer = await input(expected);
    else {
      const readline = createInterface({ input: process.stdin, output: process.stdout });
      try { answer = await readline.question(`Type '${expected}' to authorize billable execution and run-scoped cleanup, including on failure: `); }
      finally { readline.close(); }
    }
    if (answer.trim() !== expected) throw new Error("Campaign approval was not granted");
  }
  for (const relative of plan.manifests) {
    const path = resolve(root, relative);
    const value = await readJson(path);
    value.status = "approved-ready";
    value.approvals.executionApproved = true;
    value.approvals.cleanupPreapproved = true;
    await writeJson(path, value);
  }
  Object.assign(plan, {
    approved: true, approvedUtc: utcNow(),
    approvalMethod: acceptBillableCharges ? "accept-billable-charges-flag" : "interactive-token"
  });
  await writeJson(planPath, plan);
  return plan;
}

export async function latestAtlasPlan(campaigns) {
  const files = (await readdir(campaigns)).filter((name) => name.endsWith(".json"));
  const candidates = await Promise.all(files.map(async (name) => {
    const path = resolve(campaigns, name);
    const plan = await readJson(path);
    return { path, modified: (await stat(path)).mtimeMs, current: plan.provider === "atlas" && plan.schemaVersion === 3 };
  }));
  const matching = candidates.filter((item) => item.current);
  if (!matching.length) throw new Error("No current Atlas campaign plan exists to resume");
  return matching.sort((a, b) => a.modified - b.modified).at(-1).path;
}

export async function main(
  argv = process.argv.slice(2),
  { root = resolve(dirname(fileURLToPath(import.meta.url)), "..") } = {}
) {
  const args = parseArgs(argv, {
    iterations: { type: "number", default: DEFAULT_ITERATIONS },
    "resume-plan": { type: "string" }, "resume-latest": { type: "boolean" },
    "fixed-profile": { type: "boolean" }, "prompt-results-uri": { type: "boolean" },
    "accept-billable-charges": { type: "boolean" },
    provider: { type: "string", default: "atlas" },
    "prompt-atlas-credentials": { type: "boolean" }, "prepare-only": { type: "boolean" }
  });
  if (args.provider !== "atlas") throw new Error("--provider must be atlas");
  if (!Number.isInteger(args.iterations) || args.iterations < 1 || args.iterations > 200) {
    throw new Error("--iterations must be an integer between 1 and 200");
  }
  if (args["fixed-profile"] && args.iterations !== 1) throw new Error("--fixed-profile requires --iterations 1");
  let planPath;
  if (args["resume-plan"]) planPath = resolve(args["resume-plan"]);
  else if (args["resume-latest"]) planPath = await latestAtlasPlan(resolve(root, "resiliency", "campaigns"));
  else if (args["fixed-profile"]) {
    planPath = await prepare(root, 1, () => ["eastus2", "M10", "M20"], {
      clusterLabel: "eus2", storageSizeGb: 128
    });
  } else planPath = await prepare(root, args.iterations, randomConfiguration);
  let plan = await readJson(planPath);
  if (plan.provider !== "atlas" || plan.schemaVersion !== 3) throw new Error("Only current Atlas campaign plans can be executed");
  if (args["prepare-only"]) {
    console.log(`Prepared campaign without approval or cloud changes: ${planPath}`);
    return 0;
  }
  if (args["prompt-atlas-credentials"]) {
    for (const [name, prompt] of [
      ["ATLAS_CLIENT_ID", "Atlas service account client ID: "],
      ["ATLAS_CLIENT_SECRET", "Atlas service account client secret: "]
    ]) {
      if (!process.env[name]) {
        process.env[name] = (await readSecret(prompt)).trim();
        if (!process.env[name]) throw new Error(`${name} cannot be empty`);
      }
    }
  }
  if (!plan.approved) plan = await approve(root, planPath, { acceptBillableCharges: args["accept-billable-charges"] });
  const commandArgs = plan.manifests.map((relative) => resolve(root, relative));
  if (args["prompt-results-uri"]) commandArgs.push("--prompt-results-uri");
  await runNode(resolve(root, "resiliency", "campaign-coordinator.js"), commandArgs, { cwd: root });
  return 0;
}

if (isMain(import.meta.url)) process.exitCode = await main();
