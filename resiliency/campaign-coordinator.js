import { mkdir, open, readdir, stat, unlink } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { REGIONS, storageSizeForInitialTier } from "./campaign.js";
import { validateComputeTransition } from "./sku-config.js";
import {
  atomicJson, errorName, isMain, parseArgs, readJson, round, runNode, utcNow
} from "./common.js";
import { driverMetadata } from "./driver-info.js";
import {
  aggregateRequestMetrics, bucketDips, buildExperienceReport, controlPlaneFailures,
  formatUtc, parseUtc, readJsonl
} from "./finalize-run.js";
import { ResultSink } from "./result-sink.js";
import { validate } from "./validate-run-document.js";
import { AtlasClient, validateAtlasManifest } from "./atlas.js";

const CLUSTER_PATTERN = /^abinav-resiliency-agent-[a-z0-9]+-[a-z0-9]{5}$/;
const ALLOWED_REGIONS = new Set(REGIONS);

export function processIsRunning(pid) {
  if (!Number.isInteger(pid) || pid <= 0) return false;
  try { process.kill(pid, 0); return true; } catch { return false; }
}

export class CampaignLock {
  constructor(path) {
    this.path = path;
    this.handle = null;
  }

  async acquire() {
    try {
      const owner = await readJson(this.path);
      if (processIsRunning(Number(owner.pid))) throw new Error(`Another campaign process owns ${this.path}`);
      await unlink(this.path);
    } catch (error) {
      if (error.code !== "ENOENT" && !error.message.startsWith("Another campaign")) {
        await unlink(this.path).catch(() => {});
      } else if (error.message.startsWith("Another campaign")) throw error;
    }
    try {
      this.handle = await open(this.path, "wx");
      await this.handle.writeFile(JSON.stringify({ pid: process.pid, startedUtc: utcNow() }));
    } catch (error) {
      if (error.code === "EEXIST") {
        throw new Error(`Another campaign owns ${this.path}; remove it only after confirming that process is gone`);
      }
      throw error;
    }
    return this;
  }

  async release() {
    await this.handle?.close();
    await unlink(this.path).catch((error) => {
      if (error.code !== "ENOENT") throw error;
    });
  }
}

export async function validateManifest(path, { env = process.env, cleanupOnly = false } = {}) {
  const manifest = await readJson(path);
  const runId = String(manifest.runId ?? "");
  const cluster = String(manifest.clusterName ?? "");
  if (!runId || !CLUSTER_PATTERN.test(cluster)) throw new Error(`Invalid run ID or cluster name in ${path}`);
  const region = String(manifest.location ?? "");
  const experiment = manifest.experiment ?? {};
  validateAtlasManifest(manifest, { env, cleanupOnly });
  if (cleanupOnly) return manifest;
  validateComputeTransition(experiment.initialComputeTier, experiment.targetComputeTier);
  if (!ALLOWED_REGIONS.has(region)) throw new Error(`Unapproved region for ${runId}: ${region}`);
  const fixedProfile = cluster.startsWith("abinav-resiliency-agent-eus2-");
  if (fixedProfile && (
    region !== "eastus2"
    || experiment.initialComputeTier !== "M10"
    || experiment.targetComputeTier !== "M20"
  )) {
    throw new Error(`Invalid fixed profile for ${runId}`);
  }
  const expectedStorageSizeGb = storageSizeForInitialTier(
    String(experiment.initialComputeTier ?? "")
  );
  if (experiment.storageSizeGb !== expectedStorageSizeGb) {
    throw new Error(`Unapproved storage size for ${runId}`);
  }
  return manifest;
}

async function cleanupRun(manifestPath, cleanupPath) {
  const manifest = await validateManifest(manifestPath, { cleanupOnly: true });
  return new AtlasClient().cleanup(manifest, manifestPath, cleanupPath);
}

export function validateDocument(document, schema) {
  const errors = validate(document, schema);
  if (errors.length) throw new Error(`Run-document validation failed: ${errors.join("; ")}`);
}

export async function validateCandidate(path, schemaPath) {
  const document = await ResultSink.load(path);
  validateDocument(document, await ResultSink.load(schemaPath));
  return document;
}

function lastEvent(control, name) {
  return [...control].reverse().find((item) => item.event === name) ?? null;
}

export async function buildInconclusiveCandidate({ planned, manifest, resultDirectory, cleanup, error }) {
  const document = structuredClone(planned);
  Object.assign(document, driverMetadata());
  const controlPath = resolve(resultDirectory, "control-plane.jsonl");
  const operationsPath = resolve(resultDirectory, "operations.jsonl");
  const control = await stat(controlPath).then(() => readJsonl(controlPath)).catch(() => []);
  const operationEvents = await stat(operationsPath).then(() => readJsonl(operationsPath)).catch(() => []);
  const operations = operationEvents.filter((item) => item.kind === "data_plane_operation");
  const runStart = lastEvent(control, "run_start");
  const runComplete = lastEvent(control, "run_complete") ?? lastEvent(control, "run_failed");
  const provisionStart = lastEvent(control, "provision_attempt_start");
  const provisionEnd = [...control].reverse().find((item) =>
    item.event === "state_transition" && item.operation === "provision" &&
    item.provisioning_state === "Succeeded" && item.cluster_status === "Ready") ?? null;
  const scaleStartEvent = lastEvent(control, "scale_submitted");
  const scaleEndEvent = lastEvent(control, "scale_terminal");
  const scaleStart = scaleStartEvent ? parseUtc(scaleStartEvent.timestamp_utc) : null;
  const scaleEnd = scaleEndEvent ? parseUtc(scaleEndEvent.timestamp_utc) : null;
  if (operations.length) {
    const aggregate = aggregateRequestMetrics(
      operations, Number(manifest.experiment.workloadThreads),
      scaleStart !== null && scaleEnd !== null ? scaleStart : null,
      scaleStart !== null && scaleEnd !== null ? scaleEnd : null
    );
    const scaling = aggregate.scalingInProgressPhase;
    const scaleObserved = Number.isFinite(scaleEndEvent?.elapsed_seconds)
      ? round(Number(scaleEndEvent.elapsed_seconds)) : Number(scaling.observedSeconds);
    const dips = scaleStart !== null && scaleEnd !== null
      ? bucketDips(operations, scaleStart, scaleEnd) : [[], [], 0];
    Object.assign(scaling, {
      observedSeconds: scaleObserved,
      availableSeconds: round(Math.max(0, scaleObserved - dips[2])),
      disruptedSeconds: dips[2],
      timeAvailabilityPercent: scaleObserved
        ? round(Math.max(0, scaleObserved - dips[2]) / scaleObserved * 100) : null,
      rawDisruptionWindows: dips[1].length,
      longestRawDisruptionSeconds: Math.max(0, ...dips[1].map((item) => item.durationSeconds))
    });
    aggregate.allOperations.observedSeconds = round([
      "preScalingPhase", "scalingInProgressPhase", "postScalePhase"
    ].reduce((sum, name) => sum + aggregate[name].observedSeconds, 0));
    document.aggregateMetrics = aggregate;
    document.availabilityDips = dips[1];
    document.clientObservedAvailabilityPerc = scaling.availabilityPercent;
  }
  const timeline = document.controlPlaneTimeline;
  if (provisionStart) timeline.provisioningSubmittedUtc = provisionStart.timestamp_utc;
  if (provisionEnd) timeline.provisioningCompletedUtc = provisionEnd.timestamp_utc;
  if (provisionStart && provisionEnd) {
    document.provisioningTimeSeconds = round((parseUtc(provisionEnd.timestamp_utc) - parseUtc(provisionStart.timestamp_utc)) / 1000);
  }
  if (scaleStartEvent) timeline.scaleSubmittedUtc = scaleStartEvent.timestamp_utc;
  if (scaleEndEvent) {
    timeline.scaleCompleted = scaleEndEvent.timestamp_utc;
    timeline.scaleResult = String(scaleEndEvent.provisioning_state ?? "Unknown");
    if (Number.isFinite(scaleEndEvent.elapsed_seconds)) document.scaleUpTimeSeconds = round(Number(scaleEndEvent.elapsed_seconds));
  }
  const cleanupStart = cleanup?.cleanupSubmittedUtc ?? null;
  const cleanupEnd = cleanup?.cleanupCompletedUtc ?? null;
  timeline.cleanupSubmittedUtc = cleanupStart;
  timeline.cleanupCompletedUtc = cleanupEnd;
  if (cleanupStart && cleanupEnd) document.cleanupTimeSeconds = round((parseUtc(cleanupEnd) - parseUtc(cleanupStart)) / 1000);
  const startValue = runStart?.timestamp_utc ?? null;
  const endValue = cleanupEnd ?? runComplete?.timestamp_utc ?? null;
  if (startValue) {
    document.startTimeUTC = formatUtc(parseUtc(startValue));
    document.date = document.startTimeUTC.slice(0, 10);
  }
  if (endValue) document.endTimeUTC = formatUtc(parseUtc(endValue));
  if (startValue && endValue) document.totalRuntimeSeconds = round((parseUtc(endValue) - parseUtc(startValue)) / 1000);
  document.controlPlaneFailures = controlPlaneFailures(control, cleanup);
  document.reportMarkdown = buildExperienceReport({
    manifest, aggregateMetrics: document.aggregateMetrics,
    provisioningSeconds: document.provisioningTimeSeconds,
    scaleSeconds: document.scaleUpTimeSeconds,
    availabilityDips: document.availabilityDips,
    cleanupComplete: Boolean(cleanup?.cleanupCompletedUtc), inconclusiveErrorCategory: errorName(error)
  });
  return document;
}

async function finalize(root, resultDirectory, manifestPath, cleanup = null) {
  const args = [resultDirectory, "--manifest", manifestPath];
  if (cleanup?.cleanupSubmittedUtc && cleanup?.cleanupCompletedUtc) {
    args.push(
      "--cleanup-submitted-utc", cleanup.cleanupSubmittedUtc,
      "--cleanup-completed-utc", cleanup.cleanupCompletedUtc,
      "--end-utc", cleanup.cleanupCompletedUtc
    );
  }
  await runNode(resolve(root, "resiliency/finalize-run.js"), args, { cwd: root });
  return resolve(resultDirectory, "final-run-document.json");
}

export async function runOne({
  manifestPath, sink, root, schemaPath, statePath, finalizeRun = finalize, cleanupResources = cleanupRun
}) {
  let manifest = await validateManifest(manifestPath);
  const runId = manifest.runId;
  const resultDirectory = resolve(root, "resiliency/results", runId);
  await mkdir(resultDirectory, { recursive: true });
  const plannedPath = resolve(dirname(manifestPath), "planned-run-document.json");
  if (["completed-cleanup-confirmed", "failed-cleanup-confirmed", "failed-no-owned-resources"].includes(manifest.status)) {
    await sink.verify(await validateCandidate(resolve(resultDirectory, "final-run-document.json"), schemaPath));
    return;
  }
  await atomicJson(statePath, { runId, stage: "persisting-initial", updatedUtc: utcNow() });
  const planned = await validateCandidate(plannedPath, schemaPath);
  if (["approved-ready", "approved-awaiting-execution"].includes(manifest.status)) await sink.ensureInitial(planned);
  else validateDocument(await sink.ensureResumeBase(planned), await ResultSink.load(schemaPath));
  if (["approved-ready", "approved-awaiting-execution"].includes(manifest.status)) {
    await atomicJson(statePath, { runId, stage: "orchestrating", updatedUtc: utcNow() });
    await runNode(resolve(root, "resiliency/orchestrate.js"), [manifestPath], { cwd: root });
    manifest = await validateManifest(manifestPath);
  }
  if (!["completed-awaiting-cleanup-confirmation",
    "completed-with-data-plane-errors-awaiting-cleanup-confirmation",
    "completed-resources-retained"].includes(manifest.status)) {
    throw new Error(`Run ${runId} cannot be finalized from status ${manifest.status}`);
  }
  await atomicJson(statePath, { runId, stage: "persisting-pre-cleanup", updatedUtc: utcNow() });
  let candidatePath = await finalizeRun(root, resultDirectory, manifestPath);
  await sink.replaceAndVerify(await validateCandidate(candidatePath, schemaPath));
  const cleanupPath = resolve(resultDirectory, "cleanup-evidence.json");
  const existingCleanup = await ResultSink.load(cleanupPath).catch((error) => {
    if (error.code !== "ENOENT") throw error;
    return null;
  });
  if (!existingCleanup?.cleanupCompletedUtc) {
    await atomicJson(statePath, { runId, stage: "cleaning-up", updatedUtc: utcNow() });
    await cleanupResources(manifestPath, cleanupPath);
  }
  const cleanup = await ResultSink.load(cleanupPath);
  await atomicJson(statePath, { runId, stage: "persisting-final", updatedUtc: utcNow() });
  candidatePath = await finalizeRun(root, resultDirectory, manifestPath, cleanup);
  await sink.replaceFinalAndVerify(await validateCandidate(candidatePath, schemaPath));
  manifest = await validateManifest(manifestPath);
  manifest.status = "completed-cleanup-confirmed";
  manifest.cleanup = cleanup;
  await atomicJson(manifestPath, manifest);
  for (const name of await readdir(resultDirectory)) {
    if (/^operations.*\.jsonl$/.test(name)) await unlink(resolve(resultDirectory, name));
  }
  await atomicJson(statePath, { runId, stage: "completed-cleanup-confirmed", updatedUtc: utcNow() });
}

export async function recoverFailedRun({
  manifestPath, sink, root, schemaPath, statePath, error, cleanupResources = cleanupRun
}) {
  const manifest = await validateManifest(manifestPath, { cleanupOnly: true });
  const runId = manifest.runId;
  const resultDirectory = resolve(root, "resiliency/results", runId);
  await mkdir(resultDirectory, { recursive: true });
  const cleanupPath = resolve(resultDirectory, "cleanup-evidence.json");
  let cleanupError = null;
  let cleanup = null;
  if (manifest.ownedResourceIds.length) {
    try {
      cleanup = await cleanupResources(manifestPath, cleanupPath);
    } catch (failure) {
      cleanupError = failure;
      cleanup = await ResultSink.load(cleanupPath).catch(() => null);
    }
  }
  const planned = await validateCandidate(resolve(dirname(manifestPath), "planned-run-document.json"), schemaPath);
  const failure = await buildInconclusiveCandidate({
    planned, manifest, resultDirectory, cleanup, error
  });
  const failurePath = resolve(resultDirectory, "final-run-document.json");
  await atomicJson(failurePath, failure);
  await sink.replaceFinalAndVerify(await validateCandidate(failurePath, schemaPath));
  manifest.status = cleanupError
    ? "failed-cleanup-pending"
    : cleanup ? "failed-cleanup-confirmed" : "failed-no-owned-resources";
  if (cleanup) manifest.cleanup = cleanup;
  await atomicJson(manifestPath, manifest);
  if (!cleanupError) {
    for (const name of await readdir(resultDirectory)) {
      if (/^operations.*\.jsonl$/.test(name)) await unlink(resolve(resultDirectory, name));
    }
  }
  await atomicJson(statePath, {
    runId, stage: manifest.status,
    errorCategory: errorName(cleanupError ?? error), updatedUtc: utcNow()
  });
  return !cleanupError;
}

export async function executeManifests({
  manifests, sink, root, schemaPath, statePath, finalizeRun = finalize, cleanupResources = cleanupRun
}) {
  const failures = [];
  const cleanupUnverified = [];
  for (const manifestPath of manifests) {
    try {
      await runOne({ manifestPath, sink, root, schemaPath, statePath, finalizeRun, cleanupResources });
    } catch (error) {
      const verified = await recoverFailedRun({
        manifestPath, sink, root, schemaPath, statePath, error, cleanupResources
      });
      const runId = (await validateManifest(manifestPath)).runId;
      failures.push(runId);
      if (!verified) cleanupUnverified.push(runId);
    }
  }
  return [failures, cleanupUnverified];
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv, {
    "results-uri-env": { type: "string", default: "ATLAS_RESULTS_URI" },
    "prompt-results-uri": { type: "boolean" }
  });
  if (!args._.length) throw new Error("At least one manifest is required");
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const manifests = args._.map((path) => resolve(path));
  const validated = await Promise.all(manifests.map((path) => validateManifest(path)));
  const runIds = validated.map((value) => value.runId);
  if (new Set(runIds).size !== runIds.length) throw new Error("Campaign contains duplicate run IDs");
  if (new Set(validated.map((value) => value.atlas.projectId)).size !== 1) throw new Error("Cannot mix Atlas projects in one campaign");
  await new AtlasClient().accessToken();
  const sink = await ResultSink.connect({
    uriEnv: args["results-uri-env"],
    promptIfMissing: args["prompt-results-uri"]
  });
  const here = resolve(root, "resiliency");
  const statePath = resolve(here, "atlas-campaign-state.json");
  const schemaPath = resolve(here, "run-document.schema.json");
  const lock = await new CampaignLock(resolve(here, "atlas-campaign.lock")).acquire();
  try {
    await atomicJson(statePath, { stage: "campaign-started", runIds, updatedUtc: utcNow() });
    const [failures, cleanupUnverified] = await executeManifests({
      manifests, sink, root, schemaPath, statePath
    });
    await atomicJson(statePath, {
      stage: failures.length ? "campaign-completed-with-failures" : "campaign-completed",
      runIds, failedRunIds: failures, cleanupUnverifiedRunIds: cleanupUnverified,
      updatedUtc: utcNow()
    });
    return failures.length ? 1 : 0;
  } finally {
    await lock.release();
    await sink.close();
  }
}

if (isMain(import.meta.url)) process.exitCode = await main();
