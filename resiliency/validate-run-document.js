import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import Ajv2020 from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { isMain, parseArgs, readJson, round } from "./common.js";
import { ATLAS_AGENT, validateAtlasManifest } from "./atlas.js";
import { validateComputeTransition } from "./sku-config.js";

const PROHIBITED_ROOT = new Set([
  "tags", "plannedResourceIds", "ownedResourceIds", "validation", "evidenceHashes",
  "summaryHash", "clusterResourceId", "cleanup", "integrity", "eventPersistence",
  "limitations", "runId", "schemaVersion", "status", "verdict"
]);
const PROHIBITED_PARAMETERS = new Set([
  "baselineSeconds", "postScaleSeconds", "requestPacing", "errorBudget",
  "networkPolicy", "cleanupPolicy", "initialComputeTier", "targetComputeTier",
  "storageSizeGb"
]);
const PROHIBITED_TIMELINE = new Set([
  "terminalComputeTier", "terminalTier", "terminalClusterStatus",
  "scaleDurationSeconds", "scaleTerminalUtc", "scaleTerminalResult"
]);

function intersects(set, object) {
  return Object.keys(object ?? {}).filter((key) => set.has(key));
}

function close(left, right, tolerance = 0.002) {
  return Math.abs(left - right) <= tolerance;
}

function expectedAvailability(metrics) {
  return metrics.attempted ? round(metrics.succeeded / metrics.attempted * 100) : null;
}

export function validate(document, schema) {
  const ajv = new Ajv2020({ allErrors: true, strict: false });
  addFormats(ajv);
  const schemaValidate = ajv.compile(schema);
  const errors = schemaValidate(document)
    ? []
    : schemaValidate.errors.map((error) => `${error.instancePath || "/"} ${error.message}`);
  if (document.provider !== "atlas" || document.agentName !== ATLAS_AGENT ||
      !/^[a-f0-9]{24}$/i.test(document.atlasProjectId ?? "") ||
      document.atlasTopology !== "3-node replica set") {
    errors.push("Atlas result identity or topology is invalid");
  }
  try { validateComputeTransition(document.initialComputeTier, document.targetComputeTier); }
  catch (error) { errors.push(error.message); }

  const prohibitedRoot = intersects(PROHIBITED_ROOT, document);
  if (prohibitedRoot.length) errors.push(`prohibited root fields: ${prohibitedRoot.sort()}`);
  if (intersects(PROHIBITED_PARAMETERS, document.parameters).length) {
    errors.push("prohibited parameter fields present");
  }
  const timeline = document.controlPlaneTimeline ?? {};
  if (intersects(PROHIBITED_TIMELINE, timeline).length) {
    errors.push("prohibited control-plane timeline fields present");
  }

  const metrics = document.aggregateMetrics ?? {};
  const phaseNames = ["preScalingPhase", "scalingInProgressPhase", "postScalePhase"];
  const phases = phaseNames.map((name) => metrics[name]);
  const all = metrics.allOperations;
  for (const [name, value] of [["allOperations", all], ...phaseNames.map((name, index) => [name, phases[index]])]) {
    if (!value) continue;
    if (value.succeeded + value.failed !== value.attempted) {
      errors.push(`${name} operation counts do not reconcile`);
    }
    if (value.availabilityPercent !== expectedAvailability(value)) {
      errors.push(`${name} availabilityPercent is incorrect`);
    }
  }
  const scaling = metrics.scalingInProgressPhase;
  if (document.clientObservedAvailabilityPerc !== (scaling?.availabilityPercent ?? null)) {
    errors.push("clientObservedAvailabilityPerc differs from scaling availability");
  }
  const nonNullPhases = phases.filter(Boolean);
  if (all) {
    for (const field of ["attempted", "succeeded", "failed"]) {
      if (all[field] !== nonNullPhases.reduce((sum, value) => sum + value[field], 0)) {
        errors.push(`allOperations.${field} does not equal phase sum`);
      }
    }
    if (!close(all.observedSeconds, nonNullPhases.reduce((sum, value) => sum + value.observedSeconds, 0))) {
      errors.push("allOperations.observedSeconds does not equal phase sum");
    }
  }
  const perThread = metrics.perThread ?? [];
  const dipLists = [
    ["availabilityDips", document.availabilityDips ?? []],
    ...perThread.map((thread) => [`thread ${thread.threadId} availabilityDips`, thread.availabilityDips ?? []])
  ];
  for (const [name, dips] of dipLists) {
    for (const [index, dip] of dips.entries()) {
      const counts = dip.errorCategoryCounts;
      if (!counts || typeof counts !== "object" || Array.isArray(counts)) continue;
      if (Object.values(counts).reduce((sum, count) => sum + count, 0) !== dip.failedOperations) {
        errors.push(`${name}[${index}] errorCategoryCounts does not sum to failedOperations`);
      }
      if (JSON.stringify(Object.keys(counts).sort()) !== JSON.stringify([...(dip.errorCategories ?? [])].sort())) {
        errors.push(`${name}[${index}] errorCategoryCounts keys differ from errorCategories`);
      }
    }
  }
  const ids = perThread.map((value) => value.threadId);
  if (new Set(ids).size !== ids.length) errors.push("perThread contains duplicate threadId values");
  for (const thread of perThread) {
    const threadPhases = phaseNames.map((name) => thread[name]);
    for (const [name, value] of [["allOperations", thread.allOperations], ...phaseNames.map((name, index) => [name, threadPhases[index]])]) {
      if (value.succeeded + value.failed !== value.attempted) {
        errors.push(`thread ${thread.threadId} ${name} operation counts do not reconcile`);
      }
      if (value.availabilityPercent !== expectedAvailability(value)) {
        errors.push(`thread ${thread.threadId} ${name} availabilityPercent is incorrect`);
      }
    }
    for (const field of ["attempted", "succeeded", "failed"]) {
      if (thread.allOperations[field] !== threadPhases.reduce((sum, value) => sum + value[field], 0)) {
        errors.push(`thread ${thread.threadId} allOperations.${field} does not equal phase sum`);
      }
    }
  }
  if (all) {
    for (const field of ["attempted", "succeeded", "failed"]) {
      if (all[field] !== perThread.reduce((sum, thread) => sum + thread.allOperations[field], 0)) {
        errors.push(`allOperations.${field} does not equal perThread sum`);
      }
    }
    if (metrics.requestVolumePerMinute &&
        all.attempted !== metrics.requestVolumePerMinute.reduce((sum, bucket) => sum + bucket.requestCount, 0)) {
      errors.push("allOperations.attempted does not equal request-volume sum");
    }
  }
  phaseNames.forEach((phaseName, index) => {
    const phase = phases[index];
    if (!phase) return;
    for (const field of ["attempted", "succeeded", "failed"]) {
      if (phase[field] !== perThread.reduce((sum, thread) => sum + thread[phaseName][field], 0)) {
        errors.push(`${phaseName}.${field} does not equal perThread sum`);
      }
    }
  });
  if (scaling && !close(scaling.availableSeconds + scaling.disruptedSeconds, scaling.observedSeconds)) {
    errors.push("scaling time components do not reconcile");
  }

  const elapsedSeconds = (start, end) => round((Date.parse(end) - Date.parse(start)) / 1000);
  if (document.startTimeUTC && document.date !== document.startTimeUTC.slice(0, 10)) {
    errors.push("date differs from startTimeUTC date");
  }
  if (document.startTimeUTC && document.endTimeUTC && document.totalRuntimeSeconds !== null &&
      !close(document.totalRuntimeSeconds, elapsedSeconds(document.startTimeUTC, document.endTimeUTC))) {
    errors.push("totalRuntimeSeconds differs from UTC boundaries");
  }
  for (const [duration, start, end] of [
    ["provisioningTimeSeconds", "provisioningSubmittedUtc", "provisioningCompletedUtc"],
    ["scaleUpTimeSeconds", "scaleSubmittedUtc", "scaleCompleted"],
    ["cleanupTimeSeconds", "cleanupSubmittedUtc", "cleanupCompletedUtc"]
  ]) {
    if (document[duration] !== null && timeline[start] && timeline[end] &&
        !close(document[duration], elapsedSeconds(timeline[start], timeline[end]))) {
      errors.push(`${duration} differs from UTC boundaries`);
    }
  }
  return errors;
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv, { schema: { type: "string" }, manifest: { type: "string" } });
  if (args._.length !== 1) throw new Error("Usage: validate-run-document.js <document> [--schema path] [--manifest path]");
  const here = dirname(fileURLToPath(import.meta.url));
  const document = await readJson(resolve(args._[0]));
  const schema = await readJson(resolve(args.schema ?? here, args.schema ? "" : "run-document.schema.json"));
  const errors = validate(document, schema);
  if (args.manifest) {
    const manifest = await readJson(resolve(args.manifest));
    for (const field of ["clusterName", "provider"]) {
      if (document[field] !== manifest[field]) errors.push(`document and manifest ${field} differ`);
    }
    try { validateAtlasManifest(manifest, { approved: false }); }
    catch (error) { errors.push(error.message); }
    if (document.atlasProjectId !== manifest.atlas?.projectId) errors.push("document and manifest Atlas projects differ");
    if (manifest.ownedResourceIds?.length) errors.push("fresh manifest already has owned resources");
  }
  if (errors.length) {
    errors.forEach((error) => console.error(`ERROR: ${error}`));
    return 1;
  }
  console.log("PASS: schema and run-document invariants");
  return 0;
}

if (isMain(import.meta.url)) {
  process.exitCode = await main();
}
