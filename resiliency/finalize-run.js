import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { basename, dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { driverMetadata } from "./driver-info.js";
import { ATLAS_AGENT, validateAtlasManifest } from "./atlas.js";
import { isMain, parseArgs, readJson, round, writeJson } from "./common.js";
import { validate } from "./validate-run-document.js";

export const PHASE_MAP = Object.freeze({
  baseline: "preScalingPhase",
  "scale-up": "scalingInProgressPhase",
  "post-scale": "postScalePhase"
});
export const OPERATION_TYPES = Object.freeze(["insert", "point_read", "health"]);

export function parseUtc(value) {
  return Date.parse(value);
}

export function formatUtc(value) {
  return new Date(value).toISOString();
}

export function readJsonlText(text, name = "input") {
  return text.split(/\r?\n/).flatMap((line, index) => {
    if (!line) return [];
    try { return [JSON.parse(line)]; }
    catch (error) { throw new Error(`Invalid JSONL at ${name}:${index + 1}: ${error.message}`); }
  });
}

export async function readJsonl(path) {
  return readJsonlText(await readFile(path, "utf8"), basename(path));
}

export async function sha256(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

export function latestCompleteSequence(events) {
  const sequences = [];
  for (const event of events) {
    if (event.event === "run_start") sequences.push([]);
    sequences.at(-1)?.push(event);
  }
  const complete = sequences.filter((sequence) => sequence.some((item) => item.event === "run_complete"));
  if (!complete.length) throw new Error("No complete control-plane event sequence");
  return [complete.at(-1), sequences];
}

function controlPlaneOperation(operation) {
  if (/^provision/i.test(operation)) return "provisioning";
  if (/^(?:scale|scaling)/i.test(operation)) return "scaling";
  if (/^(?:cleanup|delete)/i.test(operation)) return "cleanup";
  return null;
}

export function controlPlaneFailures(events, cleanup = null) {
  const failures = events
    .flatMap((item) => {
      if (item.event !== "operation_terminal_error") return [];
      const operation = controlPlaneOperation(String(item.operation ?? ""));
      if (!operation) return [];
      return [{
        operation,
        timestampUtc: item.timestamp_utc,
        errorCode: String(item.error_code ?? "AtlasRequestFailed"),
        errorMessage: String(item.error_detail ?? "Atlas operation failed"),
        controlPlaneOperationId: item.control_plane_operation_id == null ? null : String(item.control_plane_operation_id),
        retryCount: Number.isInteger(item.retry_count) ? item.retry_count : null,
        finalOutcome: ["succeeded", "failed"].includes(item.final_outcome)
          ? item.final_outcome
          : null,
        stackTrace: String(
          item.stack_trace ??
          `${item.error_code ?? "Error"}: ${item.error_detail ?? "Atlas operation failed"}`
        )
      }];
    });
  if (cleanup?.failure) {
    failures.push({
      operation: controlPlaneOperation(String(cleanup.failure.operation ?? "")) ?? "cleanup",
      timestampUtc: cleanup.failure.timestampUtc,
      errorCode: String(cleanup.failure.errorCode ?? "AtlasRequestFailed"),
      errorMessage: String(cleanup.failure.errorMessage ?? "Atlas cleanup failed"),
      controlPlaneOperationId: cleanup.failure.controlPlaneOperationId == null
        ? null : String(cleanup.failure.controlPlaneOperationId),
      retryCount: Number.isInteger(cleanup.failure.retryCount)
        ? cleanup.failure.retryCount
        : 0,
      finalOutcome: cleanup.failure.finalOutcome ?? "failed",
      stackTrace: String(
        cleanup.failure.stackTrace ??
        `${cleanup.failure.errorCode ?? "Error"}: ${cleanup.failure.errorMessage ?? "Atlas cleanup failed"}`
      )
    });
  }
  const consolidated = new Map();
  for (const failure of failures) {
    const previous = consolidated.get(failure.operation);
    if (!previous) {
      consolidated.set(failure.operation, { ...failure, legacyFailureCount: 1 });
      continue;
    }
    consolidated.set(failure.operation, {
      ...failure,
      controlPlaneOperationId:
        failure.controlPlaneOperationId ?? previous.controlPlaneOperationId,
      retryCount: failure.retryCount ?? previous.retryCount,
      finalOutcome: failure.finalOutcome ?? previous.finalOutcome,
      legacyFailureCount: previous.legacyFailureCount + 1
    });
  }
  return [...consolidated.values()].map(({ legacyFailureCount, ...failure }) => ({
    ...failure,
    retryCount: failure.retryCount ?? Math.max(0, legacyFailureCount - 1),
    finalOutcome: failure.finalOutcome ?? (
      failure.operation === "provisioning" &&
      events.some((item) => item.event === "provision_attempt_end" && item.success === true)
        ? "succeeded"
        : "failed"
    )
  }));
}

export function percentile(values, percentileValue) {
  if (!values.length) return null;
  const ordered = [...values].sort((a, b) => a - b);
  const rank = (ordered.length - 1) * percentileValue;
  const lower = Math.floor(rank);
  const upper = Math.ceil(rank);
  return round(lower === upper ? ordered[lower] :
    ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower));
}

export function operationDetail(events) {
  const attempted = events.length;
  const succeeded = events.filter((item) => item.success === true).length;
  const latencies = events.map((item) => item.latency_ms).filter(Number.isFinite);
  const maximumLatency = latencies.reduce((maximum, latency) => Math.max(maximum, latency), -Infinity);
  const errorCategories = {};
  for (const item of events.filter((item) => item.success !== true)) {
    const category = String(item.error_type ?? "UnknownError");
    errorCategories[category] = (errorCategories[category] ?? 0) + 1;
  }
  return {
    attempted, succeeded, failed: attempted - succeeded,
    retryAttempts: events.reduce((sum, item) => sum + Number(item.retry_count ?? 0), 0),
    availabilityPercent: attempted ? round(succeeded / attempted * 100) : null,
    latencyMs: {
      p50: percentile(latencies, 0.5), p95: percentile(latencies, 0.95),
      p99: percentile(latencies, 0.99), max: latencies.length ? round(maximumLatency) : null
    },
    errorCategories: Object.fromEntries(Object.entries(errorCategories).sort())
  };
}

export function phaseMetrics(events) {
  const attempted = events.length;
  const succeeded = events.filter((item) => item.success === true).length;
  let earliestStart = Infinity;
  let latestEnd = -Infinity;
  for (const event of events) {
    earliestStart = Math.min(earliestStart, parseUtc(event.started_utc));
    latestEnd = Math.max(latestEnd, parseUtc(event.ended_utc));
  }
  const observedSeconds = events.length ? round((latestEnd - earliestStart) / 1000) : 0;
  return {
    attempted, succeeded, failed: attempted - succeeded,
    availabilityPercent: attempted ? round(succeeded / attempted * 100) : null,
    observedSeconds
  };
}

function floorMinute(value) {
  const date = new Date(value);
  date.setUTCSeconds(0, 0);
  return date.getTime();
}

export function requestVolumePerMinute(events) {
  if (!events.length) return [];
  let earliestStart = Infinity;
  let latestEnd = -Infinity;
  const counts = new Map();
  for (const event of events) {
    earliestStart = Math.min(earliestStart, parseUtc(event.started_utc));
    latestEnd = Math.max(latestEnd, parseUtc(event.ended_utc));
    const bucket = floorMinute(parseUtc(event.ended_utc));
    counts.set(bucket, (counts.get(bucket) ?? 0) + 1);
  }
  const first = floorMinute(earliestStart);
  const final = floorMinute(latestEnd);
  const buckets = [];
  for (let cursor = first; cursor <= final; cursor += 60_000) {
    buckets.push({ startTimestamp: formatUtc(cursor), requestCount: counts.get(cursor) ?? 0 });
  }
  return buckets;
}

function floorSecond(value) {
  const date = new Date(value);
  date.setUTCMilliseconds(0);
  return date.getTime();
}

function dip(start, end, events) {
  const failures = events.filter((item) => item.success !== true);
  const affected = [...new Set(failures.map((item) => String(item.operation ?? "unknown")))].sort();
  const counts = new Map();
  for (const failure of failures) {
    const category = String(failure.error_type ?? "UnknownError");
    counts.set(category, (counts.get(category) ?? 0) + 1);
  }
  const errorCategories = [...counts.keys()].sort();
  return {
    startUtc: formatUtc(start), endUtc: formatUtc(end),
    durationSeconds: round(Math.max(0, end - start) / 1000),
    phase: "scale-up",
    affectedOperationTypes: affected.length ? affected : [...OPERATION_TYPES],
    failedOperations: failures.length,
    errorCategories,
    errorCategoryCounts: Object.fromEntries(errorCategories.map((category) => [category, counts.get(category)])),
    overlapsControlPlaneOperation: true
  };
}

export function bucketDips(events, scaleStart, scaleEnd) {
  const buckets = new Map();
  for (const event of events.filter((item) => item.phase === "scale-up")) {
    const started = parseUtc(event.started_utc);
    if (started < scaleStart || started > scaleEnd) continue;
    const bucket = floorSecond(started);
    buckets.set(bucket, [...(buckets.get(bucket) ?? []), event]);
  }
  const states = [];
  for (let cursor = floorSecond(scaleStart); cursor <= floorSecond(scaleEnd); cursor += 1000) {
    const items = buckets.get(cursor) ?? [];
    const healthy = items.some((item) => item.success === true) &&
      !items.some((item) => item.success !== true);
    states.push([cursor, healthy, items]);
  }
  const raw = [];
  let rawStart = null;
  let rawItems = [];
  for (const [bucket, healthy, items] of states) {
    if (!healthy) {
      rawStart ??= Math.max(bucket, scaleStart);
      rawItems.push(...items);
    } else if (rawStart !== null) {
      raw.push(dip(rawStart, Math.min(bucket, scaleEnd), rawItems));
      rawStart = null;
      rawItems = [];
    }
  }
  if (rawStart !== null) raw.push(dip(rawStart, scaleEnd, rawItems));

  const merged = [];
  let index = 0;
  while (index < states.length) {
    const [bucket, healthy] = states[index];
    if (healthy) { index += 1; continue; }
    const start = Math.max(bucket, scaleStart);
    const collected = [];
    let healthyRun = 0;
    let firstHealthy = null;
    while (index < states.length) {
      const [currentBucket, currentHealthy, items] = states[index];
      if (currentHealthy) {
        if (healthyRun === 0) firstHealthy = currentBucket;
        healthyRun += 1;
        if (healthyRun >= 3) break;
      } else {
        healthyRun = 0;
        firstHealthy = null;
        collected.push(...items);
      }
      index += 1;
    }
    const end = firstHealthy !== null && healthyRun >= 3 ? Math.min(firstHealthy, scaleEnd) : scaleEnd;
    merged.push(dip(start, end, collected));
    if (healthyRun >= 3) index += 1;
  }
  return [raw, merged, round(merged.reduce((sum, item) => sum + item.durationSeconds, 0))];
}

export function aggregateRequestMetrics(operations, threadCount, scaleStart = null, scaleEnd = null) {
  if (threadCount < 1) throw new Error("Workload thread count must be positive");
  if ((scaleStart === null) !== (scaleEnd === null)) throw new Error("Both scale boundaries are required for thread availability dips");
  const byThread = new Map();
  for (const operation of operations) {
    const id = operation.thread_id;
    if (!Number.isInteger(id) || id < 1 || id > threadCount) {
      throw new Error(`Operation evidence has invalid thread_id ${id}`);
    }
    const threadOperations = byThread.get(id);
    if (threadOperations) threadOperations.push(operation);
    else byThread.set(id, [operation]);
  }
  const metricsFor = (events) => {
    const phases = Object.fromEntries(Object.entries(PHASE_MAP).map(([phase, schemaName]) => [
      schemaName, phaseMetrics(events.filter((item) => item.phase === phase))
    ]));
    const allOperations = {};
    for (const field of ["attempted", "succeeded", "failed"]) {
      allOperations[field] = Object.values(phases).reduce((sum, item) => sum + item[field], 0);
    }
    allOperations.availabilityPercent = allOperations.attempted
      ? round(allOperations.succeeded / allOperations.attempted * 100) : null;
    allOperations.observedSeconds = round(Object.values(phases).reduce((sum, item) => sum + item.observedSeconds, 0));
    return { allOperations, ...phases };
  };
  const perThread = Array.from({ length: threadCount }, (_, index) => {
    const threadId = index + 1;
    const threadOperations = byThread.get(threadId) ?? [];
    const availabilityDips = scaleStart === null ? [] : bucketDips(threadOperations, scaleStart, scaleEnd)[1];
    return { threadId, ...metricsFor(threadOperations), availabilityDips };
  });
  const root = metricsFor(operations);
  for (const metricName of ["allOperations", ...Object.values(PHASE_MAP)]) {
    for (const field of ["attempted", "succeeded", "failed"]) {
      root[metricName][field] = perThread.reduce((sum, thread) => sum + thread[metricName][field], 0);
    }
    root[metricName].availabilityPercent = root[metricName].attempted
      ? round(root[metricName].succeeded / root[metricName].attempted * 100) : null;
  }
  root.perThread = perThread;
  root.requestVolumePerMinute = requestVolumePerMinute(operations);
  return root;
}

export function longestNoSuccess(events, operation) {
  const successes = events.filter((item) => item.operation === operation && item.success === true)
    .map((item) => parseUtc(item.ended_utc)).sort((a, b) => a - b);
  if (successes.length < 2) return null;
  let longestSeconds = 0;
  for (let index = 1; index < successes.length; index += 1) {
    longestSeconds = Math.max(longestSeconds, (successes[index] - successes[index - 1]) / 1000);
  }
  return round(longestSeconds);
}

export function buildExperienceReport({
  manifest, aggregateMetrics, provisioningSeconds, scaleSeconds,
  availabilityDips, cleanupComplete, inconclusiveErrorCategory = null
}) {
  const { initialComputeTier, targetComputeTier } = manifest.experiment;
  const all = aggregateMetrics.allOperations;
  const scaling = aggregateMetrics.scalingInProgressPhase ?? {};
  const attempted = Number(all.attempted);
  const failed = Number(all.failed);
  const failureRate = attempted ? failed / attempted * 100 : 0;
  const disrupted = Number(scaling.disruptedSeconds ?? 0);
  const threadMetrics = aggregateMetrics.perThread ?? [];
  const affectedThreads = threadMetrics.filter((thread) => thread.allOperations.failed > 0).length;
  const provisioningOpinion = provisioningSeconds == null
    ? "I could not determine how long provisioning took, which made the setup experience hard to judge."
    : provisioningSeconds <= 300
      ? `Provisioning took ${provisioningSeconds} seconds, so I did not spend much time waiting to begin.`
      : provisioningSeconds <= 600
        ? `Provisioning took ${provisioningSeconds} seconds. That felt noticeable but still reasonable for a temporary test cluster.`
        : `Provisioning took ${provisioningSeconds} seconds. I spent longer waiting to start than I would want for a simple developer test.`;
  const scaleOpinion = scaleSeconds == null
    ? "I could not determine the scale duration, so I was left without a clear sense of how long the operation occupied the test."
    : scaleSeconds <= 300
      ? `The ${initialComputeTier}-to-${targetComputeTier} scale completed in ${scaleSeconds} seconds, which felt quick for a live tier change.`
      : scaleSeconds <= 900
        ? `The ${initialComputeTier}-to-${targetComputeTier} scale took ${scaleSeconds} seconds. I noticed the wait, but it did not dominate the exercise.`
        : `The ${initialComputeTier}-to-${targetComputeTier} scale took ${scaleSeconds} seconds. I spent a lot of time waiting for the cluster to move up one tier.`;
  const requestOpinion = failed === 0
    ? `All ${attempted} observed requests succeeded, which gave me confidence that the application stayed usable throughout the test.`
    : failureRate <= 0.1
      ? `I saw ${failed} ${failed === 1 ? "failure" : "failures"} across ${attempted} requests (${failureRate.toFixed(3)}%). The rate was low, but I still expected a routine scale-up to avoid request failures.`
      : `I saw ${failed} ${failed === 1 ? "failure" : "failures"} across ${attempted} requests (${failureRate.toFixed(3)}%). That was more disruption than I expected from a one-tier scale-up.`;
  const connectivityOpinion = disrupted === 0
    ? "I did not observe a connectivity-loss window during scaling, which matched the experience I hoped for."
    : disrupted <= 5
      ? `Connectivity disruption totaled ${disrupted} seconds. I noticed it, but the interruption was brief.`
      : `Connectivity disruption totaled ${disrupted} seconds across ${availabilityDips.length} observed dip(s). I expected the loss of connectivity during scaling to be much shorter.`;
  const threadOpinion = threadMetrics.length
    ? `Failures affected ${affectedThreads} of ${threadMetrics.length} workload threads, so the impact was ${affectedThreads <= 1 ? "localized" : "visible across concurrent application activity"}.`
    : "I did not have thread-level evidence available, which limited how confidently I could judge the breadth of the impact.";
  const outcome = inconclusiveErrorCategory
    ? `**INCONCLUSIVE.** I could not reach a conclusive end-to-end verdict because the run ended with ${inconclusiveErrorCategory}. `
    : "I completed the test and used the observed behavior, rather than just the control-plane success state, to judge the experience. ";
  const cleanupOpinion = cleanupComplete
    ? "The temporary resources were cleaned up successfully, so I did not have to do manual follow-up."
    : "Cleanup was not yet verified, so I would not consider the exercise finished without follow-up.";
  return `# Backend developer experience for run ${manifest.runId}\n\n${outcome}I approached this as a backend developer checking whether a database scale-up would be uneventful for a simple application.\n\n${provisioningOpinion} ${scaleOpinion}\n\n${requestOpinion} ${connectivityOpinion} ${threadOpinion}\n\n${cleanupOpinion}`;
}

export async function buildFinalDocument(directory, manifest, options = {}) {
  const operationsPath = resolve(directory, "operations.jsonl");
  const controlPath = resolve(directory, "control-plane.jsonl");
  const operationEvents = await readJsonl(operationsPath);
  const operations = operationEvents.filter((item) => item.kind === "data_plane_operation");
  const control = await readJsonl(controlPath);
  if (!operations.length) throw new Error("No data-plane operation evidence");
  const mismatches = operations.filter((item) => String(item.run_id ?? item.runId ?? "") !== manifest.runId).length;
  if (mismatches) throw new Error(`Operation evidence contains ${mismatches} run-ID mismatches`);
  const [latest, sequences] = latestCompleteSequence(control);
  const event = Object.fromEntries(latest.map((item) => [String(item.event), item]));
  for (const required of ["run_start", "provision_attempt_start", "provision_attempt_end", "scale_submitted", "scale_terminal", "run_complete"]) {
    if (!event[required]) throw new Error(`Latest sequence is missing event: ${required}`);
  }
  const scaleStart = parseUtc(event.scale_submitted.timestamp_utc);
  const scaleEnd = parseUtc(event.scale_terminal.timestamp_utc);
  const [rawDips, mergedDips, disrupted] = bucketDips(operations, scaleStart, scaleEnd);
  const scaleStopwatch = round(Number(event.scale_terminal.elapsed_seconds));
  const scaleObserved = round((scaleEnd - scaleStart) / 1000);
  const available = round(Math.max(0, scaleObserved - disrupted));
  const aggregateMetrics = aggregateRequestMetrics(
    operations, Number(manifest.experiment.workloadThreads), scaleStart, scaleEnd
  );
  const scaling = aggregateMetrics.scalingInProgressPhase;
  Object.assign(scaling, {
    observedSeconds: scaleObserved, availableSeconds: available,
    disruptedSeconds: disrupted,
    timeAvailabilityPercent: scaleObserved ? round(available / scaleObserved * 100) : null,
    rawDisruptionWindows: rawDips.length,
    longestRawDisruptionSeconds: rawDips.reduce(
      (longest, item) => Math.max(longest, item.durationSeconds), 0
    )
  });
  aggregateMetrics.allOperations.observedSeconds = round([
    "preScalingPhase", "scalingInProgressPhase", "postScalePhase"
  ].reduce((sum, name) => sum + aggregateMetrics[name].observedSeconds, 0));
  const provisioningStart = parseUtc(event.provision_attempt_start.timestamp_utc);
  const provisionState = latest.find((item) =>
    item.event === "state_transition" && item.operation === "provision" &&
    item.provisioning_state === "Succeeded" && item.cluster_status === "Ready");
  if (!provisionState) throw new Error("Latest sequence has no successful provision state transition");
  const provisioningEnd = parseUtc(provisionState.timestamp_utc);
  const startUtc = parseUtc(event.run_start.timestamp_utc);
  const endUtc = options.endUtc ? parseUtc(options.endUtc) : Date.now();
  const cleanupSubmitted = options.cleanupSubmittedUtc ? parseUtc(options.cleanupSubmittedUtc) : null;
  const cleanupCompleted = options.cleanupCompletedUtc ? parseUtc(options.cleanupCompletedUtc) : null;
  const report = buildExperienceReport({
    manifest, aggregateMetrics,
    provisioningSeconds: round((provisioningEnd - provisioningStart) / 1000),
    scaleSeconds: scaleObserved, availabilityDips: mergedDips,
    cleanupComplete: cleanupCompleted !== null
  });
  const document = {
    _id: manifest.runId, date: formatUtc(startUtc).slice(0, 10),
    agentName: ATLAS_AGENT, provider: "atlas", product: "MongoDB Atlas",
    atlasProjectId: manifest.atlas.projectId, atlasTopology: "3-node replica set",
    ...driverMetadata(),
    clientObservedAvailabilityPerc: scaling.availabilityPercent,
    totalRuntimeSeconds: round((endUtc - startUtc) / 1000),
    provisioningTimeSeconds: round((provisioningEnd - provisioningStart) / 1000),
    scaleUpTimeSeconds: scaleObserved,
    cleanupTimeSeconds: cleanupSubmitted !== null && cleanupCompleted !== null
      ? round((cleanupCompleted - cleanupSubmitted) / 1000) : null,
    startTimeUTC: formatUtc(startUtc), endTimeUTC: formatUtc(endUtc),
    clusterName: manifest.clusterName, region: manifest.location,
    initialComputeTier: manifest.experiment.initialComputeTier,
    targetComputeTier: manifest.experiment.targetComputeTier,
    storageSizeGb: manifest.experiment.storageSizeGb,
    parameters: {
      connectionTimeoutSeconds: manifest.experiment.timeoutSeconds,
      operationTimeoutSeconds: manifest.experiment.timeoutSeconds,
      maxDataPlaneRetries: manifest.experiment.maxDataPlaneRetries
    },
    controlPlaneTimeline: {
      provisioningSubmittedUtc: formatUtc(provisioningStart),
      provisioningCompletedUtc: formatUtc(provisioningEnd),
      scaleSubmittedUtc: formatUtc(scaleStart), scaleCompleted: formatUtc(scaleEnd),
      scaleResult: "Succeeded",
      cleanupSubmittedUtc: cleanupSubmitted === null ? null : formatUtc(cleanupSubmitted),
      cleanupCompletedUtc: cleanupCompleted === null ? null : formatUtc(cleanupCompleted)
    },
    controlPlaneFailures: controlPlaneFailures(latest),
    aggregateMetrics, availabilityDips: mergedDips, reportMarkdown: report
  };
  const evidence = {
    runId: manifest.runId,
    authoritativeAttemptRule: "latest control-plane sequence ending in run_complete; current operations.jsonl only",
    excludedEvidence: ["earlier failed control-plane sequences"],
    latestSequence: { startUtc: formatUtc(startUtc), endUtc: event.run_complete.timestamp_utc },
    controlPlaneSequences: sequences.map((sequence, index) => {
      const terminal = [...sequence].reverse().find((item) => ["run_complete", "run_failed"].includes(item.event));
      return {
        sequence: index + 1, startUtc: sequence[0].timestamp_utc,
        terminalEvent: terminal?.event ?? "incomplete",
        terminalUtc: terminal?.timestamp_utc ?? null, eventCount: sequence.length
      };
    }),
    provisioningTimeSeconds: document.provisioningTimeSeconds,
    scaleBoundarySeconds: round((scaleEnd - scaleStart) / 1000),
    scaleStopwatchSeconds: scaleStopwatch, scaleObservedSeconds: scaleObserved,
    availableSeconds: available, disruptedSeconds: disrupted,
    timeAvailabilityPercent: scaling.timeAvailabilityPercent,
    rawAvailabilityDips: rawDips, mergedAvailabilityDips: mergedDips,
    runIdMismatchCount: mismatches,
    files: {
      operations: { path: basename(operationsPath), lines: operationEvents.length, sha256: await sha256(operationsPath) },
      controlPlane: { path: basename(controlPath), lines: control.length, sha256: await sha256(controlPath) }
    }
  };
  return { document, evidence, report };
}

export async function main(argv = process.argv.slice(2)) {
  const here = dirname(fileURLToPath(import.meta.url));
  const args = parseArgs(argv, {
    manifest: { type: "string" }, schema: { type: "string", default: resolve(here, "run-document.schema.json") },
    "cleanup-submitted-utc": { type: "string" }, "cleanup-completed-utc": { type: "string" },
    "end-utc": { type: "string" }
  });
  if (args._.length !== 1 || !args.manifest) {
    throw new Error("Usage: finalize-run.js <run-directory> --manifest <path>");
  }
  const directory = resolve(args._[0]);
  const manifest = await readJson(resolve(args.manifest));
  validateAtlasManifest(manifest);
  const { document, evidence, report } = await buildFinalDocument(directory, manifest, {
    cleanupSubmittedUtc: args["cleanup-submitted-utc"],
    cleanupCompletedUtc: args["cleanup-completed-utc"],
    endUtc: args["end-utc"]
  });
  const errors = validate(document, await readJson(resolve(args.schema)));
  if (errors.length) throw new Error(`Run document validation failed: ${errors.join("; ")}`);
  await writeJson(resolve(directory, "aggregate-evidence.json"), evidence);
  await writeFile(resolve(directory, "report.md"), report, "utf8");
  await writeJson(resolve(directory, "final-run-document.json"), document);
  console.log(JSON.stringify({
    validation: "PASS", document: resolve(directory, "final-run-document.json"),
    report: resolve(directory, "report.md"), evidence: resolve(directory, "aggregate-evidence.json"),
    scaleAvailabilityPercent: document.aggregateMetrics.scalingInProgressPhase.availabilityPercent,
    scaleTimeAvailabilityPercent: document.aggregateMetrics.scalingInProgressPhase.timeAvailabilityPercent,
    availabilityDips: document.availabilityDips.length,
    cleanupComplete: Boolean(args["cleanup-completed-utc"])
  }, null, 2));
  return 0;
}

if (isMain(import.meta.url)) process.exitCode = await main();
