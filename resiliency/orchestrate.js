import { appendFile, mkdir, readFile, rename, stat, unlink, writeFile } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { spawn } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  atomicJson, atomicText, isMain, parseArgs, readJson, round, runNode,
  sanitizeError, sanitizeStackTrace, sleep, utcNow
} from "./common.js";
import { sendControlPlaneFailureEmail } from "./email-notifier.js";
import { validateComputeTransition, workloadThreads } from "./sku-config.js";
import { AtlasClient, validateAtlasManifest } from "./atlas.js";

let stopping = false;

export async function recordControlPlaneFailure(
  events, operation, error, { retryCount = 0, finalOutcome = "failed" } = {}
) {
  if (error && typeof error === "object" && error.recorded) return;
  if (!["provisioning", "scaling", "cleanup"].includes(operation)) {
    throw new Error(`Unknown control-plane operation: ${operation}`);
  }
  const failure = error instanceof Error ? error : new Error(String(error));
  const details = {
    runId: events.runId, operation, timestampUtc: utcNow(),
    errorCode: String(error?.code ?? failure.name),
    errorMessage: sanitizeError(failure.message),
    controlPlaneOperationId: error?.controlPlaneOperationId == null ? null : String(error.controlPlaneOperationId),
    retryCount, finalOutcome, stackTrace: sanitizeStackTrace(failure.stack ?? `${failure.name}: ${failure.message}`)
  };
  await events.write("operation_terminal_error", {
    timestamp_utc: details.timestampUtc, operation,
    error_code: details.errorCode, error_detail: details.errorMessage,
    control_plane_operation_id: details.controlPlaneOperationId,
    retry_count: retryCount, final_outcome: finalOutcome, stack_trace: details.stackTrace
  });
  try { await sendControlPlaneFailureEmail(details); }
  catch (notificationError) {
    await events.write("control_plane_notification_failed", {
      failed_operation: operation, error_category: notificationError.name,
      error_detail: sanitizeError(notificationError.message)
    });
  }
  if (error && typeof error === "object") error.recorded = true;
}

export class Events {
  constructor(path, runId) {
    this.path = path;
    this.runId = runId;
    this.sequence = 0;
    this.pending = Promise.resolve();
  }

  async open() {
    await mkdir(dirname(this.path), { recursive: true });
  }

  async write(event, details = {}) {
    const sequence = this.sequence++;
    const value = {
      _id: `${this.runId}:control:${sequence}`, schema_version: 1,
      kind: "control_plane", run_id: this.runId, runId: this.runId,
      event_sequence: sequence, timestamp_utc: utcNow(), event, ...details
    };
    this.pending = this.pending.then(() => appendFile(this.path, `${JSON.stringify(value)}\n`, "utf8"));
    await this.pending;
  }

  async close() {
    await this.pending;
  }
}

async function exists(path) {
  try { await stat(path); return true; }
  catch (error) {
    if (error.code !== "ENOENT") throw error;
    return false;
  }
}

export async function workloadHealth(path, phase, minimumSuccesses = 1) {
  const counts = { insert: 0, point_read: 0, health: 0 };
  let latestEnd = 0;
  for (const line of (await readFile(path, "utf8")).split(/\r?\n/)) {
    if (!line) continue;
    const event = JSON.parse(line);
    if (event.kind !== "data_plane_operation" || event.phase !== phase) continue;
    if (Object.hasOwn(counts, event.operation) && event.success === true) {
      counts[event.operation] += 1;
      latestEnd = Math.max(latestEnd, Date.parse(event.ended_utc));
    }
  }
  const missing = Object.entries(counts).filter(([, count]) => count < minimumSuccesses).map(([name]) => name);
  if (missing.length) throw new Error(`Workload has no successful ${missing.join(", ")} evidence in phase ${phase}`);
  if (Date.now() - latestEnd > 25_000) throw new Error(`Workload evidence for phase ${phase} is stale`);
  return counts;
}

export function createPassword() {
  const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#%_-";
  while (true) {
    const value = [...randomBytes(32)].map((byte) => alphabet[byte % alphabet.length]).join("");
    if (/[a-z]/.test(value) && /[A-Z]/.test(value) && /\d/.test(value)) return value;
  }
}

export function workloadEnvironment(env, connection, runId, phaseFile, stopFile) {
  const result = { ...env };
  for (const key of ["ATLAS_CLIENT_ID", "ATLAS_CLIENT_SECRET", "ATLAS_RESULTS_URI", "AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING"]) {
    delete result[key];
  }
  return {
    ...result, ATLAS_CONNECTION_STRING: connection,
    RESILIENCY_RUN_ID: runId, RESILIENCY_PHASE_FILE: phaseFile, RESILIENCY_STOP_FILE: stopFile
  };
}

async function waitForChild(child, timeoutMs) {
  if (child.exitCode !== null) return child.exitCode;
  let timer;
  try {
    return await Promise.race([
      new Promise((resolvePromise) => child.once("exit", resolvePromise)),
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Child process did not exit within ${timeoutMs} ms`)), timeoutMs);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv, { "validate-only": { type: "boolean" } });
  if (args._.length !== 1) throw new Error("Usage: orchestrate.js <manifest> [--validate-only]");
  const here = dirname(fileURLToPath(import.meta.url));
  const manifestPath = resolve(args._[0]);
  const manifest = await readJson(manifestPath);
  validateComputeTransition(manifest.experiment?.initialComputeTier, manifest.experiment?.targetComputeTier);
  validateAtlasManifest(manifest, { approved: !args["validate-only"] });
  if (args["validate-only"]) {
    console.log("PASS: local Atlas manifest validation; no API calls or cloud changes");
    return 0;
  }
  const experiment = manifest.experiment;
  const atlasClient = new AtlasClient();
  const runId = manifest.runId;
  const results = resolve(here, "results", runId);
  await mkdir(results, { recursive: true });
  const events = new Events(resolve(results, "control-plane.jsonl"), runId);
  await events.open();
  const phaseFile = resolve(results, "phase.txt");
  const stopFile = resolve(results, "stop");
  const operationsFile = resolve(results, "operations.jsonl");
  const password = createPassword();
  const started = performance.now();
  let workload = null;
  let currentControlPlaneOperation = null;
  const provisioningFailures = [];
  try {
    await events.write("run_start");
    currentControlPlaneOperation = "provisioning";
    let resource;
    const attempts = [];
    for (let attempt = 1; attempt <= 4; attempt += 1) {
      const attemptStart = performance.now();
      await events.write("provision_attempt_start", { attempt });
      try {
        resource = await atlasClient.provision(manifest, manifestPath, password, events, () => stopping);
        const record = { attempt, success: true, elapsed_seconds: round((performance.now() - attemptStart) / 1000) };
        attempts.push(record);
        await events.write("provision_attempt_end", record);
        break;
      } catch (error) {
        provisioningFailures.push(error);
        const record = {
          attempt, success: false, elapsed_seconds: round((performance.now() - attemptStart) / 1000),
          error_category: error.name, error_code: error.code ?? error.name
        };
        attempts.push(record);
        await events.write("provision_attempt_end", { ...record, retry: attempt < 4 && !stopping });
        if (attempt === 4 || stopping) throw error;
        await sleep([2000, 5000, 10_000][attempt - 1]);
      }
    }
    manifest.status = "cluster-ready";
    manifest.provisioningAttempts = attempts;
    await atomicJson(manifestPath, manifest);
    if (provisioningFailures.length) {
      await recordControlPlaneFailure(events, "provisioning", provisioningFailures.at(-1), {
        retryCount: attempts.length - 1, finalOutcome: "succeeded"
      });
    }
    const connection = atlasClient.connectionString(resource, manifest, password);
    currentControlPlaneOperation = null;
    if (await exists(stopFile)) await unlink(stopFile);
    if (await exists(operationsFile)) await rename(operationsFile, resolve(results, `operations-abandoned-${Date.now()}.jsonl`));
    await atomicText(phaseFile, "baseline");
    const threadCount = workloadThreads(experiment.initialComputeTier);
    workload = spawn(process.execPath, [
      resolve(here, "workload.js"), "--duration-seconds", "7200",
      "--operations-per-second", String(experiment.operationsPerSecondCeiling),
      "--max-retries", String(experiment.maxDataPlaneRetries),
      "--timeout-ms", String(experiment.timeoutSeconds * 1000),
      "--threads", String(threadCount), "--output", operationsFile
    ], {
      env: workloadEnvironment(process.env, connection, runId, phaseFile, stopFile),
      stdio: "ignore", windowsHide: true
    });
    let workloadSpawnError = null;
    workload.once("error", (error) => { workloadSpawnError = error; });
    const workloadStopped = () => workloadSpawnError || workload.exitCode !== null || workload.signalCode !== null;
    await events.write("baseline_start", { workload_threads: threadCount });
    let deadline = performance.now() + experiment.baselineSeconds * 1000;
    while (!stopping && performance.now() < deadline && !workloadStopped()) await sleep(1000);
    if (stopping || workloadStopped()) throw workloadSpawnError ?? new Error("Workload stopped during baseline");
    await events.write("baseline_validated", { successful_operations: await workloadHealth(operationsFile, "baseline", 3) });
    await atomicText(phaseFile, "scale-up");
    const scaleBody = await atlasClient.scaleBody(manifest);
    const scaleStarted = performance.now();
    await events.write("scale_submitted", { target_tier: experiment.targetComputeTier });
    currentControlPlaneOperation = "scaling";
    await atlasClient.request("PATCH", manifest.plannedResourceIds[0], { body: scaleBody });
    await atlasClient.waitCluster(manifest, events, "scale-up", experiment.targetComputeTier, workload, () => stopping);
    const scaleElapsed = round((performance.now() - scaleStarted) / 1000);
    await events.write("scale_terminal", {
      success: true, elapsed_seconds: scaleElapsed, provisioning_state: "Succeeded",
      cluster_status: "Ready", tier: experiment.targetComputeTier
    });
    currentControlPlaneOperation = null;
    await atomicText(phaseFile, "post-scale");
    await events.write("post_scale_start", { observation_seconds: experiment.postScaleSeconds });
    deadline = performance.now() + experiment.postScaleSeconds * 1000;
    while (!stopping && performance.now() < deadline && !workloadStopped()) await sleep(1000);
    if (stopping || workloadStopped()) throw workloadSpawnError ?? new Error("Workload stopped during post-scale observation");
    await writeFile(stopFile, "");
    await waitForChild(workload, 15_000);
    await runNode(resolve(here, "summarize.js"), [operationsFile], { cwd: resolve(here, "..") });
    const summary = await readJson(resolve(results, "summary.json"));
    const postScale = summary.phases?.["post-scale"] ?? {};
    const failures = Number(postScale.failed ?? 0);
    Object.assign(manifest, {
      scaleDurationSeconds: scaleElapsed, scaleSucceeded: true, scaleTerminalObserved: true,
      totalDurationSeconds: round((performance.now() - started) / 1000), postScaleFailures: failures,
      status: failures ? "completed-with-data-plane-errors-awaiting-cleanup-confirmation" : "completed-awaiting-cleanup-confirmation"
    });
    await atomicJson(manifestPath, manifest);
    await events.write("post_scale_observed", {
      attempted: postScale.attempted ?? 0, succeeded: postScale.succeeded ?? 0, failed: failures
    });
    await events.write("run_complete", { scale_elapsed_seconds: scaleElapsed, post_scale_failures: failures });
    return 0;
  } catch (error) {
    if (currentControlPlaneOperation) {
      await recordControlPlaneFailure(events, currentControlPlaneOperation, error, {
        retryCount: currentControlPlaneOperation === "provisioning"
          ? Math.max(0, provisioningFailures.length - (provisioningFailures.at(-1) === error ? 1 : 0)) : 0,
        finalOutcome: "failed"
      });
    }
    await events.write("run_failed", { error_category: error.name });
    manifest.status = "failed-awaiting-inspection";
    await atomicJson(manifestPath, manifest);
    throw error;
  } finally {
    if (workload?.exitCode === null) {
      await writeFile(stopFile, "");
      try { await waitForChild(workload, 15_000); }
      catch { workload.kill("SIGTERM"); await waitForChild(workload, 5000); }
    }
    await events.close();
  }
}

if (isMain(import.meta.url)) {
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopping = true; });
  process.exitCode = await main();
}
