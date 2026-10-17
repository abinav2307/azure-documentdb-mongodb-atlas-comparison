import { appendFile, mkdir, readFile, stat } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, resolve } from "node:path";
import { BSON, MongoClient, MongoError, MongoServerError } from "mongodb";
import { isMain, parseArgs, round, sleep, utcNow } from "./common.js";

const SAFE_RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$/;
let stopping = false;

export class WorkloadValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = "WorkloadValidationError";
  }
}

export function errorDetails(error) {
  const details = { error_type: error?.name ?? "Error" };
  if (Number.isInteger(error?.code) || typeof error?.code === "string") details.error_code = error.code;
  if (typeof error?.timeout === "boolean") details.timeout = error.timeout;
  return details;
}

export function createOneKibDocument(runId, sequence, attemptId = "default") {
  const document = {
    _id: `${runId}:${attemptId}:${sequence}`,
    run_id: runId,
    workload_attempt_id: attemptId,
    sequence,
    created_utc: utcNow(),
    payload: ""
  };
  const payloadBytes = 1024 - BSON.serialize(document).length;
  if (payloadBytes < 0) throw new Error("Document metadata exceeds the 1 KiB target");
  document.payload = "x".repeat(payloadBytes);
  if (BSON.serialize(document).length !== 1024) {
    throw new Error("Generated BSON document is not exactly 1 KiB");
  }
  return document;
}

export class JsonlWriter {
  constructor(path, runId) {
    this.path = path;
    this.runId = runId;
    this.sequence = 0;
    this.pending = Promise.resolve();
  }

  async open() {
    await mkdir(dirname(this.path), { recursive: true });
  }

  write(event) {
    const value = {
      ...event,
      event_sequence: this.sequence,
      runId: this.runId,
      _id: `${this.runId}:data:${this.sequence}`
    };
    this.sequence += 1;
    this.pending = this.pending.then(() => appendFile(this.path, `${JSON.stringify(value)}\n`, "utf8"));
    return this.pending;
  }

  async close() {
    await this.pending;
  }
}

async function currentPhase(path) {
  if (!path) return "workload";
  try {
    const value = (await readFile(path, "utf8")).trim();
    return value ? value.slice(0, 64) : "unknown";
  } catch {
    return "unknown";
  }
}

export async function runOperation({ writer, config, threadId, operation, logicalId, action }) {
  const operationId = randomUUID();
  const startedUtc = utcNow();
  const started = performance.now();
  for (let retry = 0; retry <= config.maxRetries; retry += 1) {
    try {
      await action();
      await writer.write({
        schema_version: 1, kind: "data_plane_operation", run_id: config.runId,
        thread_id: threadId, phase: await currentPhase(config.phaseFile), operation,
        operation_id: operationId, logical_id: logicalId, started_utc: startedUtc,
        ended_utc: utcNow(), latency_ms: round(performance.now() - started),
        success: true, retry_count: retry
      });
      return true;
    } catch (error) {
      if (!(error instanceof MongoError) && !(error instanceof WorkloadValidationError)) throw error;
      await writer.write({
        schema_version: 1, kind: "data_plane_retry", run_id: config.runId,
        thread_id: threadId, phase: await currentPhase(config.phaseFile), operation,
        operation_id: operationId, logical_id: logicalId, timestamp_utc: utcNow(),
        retry_count: retry, will_retry_same_operation: retry < config.maxRetries && !stopping,
        ...errorDetails(error)
      });
      if (retry < config.maxRetries && !stopping) {
        await sleep(Math.min(2000, 100 * 2 ** retry));
        continue;
      }
      await writer.write({
        schema_version: 1, kind: "data_plane_operation", run_id: config.runId,
        thread_id: threadId, phase: await currentPhase(config.phaseFile), operation,
        operation_id: operationId, logical_id: logicalId, started_utc: startedUtc,
        ended_utc: utcNow(), latency_ms: round(performance.now() - started),
        success: false, retry_count: retry, ...errorDetails(error)
      });
      return false;
    }
  }
  return false;
}

export function loadConfig(args, env = process.env) {
  const connectionString = env.ATLAS_CONNECTION_STRING ?? "";
  const runId = env.RESILIENCY_RUN_ID ?? "";
  if (!connectionString) throw new Error("ATLAS_CONNECTION_STRING is required");
  if (!SAFE_RUN_ID.test(runId)) throw new Error("RESILIENCY_RUN_ID must contain only letters, numbers, '.', '_' or '-'");
  if (args.durationSeconds <= 0 || args.operationsPerSecond < 0) throw new Error("duration must be positive and operations-per-second must be non-negative");
  if (args.maxRetries < 0 || args.maxRetries > 10) throw new Error("max-retries must be between 0 and 10");
  if (args.timeoutMs < 100 || args.timeoutMs > 60_000) throw new Error("timeout-ms must be between 100 and 60000");
  if (args.threads < 1 || args.threads > 32) throw new Error("threads must be between 1 and 32");
  return {
    connectionString,
    runId,
    durationSeconds: args.durationSeconds,
    operationsPerSecond: args.operationsPerSecond,
    maxRetries: args.maxRetries,
    timeoutMs: args.timeoutMs,
    threads: args.threads,
    database: args.database,
    collection: args.collection,
    output: resolve(args.output ?? "resiliency", args.output ? "" : `results/${runId}/operations.jsonl`),
    phaseFile: env.RESILIENCY_PHASE_FILE ? resolve(env.RESILIENCY_PHASE_FILE) : null,
    stopFile: env.RESILIENCY_STOP_FILE ? resolve(env.RESILIENCY_STOP_FILE) : null
  };
}

async function exists(path) {
  if (!path) return false;
  try { await stat(path); return true; } catch { return false; }
}

export async function main(argv = process.argv.slice(2)) {
  const parsed = parseArgs(argv, {
    "duration-seconds": { type: "number", default: 3600 },
    "operations-per-second": { type: "number", default: 0 },
    "max-retries": { type: "number", default: 0 },
    "timeout-ms": { type: "number", default: 5000 },
    threads: { type: "number", default: 1 },
    database: { type: "string", default: "resiliencyTestDB" },
    collection: { type: "string", default: "resiliencyCollection" },
    output: { type: "string" }
  });
  let config;
  try {
    config = loadConfig({
      durationSeconds: parsed["duration-seconds"],
      operationsPerSecond: parsed["operations-per-second"],
      maxRetries: parsed["max-retries"],
      timeoutMs: parsed["timeout-ms"],
      threads: parsed.threads,
      database: parsed.database,
      collection: parsed.collection,
      output: parsed.output
    });
  } catch (error) {
    console.error(`Configuration error: ${error.message}`);
    return 2;
  }
  for (const signal of ["SIGINT", "SIGTERM"]) process.on(signal, () => { stopping = true; });
  const writer = new JsonlWriter(config.output, config.runId);
  await writer.open();
  const client = new MongoClient(config.connectionString, {
    appName: `atlas-resiliency-${config.runId}`,
    serverSelectionTimeoutMS: config.timeoutMs,
    connectTimeoutMS: config.timeoutMs,
    socketTimeoutMS: config.timeoutMs,
    retryWrites: false,
    retryReads: false
  });
  const database = client.db(config.database);
  try {
    await database.createCollection(config.collection);
  } catch (error) {
    if (!(error instanceof MongoServerError) || ![48, 17399].includes(error.code)) throw error;
  }
  const collection = database.collection(config.collection);
  const interval = config.operationsPerSecond ? 1000 / config.operationsPerSecond : 0;
  const deadline = performance.now() + config.durationSeconds * 1000;
  const attemptId = randomUUID().replaceAll("-", "");
  let sequence = 0;
  let acknowledgedInserts = 0;
  await writer.write({
    schema_version: 1, kind: "workload_start", run_id: config.runId,
    workload_attempt_id: attemptId, timestamp_utc: utcNow(),
    duration_seconds: config.durationSeconds,
    request_pacing: interval ? "rate-limited" : "unthrottled",
    operations_per_second_ceiling: config.operationsPerSecond || null,
    threads: config.threads
  });
  const worker = async (threadId) => {
    while (!stopping && performance.now() < deadline && !await exists(config.stopFile)) {
      const cycleStarted = performance.now();
      const currentSequence = sequence++;
      const logicalId = `${config.runId}:${currentSequence}`;
      const document = createOneKibDocument(config.runId, currentSequence, attemptId);
      if (await runOperation({
        writer, config, threadId, operation: "insert", logicalId,
        action: () => collection.insertOne(document)
      })) acknowledgedInserts += 1;
      await runOperation({
        writer, config, threadId, operation: "point_read", logicalId,
        action: async () => {
          if (!await collection.findOne({ _id: document._id })) {
            throw new WorkloadValidationError("Acknowledged document was not found by point read");
          }
        }
      });
      await runOperation({
        writer, config, threadId, operation: "health", logicalId,
        action: () => database.command({ ping: 1 })
      });
      if (interval) await sleep(Math.max(0, interval - (performance.now() - cycleStarted)));
    }
  };
  try {
    await Promise.all(Array.from({ length: config.threads }, (_, index) => worker(index + 1)));
  } finally {
    let uniqueRecordsObserved = null;
    let integrityError = {};
    try {
      uniqueRecordsObserved = await collection.countDocuments({
        run_id: config.runId, workload_attempt_id: attemptId
      });
    } catch (error) {
      integrityError = errorDetails(error);
    }
    await writer.write({
      schema_version: 1, kind: "integrity_reconciliation", run_id: config.runId,
      workload_attempt_id: attemptId, timestamp_utc: utcNow(),
      acknowledged_logical_inserts: acknowledgedInserts,
      unique_records_observed: uniqueRecordsObserved,
      success: uniqueRecordsObserved === acknowledgedInserts, ...integrityError
    });
    await writer.write({
      schema_version: 1, kind: "workload_stop", run_id: config.runId,
      timestamp_utc: utcNow(), logical_operations_started: sequence, cancelled: stopping
    });
    await client.close();
    await writer.close();
  }
  console.log(`Evidence written to ${config.output}`);
  return 0;
}

if (isMain(import.meta.url)) process.exitCode = await main();
