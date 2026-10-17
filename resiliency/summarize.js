import { readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { isMain, parseArgs, round } from "./common.js";

export function loadOperationsText(text) {
  const operations = [];
  text.split(/\r?\n/).forEach((line, index) => {
    if (!line) return;
    let event;
    try {
      event = JSON.parse(line);
    } catch (error) {
      throw new Error(`Invalid JSONL at line ${index + 1}: ${error.message}`);
    }
    if (event.kind === "data_plane_operation") operations.push(event);
  });
  return operations;
}

export function disruptionIntervals(events) {
  const intervals = [];
  let outageStart = null;
  let outageEnd = null;
  let failures = 0;
  for (const event of [...events].sort((a, b) => a.started_utc.localeCompare(b.started_utc))) {
    const started = Date.parse(event.started_utc);
    const ended = Date.parse(event.ended_utc);
    if (!event.success) {
      outageStart ??= started;
      outageEnd = ended;
      failures += 1;
    } else if (outageStart !== null) {
      intervals.push({
        start_utc: new Date(outageStart).toISOString(),
        end_utc: new Date(ended).toISOString(),
        duration_seconds: round((ended - outageStart) / 1000),
        failed_operations: failures
      });
      outageStart = null;
      outageEnd = null;
      failures = 0;
    }
  }
  if (outageStart !== null && outageEnd !== null) {
    intervals.push({
      start_utc: new Date(outageStart).toISOString(),
      end_utc: new Date(outageEnd).toISOString(),
      duration_seconds: round((outageEnd - outageStart) / 1000),
      failed_operations: failures
    });
  }
  return intervals;
}

export function phaseSummary(events) {
  const attempted = events.length;
  const succeeded = events.filter((event) => event.success).length;
  const failed = attempted - succeeded;
  const retryAttempts = events.reduce((sum, event) => sum + Number(event.retry_count ?? 0), 0);
  let elapsed = 0;
  if (events.length) {
    let started = Infinity;
    let ended = -Infinity;
    for (const event of events) {
      started = Math.min(started, Date.parse(event.started_utc));
      ended = Math.max(ended, Date.parse(event.ended_utc));
    }
    elapsed = Math.max(0, (ended - started) / 1000);
  }
  const disruptions = disruptionIntervals(events);
  const disrupted = disruptions.reduce((sum, interval) => sum + interval.duration_seconds, 0);
  return {
    attempted,
    succeeded,
    failed,
    retried_operations: events.filter((event) => Number(event.retry_count ?? 0) > 0).length,
    retry_attempts: retryAttempts,
    success_rate_percent: attempted ? round(100 * succeeded / attempted) : null,
    failure_rate_percent: attempted ? round(100 * failed / attempted) : null,
    observed_seconds: round(elapsed),
    disrupted_seconds: round(disrupted),
    available_seconds: round(Math.max(0, elapsed - disrupted)),
    availability_time_percent: elapsed ? round(100 * Math.max(0, elapsed - disrupted) / elapsed) : null,
    disruptions
  };
}

function groupBy(values, selector) {
  const groups = new Map();
  for (const value of values) {
    const key = selector(value);
    const group = groups.get(key);
    if (group) group.push(value);
    else groups.set(key, [value]);
  }
  return groups;
}

export function buildSummary(operations) {
  const phases = {};
  for (const [phase, events] of [...groupBy(operations, (event) => String(event.phase ?? "unknown"))]
    .sort(([left], [right]) => left.localeCompare(right))) {
    const operationStats = {};
    for (const [operation, items] of [...groupBy(events, (event) => String(event.operation ?? "unknown"))]
      .sort(([left], [right]) => left.localeCompare(right))) {
      operationStats[operation] = phaseSummary(items);
    }
    const errors = {};
    for (const event of events.filter((event) => !event.success)) {
      const name = String(event.error_type ?? "UnknownError");
      errors[name] = (errors[name] ?? 0) + 1;
    }
    phases[phase] = { ...phaseSummary(events), operations: operationStats, errors };
  }
  return {
    schema_version: 1,
    operations: phaseSummary(operations),
    phases,
    availability_metric: phases["scale-up"] ?? null
  };
}

export function printSummary(summary) {
  const overall = summary.operations;
  console.log("\nResiliency test summary\n=======================");
  console.log(`Overall operations: ${overall.attempted} attempted, ${overall.succeeded} succeeded, ${overall.failed} failed (${overall.success_rate_percent}% success)`);
  console.log("\nStatistics by phase and operation");
  for (const [phase, stats] of Object.entries(summary.phases)) {
    console.log(`${phase}: ${stats.attempted} attempted, ${stats.succeeded} succeeded, ${stats.failed} failed, ${stats.retry_attempts} retries (${stats.success_rate_percent}% success)`);
    for (const [operation, operationStats] of Object.entries(stats.operations)) {
      console.log(`  ${operation}: ${operationStats.attempted} attempted, ${operationStats.succeeded} succeeded, ${operationStats.failed} failed (${operationStats.success_rate_percent}% success)`);
    }
    if (Object.keys(stats.errors).length) {
      console.log(`  Errors: ${Object.entries(stats.errors).map(([name, count]) => `${name}=${count}`).join(", ")}`);
    }
  }
  const scale = summary.availability_metric;
  console.log("\nAvailability metric (compute scale-up)");
  if (!scale) {
    console.log("INCONCLUSIVE: no operations labeled with phase 'scale-up'.");
    return;
  }
  console.log(`Operations: ${scale.attempted} attempted, ${scale.succeeded} succeeded, ${scale.failed} failed`);
  console.log(`Operation availability: ${scale.success_rate_percent}% success; ${scale.failure_rate_percent}% failure`);
  console.log(`Time availability: ${scale.available_seconds}s available of ${scale.observed_seconds}s observed (${scale.availability_time_percent}%); ${scale.disrupted_seconds}s disrupted`);
  console.log(`Disruption windows: ${scale.disruptions.length}`);
}

export async function main(argv = process.argv.slice(2)) {
  const args = parseArgs(argv, { "json-output": { type: "string" } });
  if (args._.length !== 1) throw new Error("Usage: summarize.js <events> [--json-output path]");
  const events = resolve(args._[0]);
  const summary = buildSummary(loadOperationsText(await readFile(events, "utf8")));
  const output = resolve(args["json-output"] ?? dirname(events), args["json-output"] ? "" : "summary.json");
  await writeFile(output, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  printSummary(summary);
  console.log(`Machine-readable summary: ${output}`);
  return 0;
}

if (isMain(import.meta.url)) process.exitCode = await main();
