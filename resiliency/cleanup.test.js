import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import test from "node:test";
import { approve, main as campaign, prepare } from "./campaign.js";
import { executeManifests, recoverFailedRun, runOne } from "./campaign-coordinator.js";
import { buildExperienceReport } from "./finalize-run.js";

const PROJECT = "0123456789abcdef01234567";

async function setup() {
  const root = await mkdtemp(resolve(tmpdir(), "atlas-cleanup-"));
  const schemaPath = resolve(root, "resiliency", "run-document.schema.json");
  await mkdir(dirname(schemaPath), { recursive: true });
  await writeFile(schemaPath, await readFile(new URL("./run-document.schema.json", import.meta.url)));
  const planPath = await prepare(root, 1, () => ["eastus2", "M10", "M20"], { atlasProjectId: PROJECT });
  const plan = JSON.parse(await readFile(planPath, "utf8"));
  const manifestPath = resolve(root, plan.manifests[0]);
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  manifest.approvals.executionApproved = true;
  manifest.approvals.cleanupPreapproved = true;
  manifest.ownedResourceIds = [...manifest.plannedResourceIds];
  manifest.status = "completed-awaiting-cleanup-confirmation";
  await writeFile(manifestPath, JSON.stringify(manifest));
  const resultDirectory = resolve(root, "resiliency", "results", manifest.runId);
  await mkdir(resultDirectory, { recursive: true });
  await writeFile(resolve(resultDirectory, "operations.jsonl"), "");
  const planned = JSON.parse(await readFile(resolve(dirname(manifestPath), "planned-run-document.json"), "utf8"));
  let cleanupCalls = 0;
  const cleanupResources = async (path, evidencePath) => {
    assert.equal(path, manifestPath);
    cleanupCalls += 1;
    const now = new Date().toISOString();
    const evidence = {
      runId: manifest.runId, atlasProjectId: PROJECT,
      cleanupSubmittedUtc: now, cleanupCompletedUtc: now, cleanupTimeSeconds: 0,
      verification: "Every recorded Atlas cluster and database user returned 404",
      deletedOnlyRecordedOwnedResources: true
    };
    await writeFile(evidencePath, JSON.stringify(evidence));
    return evidence;
  };
  const finalizeRun = async (_root, directory, _manifestPath, cleanup) => {
    const document = structuredClone(planned);
    if (cleanup) {
      document.controlPlaneTimeline.cleanupSubmittedUtc = cleanup.cleanupSubmittedUtc;
      document.controlPlaneTimeline.cleanupCompletedUtc = cleanup.cleanupCompletedUtc;
      document.cleanupTimeSeconds = 0;
      document.endTimeUTC = cleanup.cleanupCompletedUtc;
    }
    const path = resolve(directory, "final-run-document.json");
    await writeFile(path, JSON.stringify(document));
    return path;
  };
  return {
    root, schemaPath, planPath, manifestPath, manifest, planned, resultDirectory,
    statePath: resolve(root, "state.json"), cleanupResources, finalizeRun,
    cleanupCalls: () => cleanupCalls
  };
}

test("plans always require cleanup and the removed keep-cluster CLI option is rejected", async () => {
  const context = await setup();
  try {
    const plan = JSON.parse(await readFile(context.planPath, "utf8"));
    assert.equal(plan.cleanupPolicy, "always-delete");
    assert.equal(context.manifest.experiment.cleanupPolicy, "always-delete");
    await approve(context.root, context.planPath, { input: async (expected) => expected });
    const approved = JSON.parse(await readFile(context.manifestPath, "utf8"));
    assert.equal(approved.approvals.cleanupPreapproved, true);
    await assert.rejects(campaign(["--keep-cluster", "--prepare-only"], { root: context.root }),
      /Unknown option: --keep-cluster/);
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("success always cleans up, verifies final persistence, and resumes without repeating cleanup", async () => {
  for (const legacyRetained of [false, true]) {
    const context = await setup();
    try {
      if (legacyRetained) {
        context.manifest.experiment.cleanupPolicy = "retain";
        context.manifest.status = "completed-resources-retained";
        await writeFile(context.manifestPath, JSON.stringify(context.manifest));
      }
      const calls = [];
      const sink = {
        ensureResumeBase: async () => context.planned,
        replaceAndVerify: async (document) => {
          assert.equal(document.controlPlaneTimeline.cleanupCompletedUtc, null);
          calls.push("pre-cleanup");
        },
        replaceFinalAndVerify: async (document) => {
          assert.ok(document.controlPlaneTimeline.cleanupCompletedUtc);
          assert.equal(context.cleanupCalls(), 1);
          calls.push("final");
        },
        verify: async () => { calls.push("resume"); }
      };
      await runOne({ ...context, sink });
      const manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
      assert.equal(manifest.status, "completed-cleanup-confirmed");
      assert.ok(manifest.cleanup.cleanupCompletedUtc);
      await assert.rejects(readFile(resolve(context.resultDirectory, "operations.jsonl")), { code: "ENOENT" });
      await runOne({ ...context, sink, finalizeRun: async () => assert.fail("Completed run must not restart") });
      assert.deepEqual(calls, ["pre-cleanup", "final", "resume"]);
      assert.equal(context.cleanupCalls(), 1);
    } finally {
      await rm(context.root, { recursive: true, force: true });
    }
  }
});

test("failed runs always clean up, including legacy retained failures", async () => {
  for (const legacyRetained of [false, true]) {
    const context = await setup();
    try {
      if (legacyRetained) {
        context.manifest.experiment.cleanupPolicy = "retain";
        context.manifest.status = "failed-resources-retained";
        await writeFile(context.manifestPath, JSON.stringify(context.manifest));
      }
      let written;
      const result = await executeManifests({
        ...context, manifests: [context.manifestPath],
        sink: {
          ensureResumeBase: async () => { throw new Error("Scale failed"); },
          replaceFinalAndVerify: async (document) => { written = document; }
        }
      });
      assert.deepEqual(result, [[context.manifest.runId], []]);
      assert.equal(context.cleanupCalls(), 1);
      const manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
      assert.equal(manifest.status, "failed-cleanup-confirmed");
      assert.ok(written.controlPlaneTimeline.cleanupCompletedUtc);
      assert.match(written.reportMarkdown, /cleaned up successfully/);
      await assert.rejects(readFile(resolve(context.resultDirectory, "operations.jsonl")), { code: "ENOENT" });
    } finally {
      await rm(context.root, { recursive: true, force: true });
    }
  }
});

test("pre-cleanup persistence failure still triggers cleanup", async () => {
  const context = await setup();
  try {
    const result = await executeManifests({
      ...context, manifests: [context.manifestPath],
      sink: {
        ensureResumeBase: async () => context.planned,
        replaceAndVerify: async () => { throw new Error("Result write failed"); },
        replaceFinalAndVerify: async () => {}
      }
    });
    assert.deepEqual(result, [[context.manifest.runId], []]);
    assert.equal(context.cleanupCalls(), 1);
    const manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
    assert.equal(manifest.status, "failed-cleanup-confirmed");
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("final persistence failure never prevents cleanup and preserves raw evidence", async () => {
  const context = await setup();
  try {
    await assert.rejects(executeManifests({
      ...context, manifests: [context.manifestPath],
      sink: {
        ensureResumeBase: async () => context.planned,
        replaceAndVerify: async () => {},
        replaceFinalAndVerify: async () => { throw new Error("Final persistence failed"); }
      }
    }), /Final persistence failed/);
    assert.ok(context.cleanupCalls() >= 1);
    const cleanup = JSON.parse(await readFile(resolve(context.resultDirectory, "cleanup-evidence.json"), "utf8"));
    assert.ok(cleanup.cleanupCompletedUtc);
    assert.equal(await readFile(resolve(context.resultDirectory, "operations.jsonl"), "utf8"), "");
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("cleanup runs even if planned-result validation fails during recovery", async () => {
  const context = await setup();
  try {
    await writeFile(resolve(dirname(context.manifestPath), "planned-run-document.json"), "{}");
    await assert.rejects(recoverFailedRun({
      ...context, error: new Error("Run failed"),
      sink: { replaceFinalAndVerify: async () => assert.fail("Invalid document must not be persisted") }
    }), /Run-document validation failed/);
    assert.equal(context.cleanupCalls(), 1);
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("cleanup errors stay pending, preserve evidence and can be retried", async () => {
  const context = await setup();
  try {
    let written;
    const sink = { replaceFinalAndVerify: async (document) => { written = document; } };
    const verified = await recoverFailedRun({
      ...context, error: new Error("Scale failed"), sink,
      cleanupResources: async () => { throw new Error("Deletion failed"); }
    });
    assert.equal(verified, false);
    let manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
    assert.equal(manifest.status, "failed-cleanup-pending");
    assert.equal(written.controlPlaneTimeline.cleanupCompletedUtc, null);
    assert.match(written.reportMarkdown, /Cleanup was not yet verified/);
    assert.equal(await readFile(resolve(context.resultDirectory, "operations.jsonl"), "utf8"), "");
    assert.equal(await recoverFailedRun({ ...context, sink, error: new Error("Retry cleanup") }), true);
    manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
    assert.equal(manifest.status, "failed-cleanup-confirmed");
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("failures before resource ownership do not delete unrelated resources", async () => {
  const context = await setup();
  try {
    context.manifest.ownedResourceIds = [];
    await writeFile(context.manifestPath, JSON.stringify(context.manifest));
    await recoverFailedRun({
      ...context, error: new Error("Provision failed"),
      sink: { replaceFinalAndVerify: async () => {} }
    });
    assert.equal(context.cleanupCalls(), 0);
    const manifest = JSON.parse(await readFile(context.manifestPath, "utf8"));
    assert.equal(manifest.status, "failed-no-owned-resources");
  } finally {
    await rm(context.root, { recursive: true, force: true });
  }
});

test("reports never describe cleanup as intentionally skipped", () => {
  const report = buildExperienceReport({
    manifest: { runId: "test", experiment: { initialComputeTier: "M10", targetComputeTier: "M20", cleanupPolicy: "retain" } },
    aggregateMetrics: { allOperations: { attempted: 0, failed: 0 }, scalingInProgressPhase: {}, perThread: [] },
    provisioningSeconds: 0, scaleSeconds: 0, availabilityDips: [], cleanupComplete: false
  });
  assert.match(report, /Cleanup was not yet verified/);
  assert.doesNotMatch(report, /intentionally skipped/);
});
