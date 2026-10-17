# MongoDB Atlas resiliency campaign

This repository runs bounded, strictly sequential **MongoDB Atlas on Azure**
resiliency experiments. Each run creates a fresh cluster and scoped database
user, continuously measures database availability during compute scale-up,
persists verified aggregate results, and deletes only the resources it created.
Atlas is the only supported backend. No cloud-management CLI, subscription
credentials, infrastructure deployment templates, or database MCP server is needed.

## Prerequisites

- Node.js 20.19 or newer and dependencies installed with `npm ci`.
- A dedicated Atlas project with billing enabled.
- An Atlas service account assigned Project Owner or equivalent permissions to
  create, inspect, update and delete clusters and database users in that project.
  Legacy public/private API keys are not supported.
- API access restrictions and the database IP access list permitting the campaign
  machine's public egress IP. Prefer a `/32` entry. The campaign does not modify
  project network access lists.
- A separate, long-lived Atlas result sink, never a disposable campaign cluster.
  Its database user needs read/write access to `resiliencyResultsDB.atlasRuns`.
  The collection is created automatically on the first result insert. Existing
  data in other collections is not moved or modified. Resuming a campaign whose
  results were written to a different collection requires those results to be
  migrated to `atlasRuns` first; no automatic migration is performed.

## Credentials

Only process environment variables and masked terminal prompts accept secrets:

| Variable | Purpose |
|---|---|
| `ATLAS_PROJECT_ID` | Non-secret 24-character project ID, not organization ID |
| `ATLAS_CLIENT_ID` | Service account client ID |
| `ATLAS_CLIENT_SECRET` | Service account client secret |
| `ATLAS_RESULTS_URI` | Long-lived result-sink MongoDB connection string |
| `ATLAS_CONNECTION_STRING` | Workload URI generated internally per run; needed only for standalone workload use |

`--prompt-atlas-credentials` prompts for missing client credentials and
`--prompt-results-uri` prompts for a missing result URI. Do not paste credentials
into chat or put them in command arguments or files.

The campaign creates a fresh `readWrite` user for `resiliencyTestDB`, scoped to
the new cluster, and generates its password in memory. OAuth tokens are cached
and regenerated before expiry. Workload children receive the cluster URI but not
service-account credentials, result-sink URIs or email secrets. The coordinator
removes the result URI from its environment after reading it.

Control-plane failure notifications retain the independent Azure Communication
Services email transport. Configure `AZURE_COMMUNICATION_EMAIL_CONNECTION_STRING`
and `AZURE_COMMUNICATION_EMAIL_FROM_ADDRESS` to send notifications to
`abramees@microsoft.com`. This is an email service, not a database backend.
Missing email configuration or delivery failure is explicitly recorded without
hiding the original Atlas failure.

## Run a campaign

In PowerShell:

```powershell
$env:ATLAS_PROJECT_ID = "<atlas-project-id>"
node .\resiliency\campaign.js --iterations 1 --fixed-profile --prompt-atlas-credentials --prompt-results-uri
```

The fixed profile uses East US 2 (`US_EAST_2`), M10 to M20, and 32 GB per node.
The exact plan is displayed before requesting an approval token. To generate a
plan without credentials, approval or cloud changes:

```powershell
node .\resiliency\campaign.js --iterations 1 --fixed-profile --prepare-only
```

Set the non-secret project ID first, even for plan-only runs. To run the
randomized campaign, omit `--fixed-profile` and choose `--iterations` from 1
through 200; the default is 200. `--provider atlas` remains accepted but is
unnecessary; no other provider is supported.

VS Code tasks prefixed **Atlas Resiliency:** offer 1, 5, 10, 30, 45, 100 and 200 runs
and resume. They include `--accept-billable-charges`, which explicitly authorizes
the displayed plan without an interactive approval token, and masked credential
prompts. Set `ATLAS_PROJECT_ID` in VS Code's inherited environment before using
these tasks.

To prepare the default 200-run campaign without starting it:

```powershell
node .\resiliency\campaign.js --prepare-only
```

To execute a new 200-run campaign with interactive approval:

```powershell
node .\resiliency\campaign.js --iterations 200 --prompt-atlas-credentials --prompt-results-uri
```

Runs remain strictly sequential, with cleanup after each run. Resuming an
existing plan preserves its original run count; it does not expand it to 200.

### Tier and storage policy

| Initial compute tier | Allowed target compute tiers | Storage per node | Workload threads |
|---|---|---|---|
| M10 | M20 | 32 GB | 1 |
| M20 | M30 | 32 GB | 1 |

Every cluster is a **three-node replica set** with one shard. Compute and disk
autoscaling are disabled. Only `eastus2` is allowed, mapped to Atlas
`US_EAST_2`. No fallback to another region is permitted. Scaling is exactly one tier up;
disk size remains 32 GB per node. The selected region, tier, disk size and node
count are checked after provisioning and scale-up.

Atlas tier/storage limits and regional capacity can reject a planned
configuration. Such runs fail explicitly and are reported as inconclusive, never silently
substituted with another tier or storage size. Start with the fixed profile.

Prepare a new plan for this policy. Previously prepared plans with other tiers,
storage sizes or regions are rejected for execution rather than rewritten.
Historical files and persisted results are not modified. The manual cleanup
command still accepts approved, ownership-verified legacy Atlas manifests so
these policy restrictions do not prevent deletion of older run-owned resources.

## Workload and measurements

Each worker continuously performs an acknowledged insert, point read and ping.
Each complete inserted BSON document is exactly 1 KiB, and only the collection's
normal `_id` index is used. Traffic is unthrottled by default. Read and write
driver retries and application retries are disabled; timeouts are five seconds.
Operation failures are recorded locally and do not stop subsequent cycles.

The run phases are:

1. `baseline` for 120 seconds, followed by verification of successful insert,
   point-read and health evidence.
2. `scale-up` from immediately before scale submission until Atlas reports
   `IDLE` with the exact target configuration. The same workload must remain
   running while control-plane polling continues independently of data failures.
3. `post-scale` for the full 120 seconds, even if operations continue failing.
4. Stop the workload cleanly, aggregate results, persist and verify them, then
   delete the scoped database user and cluster and verify both are absent.

Results contain operation and time availability, per-phase and per-thread
metrics, availability dips, UTC-aligned request-volume-per-minute buckets
(including zero-volume minutes), driver metadata, Atlas project identity,
control-plane timing and failures, and a developer-facing experience report.
Atlas request IDs are retained as provider-specific strings.
Raw data-plane exceptions are not copied into persisted results.
Each root and per-thread availability dip includes `errorCategoryCounts`, a map
from error category to failed-operation count, alongside `errorCategories`.
For example, `"errorCategoryCounts": { "NetworkTimeout": 10, "NotPrimaryError": 15 }`
accounts for 25 failed operations. Counts sum to the dip's `failedOperations`;
missing error names are counted under `UnknownError`. Empty observation-gap dips
with no failed operations have an empty map `{}`. The bucketing, recovery and
duration calculations are unchanged. Previously persisted results are not
backfilled; recreating counts requires the raw operation evidence.
Every planned, finalized and inconclusive result document includes the required
root-level field `product` with the constant value `"MongoDB Atlas"`.

## Resume and cleanup

```powershell
node .\resiliency\campaign.js --resume-latest --prompt-atlas-credentials --prompt-results-uri
```

Only current schema-version-3 Atlas plans are selected. An explicit
`--resume-plan <path>` is also accepted. Completed results are verified and
skipped; interrupted experiments are finalized as inconclusive and cleaned up,
not restarted as new scale tests. Cleanup-pending runs remain recoverable.

State and locking use `atlas-campaign-state.json` and `atlas-campaign.lock`.
Plans, manifests and evidence use `campaigns`, `runs` and `results` beneath
this folder. The original historical plans, results, checkpoints, campaign state,
lock record and migration analyses are preserved as archives. They are not
selected for new Atlas execution or rewritten into Atlas records. Their cloud
resources have not been inspected or deleted by this source cleanup.

Cleanup validates the approved project, exact resource paths, cluster tags,
database-user scope and ownership records. It deletes neither the project nor
existing unrelated resources. The result sink must remain available for exact
readback verification. Final persistence retries for up to 120 seconds; raw
operation evidence is deleted only after a verified final write and successful
cleanup. It is retained if either fails.

Manual Atlas cleanup:

```powershell
node .\resiliency\cleanup-run.js <manifest-path> --evidence <evidence-path>
```

### Cleanup after every run

The coordinator always attempts cleanup of the run-owned cluster and scoped
database user after success or failure, including failures in result persistence.
There is no keep-cluster option. Newly generated plans use
`cleanupPolicy: "always-delete"`. Legacy Atlas retention plans remain readable,
but their retention metadata no longer skips cleanup. Resuming a formerly
retained run cleans up its resources rather than treating it as finished.

Cleanup errors remain explicit and recoverable; successful deletion is reported
only after absence is verified. Raw evidence is preserved when cleanup or final
persistence fails. Billing continues while resources remain; `expiresAt` is
informational, not an automatic deletion timer. Existing retained resources are
not deleted merely by updating this code: resume their campaign or use the
manual cleanup command above.

For standalone workload execution, set `ATLAS_CONNECTION_STRING` and
`RESILIENCY_RUN_ID`; optional `RESILIENCY_PHASE_FILE` and `RESILIENCY_STOP_FILE`
control phase labels and shutdown. Use the workload's flags for duration,
operation rate, timeouts, concurrency, database, collection and output.
`node .\resiliency\orchestrate.js <manifest-path> --validate-only` performs only
local manifest validation; it does not prove deployability or contact Atlas.

## Local validation

```powershell
npm test
npm run check
```

Tests include mocked Atlas creation/scaling/deletion, ownership protections,
ambiguous-create recovery, credential handling, tier limits, result persistence
and workload metrics. Passing local tests is not a live Atlas provisioning test.

API references:
[service-account authentication](https://www.mongodb.com/docs/atlas/api/api-authentication/)
and [cluster creation](https://www.mongodb.com/docs/api/doc/atlas-admin-api-v2/operation/operation-creategroupcluster).
Cluster requests use media type `2024-08-05`, with per-node `diskSizeGB`;
database-user requests use `2023-01-01`.
