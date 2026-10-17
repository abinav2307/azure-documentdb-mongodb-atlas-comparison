# Project Structure

- Project type: Node.js CLI toolkit.
- Runtime entry points: nine scripts under `resiliency/`.
- Shared layer: `common.js`, `driver-info.js`, and `sku-config.js`.
- Data layer: `workload.js` accesses the experiment cluster; `result-sink.js` accesses `resiliencyResultsDB.runs`.
- Control-plane layer: `orchestrate.js`, `cleanup-run.js`, and `diagnose-deployment.js` invoke Azure CLI as an authenticated ARM REST transport.
- Reporting layer: `summarize.js`, `finalize-run.js`, and `validate-run-document.js`.
- Campaign layer: `campaign.js` and `campaign-coordinator.js`.
- Configuration: `package.json`, `.vscode/tasks.json`, `resiliency/run-document.schema.json`, and `resiliency/infrastructure/main.json`.
