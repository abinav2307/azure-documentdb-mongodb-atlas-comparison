# Existing Data Model

## Collections

- `resiliencyTestDB.resiliencyCollection`: transient workload documents with `_id`, `run_id`, `workload_attempt_id`, `sequence`, `created_utc`, and a padding `payload`; each encoded document is exactly 1 KiB.
- `resiliencyResultsDB.runs`: aggregate run documents keyed by string `_id`; the exact shape is governed by `resiliency/run-document.schema.json`.

## Relationships and transactions

- There are no ORM entities or collection relationships.
- A run result refers to Azure resources by identifiers stored in the same aggregate document.
- Writes are individual MongoDB operations; no multi-document transaction is used.
