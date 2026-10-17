# Architecture Index

## Implementation Guide

This index is not the full contract. Do not implement from this file alone; follow the artifact paths below.

### Global artifacts

- `unit_graph.yaml`: find the assigned CLI unit, entry point, dependencies, and shared references.
- `migration_boundary.yaml`: use `must_rewrite` as implementation scope; source anchors are not rewrite targets.
- `wire_contracts.yaml`: filter rows where `unit` matches the assigned unit.
- `shared_modules.yaml`: filter rows where `used_by_units` contains the assigned unit.
- `cross_unit_state.yaml`: filter flows whose writer or reader matches the assigned unit.

### Units

The units are `campaign-cli`, `campaign-coordinator-cli`, `orchestrator-cli`, `workload-cli`, `cleanup-cli`, `diagnose-cli`, `finalizer-cli`, `summarizer-cli`, and `validator-cli`.

For each assigned unit, read:

- `units/<unit>/behavior.yaml` for required side effects, branches, errors, and concurrency.
- `units/<unit>/bindings.yaml` for CLI and environment bindings.
- `units/<unit>/unit_decomposition.yaml` for advisory split candidates only.

Before DONE report: list artifacts read, implemented `must_preserve` items, unresolved or deferred contracts, and test/build/runtime evidence.
