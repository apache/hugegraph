# CI policy

Pull requests and supported branch pushes enter `HugeGraph-Server CI`. The workflow
always reports `affected-module-tests`; license checks run independently. Module
workflows are reusable and can also be started manually.

| Changed inputs | Required Linux coverage |
| --- | --- |
| Server private implementation | Memory, RocksDB and Cluster |
| Server core, API or shared tests | Memory, RocksDB, HStore and Cluster |
| HStore adapter | PD/Store/HStore suite and Cluster |
| Distribution or shared startup scripts | Server, PD/Store/HStore, Docker and Cluster |
| Commons or Struct | Their tests and affected Server, PD, Store, HStore and Cluster tests |
| PD or Store | PD/Store/HStore suite and Cluster |
| Cluster, Docker or Helm | The corresponding suite |
| A module workflow | Its suite and known downstream suites |
| Dependencies, shared build inputs or unknown paths | Conservative full coverage |

The selector follows dependency edges. PD, Store, HStore and Struct share one suite
in this first stage. Selected tests must succeed; failure, cancellation or an
unexpected skip cannot satisfy the gate. Startup prerequisites are enforced.

HBase, macOS, RISC-V and CodeQL run after the core gate. Their results remain visible,
but they are outside `affected-module-tests`. This orders work within a PR; it does
not grant runner priority over other PRs. Existing scheduled CodeQL scanning remains.

## Documentation updates

Only explicitly allowed prose and static documentation assets qualify. Source,
types, tests, dependencies and CI configuration never qualify as documentation.
An unchanged test input can reuse a receipt from the same open PR only when its
base, source repository and CI policy match. GitHub job records must prove the
selected suites completed successfully. The current commit gets a fresh gate and
summary identifying executed and reused suites. Missing or unverifiable evidence
causes the tests to run again.

The plan and successful receipt are saved as `ci-plan` and `ci-test-receipt`
artifacts. They are evidence, not credentials; each reused proof is checked against
GitHub rather than trusted solely because an artifact exists.

## Protection migration

`.asf.yaml` requests `check-license` and `affected-module-tests`. Temporary memory
and Java analysis aliases preserve the previous protection names. Leave
`CI_OPTIMIZED_REQUIRED` unset until the live branch protection uses the new gate;
CodeQL continues scanning every update during this transition. Setting the
repository variable to `true` then permits unchanged documentation updates to
avoid redundant security scanning. Enabling the variable is a separate rollout
operation.

Automatic retry verifies the run and PR/branch before and after its existing wait.
A moved PR base or missing immutable plan also prevents an obsolete retry.
A retry has a separate concurrency group, so retrying an old commit cannot cancel
the newest PR run. Only failed jobs are retried.
