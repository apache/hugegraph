# CI policy

Pull requests and supported branch pushes enter `HugeGraph-Server CI`. The workflow
always reports `affected-module-tests`; the required `check-license` runs independently. Module
workflows are reusable and can also be started manually.

| Changed inputs | Required Linux coverage |
| --- | --- |
| Any non-document Server module input, including RocksDB and HStore | Memory, RocksDB, PD/Store/HStore and Cluster |
| Distribution or shared startup scripts | Server, PD/Store/HStore, Docker and Cluster |
| Commons or Struct | Their tests and affected Server, PD, Store, HStore and Cluster tests |
| PD or Store | PD/Store/HStore suite and Cluster |
| Cluster, Docker or Helm | The corresponding suite; Docker includes Compose render/smoke and startup contracts |
| Store port utility | PD/Store/HStore, Cluster and Docker startup contracts |
| Central `server-ci.yml` | All Linux suites controlled by the caller |
| A module workflow | Its suite and known downstream suites |
| Dependencies, shared build inputs or unknown paths | Conservative full coverage |

The selector follows dependency edges. PD, Store, HStore and Struct share one suite
in this first stage. Selected tests must succeed; failure, cancellation or an
unexpected skip cannot satisfy the gate. Startup prerequisites are enforced.

Third-party dependency inventory and vulnerability review retain their existing
non-blocking policy. They run when selected, but are excluded from the core gate
and test-reuse receipts. Their failure does not prevent affected module tests or
post-gate checks from reporting results; the required license check is unchanged.

The Docker suite builds and loads the four production images from the current
checkout on one Linux runner, sharing their Maven build through BuildKit Bake.
It verifies image healthcheck and Java contracts, then starts standalone Server
and the PD/Store/Server topology with unique run tags and pulling disabled.
Runtime checks match each container's image ID to the build, require healthy
services and validate Server version and authenticated graph-list responses.
PD must report readiness and a registered Store; unauthenticated graph and PD
metadata access must be rejected. The existing Hubble Compose smoke
remains a separate compatibility check; it does not verify the new PR images.

HBase, macOS, RISC-V and CodeQL run after the core gate. Their results remain visible,
but they are outside `affected-module-tests`. Post-gate conditions explicitly
require a successful plan and gate, including when unrelated modules were skipped. This orders work within a PR; it does
not grant runner priority over other PRs. Existing scheduled CodeQL scanning remains.

## Documentation updates

Only explicitly allowed prose and static documentation assets qualify. Source,
types, tests, dependencies and CI configuration never qualify as documentation.
An unchanged test input can reuse a receipt from the same open PR only when its
base, source repository and CI policy match. GitHub job records must prove the
selected suites completed successfully. Receipt lookup is restricted to the PR
branch and bounded pages; repository and PR provenance are still checked. The current commit gets a fresh gate and
summary identifying executed and reused suites. Missing or unverifiable evidence
causes the tests to run again.
For consecutive documentation updates, optional-check proofs retain their original
run and exact job identities and are reverified before forwarding. A newer failed
check or unverifiable evidence prevents reuse of an older optional success.

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
