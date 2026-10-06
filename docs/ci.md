# CI policy

Pull requests and supported branch pushes enter `HugeGraph-Server CI`. The workflow
always reports `affected-module-tests`; the required `check-license` runs independently. Module
workflows are reusable and can also be started manually.

| Changed inputs | Required Linux coverage |
| --- | --- |
| Any non-document Server module input, including RocksDB and HStore | Memory, RocksDB, PD/Store/HStore and Cluster |
| Server distribution or shared startup scripts | Server, PD/Store/HStore, Docker and Cluster |
| Commons or Struct | Their tests and affected Server, PD, Store, HStore and Cluster tests |
| PD or Store distribution inputs (`hg-pd-dist/`, `hg-store-dist/`), including scripts, configs, assembly descriptors and POMs | PD/Store/HStore, Cluster and Docker |
| Other PD or Store inputs | PD/Store/HStore suite and Cluster |
| Cluster, Docker or Helm | The corresponding suite; Docker includes Compose render/smoke and startup contracts |
| Central `server-ci.yml` | All Linux suites controlled by the caller |
| A module workflow | Its suite and known downstream suites |
| Dependencies, shared build inputs or unknown paths | Conservative full coverage |

The selector follows dependency edges. PD, Store, HStore and Struct share one suite
in this first stage. Selected tests must succeed; failure, cancellation or an
unexpected skip cannot satisfy the gate. Startup prerequisites are enforced.

Third-party dependency inventory and vulnerability review retain their existing
non-blocking policy. They run when selected, but are excluded from the core gate
and current-run test results. Their failure does not prevent affected module tests or
post-gate checks from reporting results; the required license check is unchanged.

The Docker suite builds and loads the four production images from the current
checkout on one Linux runner, sharing their Maven build through BuildKit Bake.
It verifies healthcheck presence and Java contracts, then starts standalone Server
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
A PR containing only allowed documentation changes can omit unaffected modules.
A source PR that later receives a documentation update still selects its cumulative
source changes and runs the necessary module tests, compatibility and security
checks on every update. Cross-run success reuse is paused: an artifact's claimed
merge SHA, parents and job IDs do not independently prove the original execution tree.
A trustworthy execution-proof design is separate work.

The plan and current-run results are saved as `ci-plan` and `ci-test-results`
artifacts. Results describe only this run's successful selected suites; they are
not authentication evidence and never authorize a later run to skip tests.

PR planning records the event's head, base and source before querying live metadata.
A known mismatch with the checkout merge or current PR fails planning; expanding
coverage cannot make an outdated checkout current. After all selected jobs and
fixtures succeed, the gate rechecks the open PR's head, base and source using a
read-only API request. Changed inputs or unavailable metadata fail the gate.
Refresh the branch and start a new PR event after head or base movement: GitHub
reruns retain the original commit and event, so rerunning alone cannot refresh the
base. An unknown API or selection failure falls back to full coverage; a manual
full rerun can recover a transient failure while the recorded inputs remain current.

## Protection migration

`.asf.yaml` requests `check-license` and `affected-module-tests`. Temporary memory
and Java analysis aliases preserve the previous protection names. Leave
`CI_OPTIMIZED_REQUIRED` unset until the live branch protection uses the new gate;
CodeQL continues scanning every update during this transition. Setting the
repository variable to `true` then permits unchanged documentation updates to
avoid redundant security scanning. Enabling the variable is a separate rollout
operation.

Automatic retry is limited to failed pushes. It verifies the run attempt, source
repository and current branch head before and after its existing wait; at most two
automatic reruns are allowed. Only failed jobs are retried. PR automatic retry is
paused because it relied on the PR's CI plan artifact to attest the tested base.
For a transient PR failure with unchanged inputs, use a manual full rerun so shared
build and fixture artifacts are rebuilt together with their consumers. A retry retains its separate concurrency
group and cannot cancel a newer PR run.
