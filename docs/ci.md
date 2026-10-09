# CI policy

CI selects tests from the cumulative pull-request diff and known consumer dependencies.
Selection controls what runs; branch protection controls what must pass before merging.

| Workflow | Purpose | Merge requirement |
| --- | --- | --- |
| License Checker | License headers and RAT | `check-license` |
| Server Memory CI | Memory unit, core and API tests on the project runtime | `Server memory tests` |
| HugeGraph-Server CI | Related modules, current images and dependency audits | Advisory |
| CodeQL | Source analysis, independently selected and scheduled | Advisory |
| Server Compatibility CI | HBase, macOS, RISC-V and published-image Compose smoke | Advisory |

Each workflow can be cancelled independently. Jobs within a workflow share its cancellation
scope. Cancelling advisory checks does not cancel Memory or license checks, and does not
prevent CodeQL from starting. Failed advisory checks retain their real failure result
(except the existing RISC-V job, which still uses `continue-on-error`);
maintainers decide whether they need another run or a requested change.

## Affected inputs

| Changed inputs | Selected validation |
| --- | --- |
| Server, including RocksDB and HStore | Memory, RocksDB, PD/Store/HStore and Cluster |
| Server distribution or shared startup scripts | Server, PD/Store/HStore, Docker and Cluster |
| Commons or Struct | Their tests and affected Server, PD, Store, HStore and Cluster tests |
| PD or Store distribution scripts, config, assembly or POM | PD/Store/HStore, Cluster and Docker |
| Other PD or Store inputs | PD/Store/HStore and Cluster |
| Server parent/API POM or Commons version resource | Normal module coverage plus Docker artifact checks |
| Cluster, Docker or Helm | Their suite and known consumers |
| PR template, Dependabot configuration or retry checker/tests | Planner checks and license checks; no product suites |
| Docker image/deployment checkers and their tests | Docker |
| Dependencies, shared build inputs or unknown paths | Conservative full coverage |

The Memory required result depends only on its planner, runtime preparation and real Memory
matrix. A selected test that fails, is cancelled or unexpectedly skipped cannot pass it.
When Memory is not selected, the result explains that no Memory test was needed.
The advisory `affected-module-tests` summary lists selection, required status and actual results.
It does not stand in for Memory or control CodeQL and compatibility checks.

The Docker suite builds the current four production images through a shared Maven build.
Checks consume those final images, verify identity, health, version and authentication, and
exercise graph writes, reads and Gremlin queries on standalone and PD/Store/Server topologies. Published-image Hubble Compose smoke runs once in
compatibility CI when RocksDB or Docker is selected; it does not verify the new PR images.

HBase compatibility uses the existing HBase version in one standalone container with local
storage. Its image is pinned by digest; no separate HDFS service or CI image build is needed.
Pull, startup and readiness preparation have a five-minute total budget. Preparation failure
ends the advisory check without falling back to a tar download. Existing HBase behavior tests
remain. This changes CI preparation, not product support or backend deprecation policy.

TinkerPop suites run only when the source, target or push/manual branch starts with
`release-`, `test-` or `tinkerpop-`. PRs run no TP suites when neither source nor target
branch matches these prefixes, including `upgrade/1.8.0` targeting `master`.
Historical task-branch exceptions and the temporary TP skip are removed. Executed TP suites
still require non-empty reports with actual executed tests.

PD, Store and Commons have total job budgets of 30, 45 and 20 minutes, respectively,
based on six recent successful master runs across Org and ASF. Cluster has 120 minutes,
preserving its two existing 45-minute test windows and allowing build/diagnostic time.
Server and HStore budgets remain unchanged pending complete TP execution history.

## Documentation and freshness

Only explicitly allowed prose and static documentation assets qualify as plain documentation.
Packaged resources, configuration, source, tests and CI inputs never qualify. Documentation-only
PRs skip compilation, backend services, images and PR CodeQL; lightweight selection and license
checks still report. A source PR with a later documentation commit still tests its cumulative
source changes. No result is reused from a previous run.

Plans record the target branch name, event head and the actual merge commit and base tested. Checkout must match
the event merge SHA, whose second parent must match the current PR head. Its first parent is
the tested base; the event base may lag when master advances before checkout. A new source
head or a change to the target branch name invalidates the old run. The tested base must
remain an ancestor of the current target; a target force-push that removes that ancestry
invalidates planning and final gates. Normal target-branch advancement remains accepted,
matching non-strict branch protection. An ancestry comparison uses the repository API so
commits added after checkout do not need to exist locally. A selection/API failure conservatively selects
all suites. A final metadata outage alone cannot invalidate completed Memory tests.
Plans and actual results are diagnostics, not execution credentials for later runs.

## Protection and retries

`.asf.yaml` requests only `check-license` and `Server memory tests`. Keep these required check
names stable. The Memory workflow runs real unit, core and API tests on the project runtime
and reports their results through `Server memory tests`; the former Java 11 placeholder is
removed. The former `build-commons (11)` placeholder is also removed; real Commons tests
remain advisory and install only the Commons reactor and its upstream modules.
`affected-module-tests` and CodeQL remain advisory. Verify live branch protection
when changing required checks; do not forge a successful result or leave retired contexts required.

Only failed push runs of License Checker and Server Memory CI automatically retry, at most
twice. The trusted checker verifies the workflow path, attempt, repository and unchanged branch
head before and after the delay. PRs and advisory workflows never automatically retry.
Use a manual rerun when appropriate; a rerun retains its original commit and event.

## Follow-up work

These changes need separate CI policy decisions or broader validation:

- Split compatibility selection by backend, native code, JDK and shared startup/core impact,
  retaining Server/PD/Store/Cluster coverage and periodic complete compatibility checks.
- Narrow PD/Store/HStore/Cluster reactors only after verifying every required distribution;
  evaluate sharing artifacts between jobs separately.
- Choose an explicit RocksDB comparison baseline for the first push to a new release-/test-
  branch, where `before` is all zeros. Do not silently compare a commit with itself.
- Revisit RISC-V `continue-on-error`; it remains advisory and is not a required check.
- Consider merging runtime resolution into planning and extracting long diagnostic scripts.
- Replace broad push failure reruns with bounded retries at known transient operations;
  preserve workflow, repository, attempt and unchanged-head checks.
- Set total timeouts only where complete test history supports a budget, including diagnostics.
  Do not size HStore or Server TP budgets from runs that skipped TP suites.
