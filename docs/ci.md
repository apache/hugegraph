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
prevent CodeQL from starting. Failed advisory checks retain their real failure result;
maintainers decide whether they need another run or a requested change.

## Affected inputs

| Changed inputs | Selected validation |
| --- | --- |
| Server, including RocksDB and HStore | Memory, RocksDB, PD/Store/HStore and Cluster |
| Server distribution or shared startup scripts | Server, PD/Store/HStore, Docker and Cluster |
| Commons or Struct | Their tests and affected Server, PD, Store, HStore and Cluster tests |
| PD or Store distribution scripts, config, assembly or POM | PD/Store/HStore, Cluster and Docker |
| Other PD or Store inputs | PD/Store/HStore and Cluster |
| Server API POM or Commons version resource | Normal module coverage plus Docker artifact checks |
| Cluster, Docker or Helm | Their suite and known consumers |
| Dependencies, shared build inputs or unknown paths | Conservative full coverage |

The Memory required result depends only on its planner, runtime preparation and real Memory
matrix. A selected test that fails, is cancelled or unexpectedly skipped cannot pass it.
When Memory is not selected, the result explains that no Memory test was needed.
The advisory `affected-module-tests` summary lists selection, required status and actual results.
It does not stand in for Memory or control CodeQL and compatibility checks.

The Docker suite builds the current four production images through a shared Maven build.
Checks consume those final images, verify identity, health, version and authentication, and
exercise the PD/Store/Server topology. Published-image Hubble Compose smoke runs once in
compatibility CI when RocksDB or Docker is selected; it does not verify the new PR images.

HBase compatibility uses the existing HBase version in one standalone container with local
storage. Its image is pinned by digest; no separate HDFS service or CI image build is needed.
Pull, startup and readiness preparation have a five-minute total budget. Preparation failure
ends the advisory check without falling back to a tar download. Existing HBase behavior tests
remain. This changes CI preparation, not product support or backend deprecation policy.

## Documentation and freshness

Only explicitly allowed prose and static documentation assets qualify as plain documentation.
Packaged resources, configuration, source, tests and CI inputs never qualify. Documentation-only
PRs skip compilation, backend services, images and PR CodeQL; lightweight selection and license
checks still report. A source PR with a later documentation commit still tests its cumulative
source changes. No result is reused from a previous run.

Plans record the event head, tested base and merge. Checkout and PR source identities must
match the event. A new source head invalidates the old run; target-branch advancement alone
does not, matching non-strict branch protection. A selection/API failure conservatively selects
all suites. A final metadata outage alone cannot invalidate completed Memory tests.
Plans and actual results are diagnostics, not execution credentials for later runs.

## Protection migration and retries

`.asf.yaml` requests only `check-license` and `Server memory tests`. Keep existing check names
through migration and verify live branch protection after merging. The migration PR may need
one complete run under the old requirements. Do not remove a still-required context or forge
success for it. Remove obsolete `affected-module-tests`/CodeQL requirements from live protection
before relying on documentation skips or independently cancelling advisory workflows.

Only failed push runs of License Checker and Server Memory CI automatically retry, at most
twice. The trusted checker verifies the workflow path, attempt, repository and unchanged branch
head before and after the delay. PRs and advisory workflows never automatically retry.
Use a manual rerun when appropriate; a rerun retains its original commit and event.
