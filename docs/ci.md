# CI policy

[HugeGraph-Server CI](../.github/workflows/server-ci.yml) is the single entry point for
PRs, supported branch pushes, weekly coverage and manual validation. Its preflight job
selects affected tests, reads the project Java version, checks CI policy and verifies
license headers and RAT. Selected suites run through reusable workflows in the same run.

CI results become advisory after [`.asf.yaml`](../.asf.yaml) is applied. It requests no required CI contexts
and retains one approving review. This protection change takes effect after the configuration
is merged and synchronized by Gitbox; verify live protection then. Failed tests keep their
real failed results. Valid selection and Java outputs allow selected tests to run even when
the preflight license checks fail.

Preflight keeps its real `check-license` name. The temporary `Server memory tests`
protection bridge has been removed after confirming both ASF and org master required
contexts are empty. Actual backend jobs are named `Server memory tests (Java ...)`
and `Server rocksdb tests (Java ...)`; their native checks report real execution.

## Coverage with selective tests

Each coverage upload has a stable test-group flag. The [Codecov configuration](../.github/codecov.yml)
enables [carryforward](https://docs.codecov.com/docs/carryforward-flags). When Codecov can
find an earlier flagged report in its recorded parent chain, it preserves that historical
report for unexecuted groups such as Commons, PD/Store and backends.
The PR comment explicitly marks carried-forward data. It is historical coverage,
not evidence that a skipped test ran; native job failures remain failures.

New flags need a complete initial baseline. The existing weekly full run supplies every
flagged group for that commit. Master pushes remain smoke-only and upload no coverage;
those commits can break Codecov's recorded parent chain. A fresh selective PR based on
such a commit is not guaranteed to inherit the weekly baseline, and its project report
may still be incomplete. Carryforward within an existing reported PR chain does not
prove this works for a fresh PR. Full and selected runs retain explicit report paths and
uploads; no extra collector runner or repeated master coverage tests are added.

## What runs

| Validation | Ordinary PR | Push to master | Weekly full run |
| --- | --- | --- | --- |
| Memory and RocksDB | Full unit, core and API tests when affected | No repeated full tests | Full tests |
| macOS ARM | Startup, JNI, graph read/write and restart smoke when affected; full core/API for platform inputs | Affected smoke | Full core/API |
| HBase and RISC-V | Directly related backend/platform or validation inputs only | No repeated tests | Full validation |
| PD, Store, HStore and Cluster | Affected modules and known consumers | Current-commit distributed smoke | Full group |
| CodeQL | Authentication, authorization, security, schema cache transactions, POM or CodeQL configuration inputs | No repeated scan | Full scan |
| Docker, Helm and dependency audit | Corresponding inputs | Corresponding deployment smoke or checks | Full validation |

macOS uses one `macos-15` ARM job for either smoke or full validation. Intel macOS is removed.
The weekly run uses `cron: '0 6 * * 1'` with `timezone: Asia/Singapore`: Monday at 06:00,
on the latest default-branch commit. GitHub may delay scheduled runs during high load;
see [schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

### Affected inputs

The rules live in [ci-policy.py](../.github/scripts/ci-policy.py). Ordinary Server changes
select both Memory and RocksDB, plus ARM smoke. They do not select the complete PD/Store group.
PD and Store changes select their own tests and downstream consumers; Commons and Struct
select their known shared consumers. HStore and Cluster additionally follow distributed
backend contracts, protocols, metadata, graph lifecycle and scheduling inputs. Distribution,
startup and RocksDB inputs select ARM full validation and their deployment consumers.

Unclassified paths or an unavailable diff/API conservatively select mainline coverage.
HBase and RISC-V remain limited to explicit related inputs, weekly coverage or manual selection.
The classifier uses paths, not PR titles or semantic interpretation of changed text.
Contract scripts explicitly select their executing suites without adding runtime consumers.

### Documentation

Documentation-only ordinary PRs and pushes allocate just the preflight CI runner. This count
excludes the non-test [Auto PR Commenter](../.github/workflows/auto-pr-review.yml), which runs
once when a PR opens. Scheduled and manual selections still run their requested suites.

Plain documentation includes regular-file root prose, README/AGENTS files and supported prose
or static images under `docs/`, including nested module documentation. Runtime resources,
configuration, source, fixtures, templates and executable or symbolic-link files do not qualify;
for example, a test fixture ending in `.txt` still selects its module's tests.

## Manual validation and reruns

Use **Actions → HugeGraph-Server CI → Run workflow**, select a branch and choose `test-set`:

| Test set | Validation |
| --- | --- |
| `mainline` (default) | Full mainline modules, deployment/dependency checks and CodeQL; ARM smoke |
| `distributed` | Struct, PD, Store, HStore and Cluster, plus distributed smoke |
| `macos-arm` | ARM smoke and full RocksDB core/API tests |
| `hbase`, `riscv64`, `codeql` | The corresponding complete specialist validation |
| `full` | All retained suites, identical to weekly coverage |

Fork contributors run additional validation in their fork's Actions. The manual entry point
does not accept a PR number, apply labels or reuse another workflow's results.
Automatic retries are disabled. Use GitHub's native rerun controls for failed jobs or a run;
a rerun retains its original commit and event, subject to the preflight freshness checks.

## Java runtime compatibility

A positive suite passes only when its tests succeed and its current-run output has no
Java runtime compatibility errors. Server, PD/Store, Commons, Struct and Cluster test
steps capture both stdout and stderr before running `check-java-runtime-logs.sh`.
Startup and Console smoke tests also inspect their output before cleanup. Cluster nodes
forward their text runtime logs to the captured suite output before deletion.
Missing or unreadable required logs fail the check.

The gate rejects denied module access and unknown or removed JVM options, including
exceptions caught and printed by dependencies. Ordinary business errors and deliberate
negative fixtures are not compatibility failures. Native smoke additionally uses `--native`
for its existing native linkage checks.

Launchers and tests share each component's `jvm-module.options`: PD and Store open
`java.lang` for Hessian, while Console and dump-store reuse Server's existing options.
Required module options are independent of custom `JAVA_OPTIONS` or `-j` flags; Surefire
retains the JaCoCo agent. CI continues to use the project Java runtime and its existing
suite-selection and required-check policy.


## Freshness, runner use and budgets

PR selection uses the cumulative diff against the checked-out merge commit's base. A source
PR followed by a documentation-only commit still tests the cumulative source changes.
Preflight verifies that the event merge commit's second parent matches the current PR head.
A stale PR head fails preflight. Target-branch advancement alone does not invalidate a run.
Plans and selection summaries describe this run; previous results are never reused.

A new PR head cancels the previous PR run, including its called workflows. Manual and weekly
runs remain independent. Each job reports its own result; there are no separate post-test
aggregation runners or end-of-run metadata checks.

Startup contracts run inside RocksDB validation, and Store stop contracts run inside Struct.
Docker's static checks, deployment contracts and current-image validation share one runner.
The Docker job has a 90-minute total budget, with 25 minutes for deployment contracts and
45 minutes for image build/runtime validation. Distributed smoke builds current PD, Store
and Server images and checks authentication, readiness and graph read/write. It does not
substitute published images for the current commit.

PD, Store and Commons have total job budgets of 30, 45 and 20 minutes. Cluster has 120 minutes
for its two 45-minute test windows and build/diagnostics. Other budgets are in the workflows.
TinkerPop suites keep their dedicated branch policy: the source, target or push/manual branch
must start with `release-`, `test-` or `tinkerpop-`. Executed suites require non-empty reports
with actual tests.

Each separate job or matrix item requests a runner; steps within a job share one.
[Standard hosted runners are free for public repositories](https://docs.github.com/en/actions/reference/runners/github-hosted-runners#standard-github-hosted-runners-for-public-repositories),
but [plan-dependent total and macOS concurrency limits](https://docs.github.com/en/actions/reference/limits#job-concurrency-limits-for-github-hosted-runners)
still apply. Reducing job allocations and long executions lowers shared-pool demand;
it does not guarantee a specific queue delay.

## Dependency baseline

The separate [dependency submission workflow](../.github/workflows/submit-dependencies.yml)
installs the complete reactor once with Java 17 in a `contents: read` job, reusing
`actions/setup-java`'s Maven cache, then generates
each module's resolved graph with Ferstl 4.0.3. The repository's
[stdlib serializer](../.github/scripts/dependency-snapshot.py) preserves the
[official Maven parser 6.0.1](https://github.com/actions/maven-dependency-submission-action/tree/1f666f7ac2ab7b72cd5e22eaf7f23c8a581db1db)
package URLs and relationships. Non-test scopes take precedence when Ferstl merges
scopes onto one artifact; only test-only artifacts are development dependencies.
Every tracked POM must have a graph; empty aggregator manifests are allowed, but a
completely empty inventory fails. It runs on master pushes or manual
dispatches. Only `apache/hugegraph` (not a fork) and the explicitly allowed
`hugegraph/hugegraph` test fork on `refs/heads/master` can run the submission job,
which alone has `contents: write`. PRs, other repositories and other refs cannot
run the writer. Maven inherits no submission token, and checkout does not persist
credentials. The same read-only job separately resolves all module dependency
scopes with Apache Maven Dependency Plugin 3.8.1 `tree` JSON, sharing the installed
reactor while keeping the two resolvers and JSON reports independent. Its
[stdlib inventory adapter](../.github/scripts/dependency-inventory.py) records each POM's
package URLs, scopes and direct/indirect relationships independently of the Ferstl
parser. The writer requires exact equality with that inventory, including empty
aggregators; a graph retaining one direct package while omitting another fails.
The global runtime/shipped jar filename inventory has a different scope and does
not replace this per-module check.

A pinned `actions/upload-artifact` transfers `snapshot.json` and `inventory.json`
as separate artifacts. The fresh writer runner uses fixed inline stdlib Python to
verify the successful producer job and both artifact/run/attempt/SHA identities
before downloading. It bounds ZIP and JSON sizes, checks each archive digest,
and reads only its one expected regular JSON member in memory. Duplicate members,
paths and symlinks fail; no archive members are extracted to disk. A signed storage
redirect is fetched without forwarding GitHub Authorization, and further redirects
are refused. The writer does not check out the repository, run Maven, or execute
downloaded code. The first-party upload action is permitted by the
[ASF Actions policy](https://infra.apache.org/github-actions-policy.html).

Checkout and snapshot use the event commit SHA. Master is checked before install
and again after both reports are generated; a stale run fails. A newer master
run cancels the older master run because only the current master can submit.
Concurrency groups include the ref, so manual runs on other refs cannot cancel master. Every master
push still starts a baseline run without path filters, so unrelated changes also
can receive an exact-SHA baseline. The writer
checks the run attempt, successful producer, artifact ID/name, JSON schema,
detector/correlator and source SHA/ref. It verifies all manifest paths against the
event commit's source tree, rejects empty or mismatched inventory, and checks master
immediately before and after submission. Snapshots retain the checked event SHA
if master advances during submission; that run fails its final freshness check.

If submission fails, use **Re-run all jobs** or start a fresh `workflow_dispatch`
on master. **Re-run failed jobs** keeps the older generation artifact, which the
writer rejects because both reports must be produced in the same run attempt.

If a PR `dependency-review` failure reports 0 base snapshots, rerun that job only
once the same repository's **Dependency baseline** run has succeeded for the event's
exact `pull_request.base.sha` (not the branch's merge base or a newer master tip).
If that SHA has no successful baseline, including a skipped or superseded run,
rerunning the review alone cannot supply it. The producer supports only the current
master tip; cancelling an older run does not backfill its earlier PR events.

To recover, wait until the current master has a successful baseline in the same
repository. Normally merge that master into the PR branch (or use Update branch)
and push the update to create a new PR event. Confirm that the new event's
`pull_request.base.sha` is the SHA with the successful baseline, then use that
new dependency-review run. Rerunning the old job retains its old event payload
and cannot move its base SHA to the submitted baseline.

A fixed master-only correlator keeps successive baseline snapshots in the
same stream within each repository; ASF and org inventories are independent. The writer's
identity is separate from automatic PR submissions so they cannot replace its baseline.
The [snapshot API](https://docs.github.com/en/rest/dependency-graph/dependency-submission#create-a-snapshot-of-dependencies-for-a-repository)
requires Contents write permission. The writer also has Actions read permission
to verify run and artifact metadata. Snapshot metadata identifies the repository
adapter rather than claiming that the combined generation/submission action ran.

This producer supplements the existing dependency review and shipped-inventory
checks without changing their severity, license rules or inventory. Merging the
workflow does not prove that either repository has a baseline: verify each repository's
first master submission, including its SHA/ref, non-empty module manifests and a real
dependency-review comparison against that baseline. Until then, that repository's baseline
acceptance remains open. This workflow changes no repository settings.

Dependency snapshot producer and submission-workflow changes select dependency checks. Snapshot fixture tests run in the entry job; they do not select runtime suites. Changes to the CI classifier still receive conservative mainline validation.
