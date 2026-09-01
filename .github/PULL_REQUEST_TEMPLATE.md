<!-- 
  Thank you very much for contributing to Apache HugeGraph, we are happy that you want to help us improve it!

  Here are some tips for you:
    1. If this is your first time, please read the [contributing guidelines](https://github.com/apache/hugegraph/blob/master/docs/CONTRIBUTING.md)

    2. If a PR fix/close an issue, type the message "close xxx" (xxx is the link of related 
issue) in the content, GitHub will auto link it (Required)

    3. Name the PR title in "Google Commit Format", start with "feat | fix | perf | refactor | doc | chore", 
      such like: "feat(core): support the PageRank algorithm" or "fix: wrong break in the compute loop" (module is optional)
      skip it if you are unsure about which is the best component.

    4. One PR address one issue, better not to mix up multiple issues.

    5. Put an `x` in the `[ ]` to mark the item as CHECKED. `[x]` (or click it directly after 
published)
-->

## Purpose of the PR

- close #xxx <!-- or use "fix #xxx", "xxx" is the ID-link of related issue, e.g: close #1024 -->

<!--
Please explain more context in this section, clarify why the changes are needed. 
e.g:
- If you propose a new API, clarify the use case for a new API.
- If you fix a bug, you can clarify why it is a bug, and should be associated with an issue.
-->

## Main Changes

<!-- Please clarify what changes you are proposing. The purpose of this section is to outline the changes and how this PR fixes the issue. These change logs are helpful for better and faster reviews.)

For example:

- If you introduce a new feature, please show detailed design here or add the link of design documentation.
- If you refactor some codes with changing classes, showing the class hierarchy will help reviewers.
- If there is a discussion in the mailing list, please add the link. -->

## Verifying these changes

<!-- Please pick the proper options below -->

- [ ] Trivial rework / code cleanup without any test coverage. (No Need)
- [ ] Already covered by existing tests, such as *(please modify tests here)*.
- [ ] Need tests and can be verified as follows:
    - xxx

## Does this PR potentially affect the following parts?

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

- [ ]  Dependencies ([add/update license](https://hugegraph.apache.org/docs/contribution-guidelines/contribute/#321-check-licenses) info & [regenerate_known_dependencies.sh](../install-dist/scripts/dependency/regenerate_known_dependencies.sh)) <!-- Don't forget to add/update the info in "LICENSE" & "NOTICE" files (both in root & dist module) -->
- [ ]  Modify configurations
- [ ]  The public API
- [ ]  Other affects (typed here)
- [ ]  Nope


## Documentation Status

<!-- DO NOT REMOVE THIS SECTION. CHECK THE PROPER BOX ONLY. -->

- [ ]  `Doc - TODO` <!-- Your PR changes impact docs and you will update later -->
- [ ]  `Doc - Done` <!-- Related docs have been already added or updated -->
- [ ]  `Doc - No Need` <!-- Your PR changes don't impact/need docs -->
