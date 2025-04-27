<!--

Copyright 2022 DataCanvas

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

-->

# Contributing

Contributions are welcome and are greatly appreciated! Every
little bit helps, and credit will always be given.

## Table of Contents

- [Contributing](#contributing)
  - [Table of Contents](#table-of-contents)
  - [Types of Contributions](#types-of-contributions)
    - [Report Bug](#report-bug)
    - [Submit Ideas or Feature Requests](#submit-ideas-or-feature-requests)
    - [Fix Bugs](#fix-bugs)
    - [Implement Features](#implement-features)
    - [Improve Documentation](#improve-documentation)
    - [Ask Questions](#ask-questions)
  - [Pull Request Guidelines](#pull-request-guidelines)
    - [Protocol](#protocol)
      - [Authoring](#authoring)
      - [Reviewing](#reviewing)
      - [Merging](#merging)
      - [Post-merge Responsibility](#post-merge-responsibility)
  - [Managing Issues and PRs](#managing-issues-and-prs)
  - [Setup Local Environment for Development](#setup-local-environment-for-development)
    - [Documentation](#documentation)

## Types of Contributions

### Report Bug

The best way to report a bug is to file an issue on GitHub. Please include:

- Your operating system name and version.
- Dingo database version.
- Detailed steps to reproduce the bug.
- Any details about your local setup that might be helpful in troubleshooting.

### Submit Ideas or Feature Requests

The best way is to file an issue on GitHub:

- Explain in detail how it would work.
- Keep the scope as narrow as possible, to make it easier to implement.
- Remember that this is a volunteer-driven project, and that contributions are welcome :)

For large features or major changes to codebase, please create **DingoDB Improvement Proposal (DIP)**. See template from [DIP-0](https://github.com/dingodb/dingo/issues/1)

### Fix Bugs

Look through the GitHub issues. Issues tagged with `#bug` are
open to whoever wants to implement them.

### Implement Features

Look through the GitHub issues. Issues tagged with
`#feature` is open to whoever wants to implement it.

### Improve Documentation

DingoDB could always use better documentation,
whether as part of the official docs or even on the web as blog posts or
articles. See [Documentation](#documentation) for more details.

### Ask Questions

There is a dedicated on [Slack](https://dingodb.slack.com) or [Mailing list](mailto:dingodb@zetyun.com). Please use it when asking questions.

## Pull Request Guidelines

A philosophy we would like to strongly encourage is

> Before creating a PR, create an issue.

The purpose is to separate problem from possible solutions.

**Bug fixes:** If you’re only fixing a small bug, it’s fine to submit a pull request right away but we highly recommend to file an issue detailing what you’re fixing. This is helpful in case we don’t accept that specific fix but want to keep track of the issue. Please keep in mind that the project maintainers reserve the rights to accept or reject incoming PRs, so it is better to separate the issue and the code to fix it from each other. In some cases, project maintainers may request you to create a separate issue from PR before proceeding.

**Refactor:** For small refactors, it can be a standalone PR itself detailing what you are refactoring and why. If there are concerns, project maintainers may request you to create a `#DIP` for the PR before proceeding.

**Feature/Large changes:** If you intend to change the public API, or make any non-trivial changes to the implementation, we require you to file a new issue as `#DIP` (DingoDB Improvement Proposal). This lets us reach an agreement on your proposal before you put significant effort into it. You are welcome to submit a PR along with the DIP (sometimes necessary for demonstration), but we will not review/merge the code until the DIP is approved.

In general, small PRs are always easier to review than large PRs. The best practice is to break your work into smaller independent PRs and refer to the same issue. This will greatly reduce turnaround time.

If you wish to share your work which is not ready to merge yet, create a [Draft PR](https://github.blog/2019-02-14-introducing-draft-pull-requests/). This will enable maintainers and the CI runner to prioritize mature PR's.

Finally, never submit a PR that will put master branch in broken state. If the PR is part of multiple PRs to complete a large feature and cannot work on its own, you can create a feature branch and merge all related PRs into the feature branch before creating a PR from feature branch to master.

### Protocol

#### Authoring

- Fill in all sections of the PR template.
- Title the PR with one of the following semantic prefixes (inspired by [Karma](http://karma-runner.github.io/0.10/dev/git-commit-msg.html])):

  - `feat` (new feature)
  - `fix` (bug fix)
  - `docs` (changes to the documentation)
  - `style` (formatting, missing semi colons, etc; no application logic change)
  - `refactor` (refactoring code)
  - `test` (adding missing tests, refactoring tests; no application logic change)
  - `chore` (updating tasks etc; no application logic change)
  - `perf` (performance-related change)
  - `build` (build tooling, Docker configuration change)
  - `ci` (test runner, Github Actions workflow changes)
  - `other` (changes that don't correspond to the above -- should be rare!)
  - Examples:
    - `feat: export charts as ZIP files`
    - `perf(api): improve API info performance`
    - `fix(chart-api): cached-indicator always shows value is cached`

- Add prefix `[WIP]` to title if not ready for review (WIP = work-in-progress). We recommend creating a PR with `[WIP]` first and remove it once you have passed CI test and read through your code changes at least once.
- If you believe your PR contributes a potentially breaking change, put a `!` after the semantic prefix but before the colon in the PR title, like so: `feat!: Added foo functionality to bar`
- **Screenshots/GIFs:** Changes to user interface require before/after screenshots, or GIF for interactions
  - Recommended capture tools ([Kap](https://getkap.co/), [LICEcap](https://www.cockos.com/licecap/), [Skitch](https://download.cnet.com/Skitch/3000-13455_4-189876.html))
  - If no screenshot is provided, the committers will mark the PR with `need:screenshot` label and will not review until screenshot is provided.
- **Dependencies:** Be careful about adding new dependency and avoid unnecessary dependencies.
- **Tests:** The pull request should include tests, either as doctests, unit tests, or both. Make sure to resolve all errors and test failures.
- **Documentation:** If the pull request adds functionality, the docs should be updated as part of the same PR.
- **CI:** Reviewers will not review the code until all CI tests are passed. Sometimes there can be flaky tests. You can close and open PR to re-run CI test. Please report if the issue persists. After the CI fix has been deployed to `master`, please rebase your PR.
- **Code coverage:** Please ensure that code coverage does not decrease.
- Remove `[WIP]` when ready for review. Please note that it may be merged soon after approved so please make sure the PR is ready to merge and do not expect more time for post-approval edits.
- If the PR was not ready for review and inactive for > 30 days, we will close it due to inactivity. The author is welcome to re-open and update.

#### Reviewing

- Use constructive tone when writing reviews.
- If there are changes required, state clearly what needs to be done before the PR can be approved.
- If you are asked to update your pull request with some changes there's no need to create a new one. Push your changes to the same branch.
- The committers reserve the right to reject any PR and in some cases may request the author to file an issue.

#### Merging

- At least one approval is required for merging a PR.
- PR is usually left open for at least 24 hours before merging.
- After the PR is merged, [close the corresponding issue(s)](https://help.github.com/articles/closing-issues-using-keywords/).

#### Post-merge Responsibility

- Project maintainers may contact the PR author if new issues are introduced by the PR.
- Project maintainers may revert your changes if a critical issue is found, such as breaking master branch CI.

## Managing Issues and PRs

To handle issues and PRs that are coming in, committers read issues/PRs and flag them with labels to categorize and help contributors spot where to take actions, as contributors usually have different expertises.

Triaging goals

- **For issues:** Categorize, screen issues, flag required actions from authors.
- **For PRs:** Categorize, flag required actions from authors. If PR is ready for review, flag required actions from reviewers.

First, add **Category labels (a.k.a. hash labels)**. Every issue/PR must have one hash label (except spam entry). Labels that begin with `#` defines issue/PR type:

| Label           | for Issue                                                                                                                               | for PR                                                                                                                                            |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `#bug`          | Bug report                                                                                                                              | Bug fix                                                                                                                                           |
| `#code-quality` | Describe problem with code, architecture or productivity                                                                                | Refactor, tests, tooling                                                                                                                          |
| `#feature`      | New feature request                                                                                                                     | New feature implementation                                                                                                                        |
| `#refine`       | Propose improvement that does not provide new features and is also not a bug fix nor refactor, such as adjust padding, refine UI style. | Implementation of improvement that does not provide new features and is also not a bug fix nor refactor, such as adjust padding, refine UI style. |
| `#doc`          | Documentation                                                                                                                           | Documentation                                                                                                                                     |
| `#question`     | Troubleshooting: Installation, Running locally, Ask how to do something. Can be changed to `#bug` later.                                | N/A                                                                                                                                               |
| `#DIP`          | DingoDB Improvement Proposal                                                                                                           | N/A                                                                                                                                               |

Then add other types of labels as appropriate.

- **Need labels:** These labels have pattern `need:xxx`, which describe the work required to progress, such as `need:rebase`, `need:update`, `need:screenshot`.
- **Risk labels:** These labels have pattern `risk:xxx`, which describe the potential risk on adopting the work, such as `risk:db-migration`. The intention was to better understand the impact and create awareness for PRs that need more rigorous testing.
- **Status labels:** These labels describe the status (`abandoned`, `wontfix`, `cant-reproduce`, etc.) Issue/PRs that are rejected or closed without completion should have one or more status labels.
- **Version labels:** These have the pattern `vx.x` such as `v0.28`. Version labels on issues describe the version the bug was reported on. Version labels on PR describe the first release that will include the PR.

Committers may also update title to reflect the issue/PR content if the author-provided title is not descriptive enough.

If the PR passes CI tests and does not have any `need:` labels, it is ready for review, add label `review` and/or `design-review`.

If an issue/PR has been inactive for >=30 days, it will be closed. If it does not have any status label, add `inactive`.

## Setup Local Environment for Development

First, [fork the repository on GitHub](https://help.github.com/articles/about-forks/), then clone it. You can clone the main repository directly, but you won't be able to send pull requests.

```bash
git clone git@github.com:your-username/dingo.git
cd dingo
./gradlew build
```

### Documentation

The latest documentation and tutorial are available at https://dingodb.io/.
