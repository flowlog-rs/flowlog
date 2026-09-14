# Migrating to one development branch

This is the cutover checklist for replacing the `main-next` development /
`main` release split. The ongoing process is in [releases.md](releases.md).

## Reviewed scope

| Area | Migration |
| --- | --- |
| CI and fuzz | Pushes and PRs target `main`; CI retains `merge_group`. |
| Release-plz | Both jobs run on `main`; only Release PRs publish. Concurrency applies to PR updates, not publishing. |
| Compiler releases | Called after library publishing for the same release commit; retains the compiler changelog fix. |
| Renovate | Targets `main`; removes the integration-branch configuration overlay. |
| Renovate approval | Reapproves on pushes because `main` dismisses stale reviews. |
| Publish scripts | Removes the unused standalone crate publisher; retains compiler version lookup. |
| Contributor docs | README and AGENTS.md link to the single-branch process. CLAUDE.md links to AGENTS.md. |
| Scheduled mutation tests | Already use the default branch, `main`; no branch change needed. |
| Crate manifests, READMEs, examples, tests, setup scripts | No active `main-next` dependency found. |
| Documentation site | `flowlog-rs.github.io` already builds/deploys from its own `main`; the playground also checks out FlowLog `main`. Explain development versus tagged releases in the installation guide. |
| Benchmarks | `flowlog-bench` README and Makefile examples must compare a release tag or PR commit with `main`, replacing the `main-next` examples. Historical benchmark records retain their original branch and SHA. |
| GitHub settings | Move queue and complete CI requirements to `main`; retire the old integration-branch protections after PR migration. |
| Environments and integrations | The `copilot` environment has no branch restriction. There are no Actions variables or Pages deployment in this repository. The release webhook is event-based, with no branch setting. |

Companion PRs:

- [Documentation site #5](https://github.com/flowlog-rs/flowlog-rs.github.io/pull/5)
- [Benchmarks #16](https://github.com/flowlog-rs/flowlog-bench/pull/16)

An organization-wide code search found no other active `main-next`
references outside these repositories. No wiki Git repository was available
to inspect. Existing release tags, changelogs, and published artifacts
describe historical releases and do not need rewriting.

## Before merging the migration PR

1. Pause merges into `main-next` and inspect its latest head. At the audit,
   `main` was `fed42113fe920cf565e41ec0f596a95512431165` and `main-next` was
   `6aab19715f0e3b74df9f8a34823e70b4af2d92b6`. The migration PR includes
   the latter's compiler release-notes fix. Bring any later changes into
   the migration PR before retiring the branch.
2. Confirm the migration PR targets `main` and all CI checks pass, including
   the fixture shards and DCO. This migration changes no crate versions.
3. Prepare the settings below. GitHub settings are outside the Git tree;
   merging YAML files does not migrate them.

## Configure main

Keep `main` as the default branch. In Settings / Rules / Rulesets, update
the existing `Release` ruleset (ID `10977965`) and rename it `Main`:

- Continue targeting `refs/heads/main`, blocking deletion and force pushes,
  and requiring the existing Committers-team approval.
- Allow squash merging for this branch.
- Add the merge queue from `Develop` (ID `20595359`): merge method `SQUASH`,
  build concurrency 2, minimum group size 1, grouping `ALLGREEN`, check
  timeout 120 minutes, and minimum group wait 5 minutes.
- Set maximum PRs to merge to **1**, rather than the old value 5. A batched
  push ending in an ordinary PR could otherwise hide a Release PR from
  the publishing job's latest-commit check.
- Preserve admin enforcement and the current approval requirements.

Update the existing classic protection for `main` as well:

- Require all checks below, associating Actions checks with GitHub Actions
  and DCO with its own app. These names are job display names, not job IDs.
- With the merge queue enabled, disable the separate "Require branches to
  be up to date before merging" option. The queue validates the combined
  result against the current base.
- Keep stale-review dismissal, one required approval, conversation
  resolution, admin enforcement, and force-push/deletion restrictions.

Required checks:

| Check | App ID |
| --- | --- |
| `DCO` | 1861 |
| `📎 Clippy` | 15368 |
| `🧪 Unit + integration tests` | 15368 |
| `📖 Doctests` | 15368 |
| `🏁 End-to-end fixtures` | 15368 |
| `🎨 rustfmt` | 15368 |
| `🛡️ cargo-deny (licenses + advisories)` | 15368 |
| `📝 typos (spell check)` | 15368 |
| `🧹 taplo (TOML format + lint)` | 15368 |

Wait for any existing queue entries before moving settings. Keep merges
paused during the cutover; do not remove protection from `main`.

## Merge and migrate open PRs

1. Squash merge the migration PR into `main`.
2. Check its `release-plz` run: publishing must be a no-op because this is
   not a Release PR; the compiler job must be skipped. The release-pr job
   may open the next version/changelog PR if it finds package changes.
3. Retarget open PRs from `main-next` to `main`. At the audit these were
   #348, #321, #302, and #204. Re-list open PRs at cutover to catch new ones.
   #281 is stacked on another feature branch and must keep that base.
4. Inspect each retargeted PR's commits and file diff. #321, #302, and #204
   descend from the history that was squashed into the release, so merely
   changing the base does not fix their ancestry. Rebase only the PR's
   actual changes onto current `main`, using the original fork point as
   the `--onto` boundary; do not replay the entire former integration
   history. Coordinate updates to contributor forks with their authors.
5. For #348, prefer letting Renovate recreate/rebase its lockfile PR on
   `main`. Reapply its `automerge` label if a new approval is needed.
6. Close any Release PR still based on `main-next`; it must be regenerated
   against `main`. #349 was already closed during the history repair.
7. Merge the documentation and benchmark companion PRs, and deploy the
   documentation through that repository's normal deployment process.

## Retire main-next

Re-fetch both branches and confirm all former `main-next` file changes are
present in `main`. The expected tree difference consists only of the
reviewed migration changes; squash merging does not preserve ancestry.
Save the old branch head locally before deletion.

After no PR targets `main-next`, disable/remove the `Develop` ruleset and
remove the classic protection targeting that branch. Delete `main-next`
using an exact lease on the head just verified. Keep the new `Main`
ruleset and `main` protection active throughout.

Contributors with no local work on their `main` can update with:

```sh
git fetch origin --prune
git switch main
git pull --ff-only
```

Preserve local work on feature branches before moving an old checkout.
Existing feature PRs may need the targeted rebase described above. New
branches and PRs start from `main`.

## Final verification

- No open PR targets the retired branch, and no active workflow or
  Renovate configuration refers to it.
- Renovate creates its next PR against `main`; required review and checks
  let it enter the squash merge queue.
- Ordinary PR merges update the rolling Release PR without publishing.
- On the next actual release, the Release PR receives its own push event,
  libraries publish first, and compiler assets use the same squash commit.
- Existing compiler and library release tags still identify their original
  published commits.
