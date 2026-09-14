# Development and releases

`main` is the development branch. Published versions are identified by tags
and GitHub Releases; the tip of `main` can contain unreleased changes.

## Contributing

Create a short-lived branch from `main`, open a PR against `main`, and sign
off every commit with `git commit -s`. Use a Conventional Commit PR title
such as `fix(parser): preserve arithmetic precedence`: the squash commit
title becomes input to the release changelog.

After review and CI, use the merge queue with squash merging. Delete the
feature branch after its PR merges, and start the next change from `main`.
Do not reuse a squashed feature branch as a long-lived integration branch.
See [AGENTS.md](../../AGENTS.md) for contribution and CI requirements.

## Preparing a release

Every push to `main` runs
[`release-plz.yml`](../../.github/workflows/release-plz.yml). Its release-pr
job opens or refreshes a `release-plz-*` PR against `main`, updating crate
versions and changelogs. Ordinary development continues while that PR is
open. Merging a development PR does not publish packages:
[`release-plz.toml`](../../release-plz.toml) sets `release_always = false`.

When ready to publish:

1. Review the Release PR's versions, dependency requirements, and changelogs.
   API compatibility and user-visible changes determine the version numbers.
2. If publishing the compiler, update `flowlog-compiler/Cargo.toml` and its
   changelog on that same Release PR. The compiler has `publish = false`
   and its version is maintained manually. Refresh `Cargo.lock` after
   changing versions. Review the scaffold's `flowlog-runtime` requirement
   if generated code now depends on a newer runtime API.
3. Give the PR a descriptive title, for example
   `chore(release): compiler 0.6.1, build 0.5.1`. Keep the branch prefix
   `release-plz-`; both publishing jobs use it to recognize a release.
4. Wait for review and all CI checks. Refresh the Release PR if development
   has added changes that are missing from its version decisions or notes.
5. Squash merge the Release PR through the queue. Do not combine its merge
   with unrelated PRs in the same push; the queue must merge one PR at a time.

Manual version adjustments belong on the Release PR. If release-plz closes
and replaces a PR containing manual edits, transfer those edits to the new
PR before merging.

## Publishing

The release-plz publish job checks whether the pushed commit belongs to a
merged Release PR. It publishes library crates in dependency order and
creates their package tags and GitHub Releases.

After that job succeeds, it calls
[`release-compiler.yml`](../../.github/workflows/release-compiler.yml) for
the same squash commit. If that compiler version already has a release,
the compiler job skips it. Otherwise it builds Linux, macOS, and Windows
archives and checksums, creates `flowlog-compiler-v<version>`, and uses that
version's compiler changelog section as the release notes.

Compiler publishing runs even for a Release PR that only changes the
compiler version. Later pushes to `main` cannot trigger it or replace its
pending run. There is no second branch promotion or history reset.

Verify the workflow run, the expected crates.io versions, release tags, and
all compiler archives and checksums after publishing. Documentation-only
or library-only releases need not bump the compiler version.

## Recovering a failed release

Fix infrastructure problems such as a missing token, then re-run the
failed jobs from the original `release-plz` run. The run retains the
release commit. Already published crate versions and existing compiler
releases are skipped.

If a workflow or source fix is required, put it in a PR whose branch starts
with `release-plz-`, review the resulting release contents, and merge it
through the same checks. Published crates are immutable; use a new version
for changed published code. A retry cannot update the workflow definition
stored in the original run's commit.

The compiler workflow is reusable and has no separate manual-dispatch
entry point. Retry its jobs through the parent `release-plz` run. If a
GitHub Release exists with incomplete assets, inspect and repair that
release explicitly; the existing-release check will skip rebuilding it.

## Repository configuration

`main` must be the default branch. Its protections must include the squash
merge queue, reviewer requirements, and every required CI check. Keep
`merge_group` enabled in CI so queue entries receive checks. Set the
queue's maximum PRs to merge to one so each Release PR produces its own
push event. The queue may build multiple entries concurrently.

Renovate targets `main`. `RENOVATE_APPROVE_TOKEN` must belong to an actor
whose approval satisfies the required Committers-team review. Its workflow
reapproves Renovate updates because `main` dismisses stale reviews.

`RELEASE_PLZ_TOKEN` needs repository contents and PR write access so bot
PRs trigger CI and release tags can be pushed. `CARGO_REGISTRY_TOKEN` needs
publishing access to the library crates. The compiler uses the workflow's
`GITHUB_TOKEN` with contents write permission. No new secret is required
for the single-branch flow.

The one-time cutover is recorded in
[main-branch-migration.md](main-branch-migration.md).
