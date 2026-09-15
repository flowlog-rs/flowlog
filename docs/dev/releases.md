# Development and releases

`main` contains ongoing development. Tags and GitHub Releases identify
published versions.

## Development

Create feature branches and PRs against `main`, with at least one label.
Sign off commits with
`git commit -s` and use a Conventional Commit PR title: the squash commit
feeds the release changelog. After review and CI, use the squash merge
queue. Merged feature branches are deleted automatically.

See [AGENTS.md](../../AGENTS.md) for contribution and CI requirements.

## Preparing a release

Every push to `main` runs [release-plz](../../.github/workflows/release-plz.yml),
which opens or updates a `release-plz-*` PR with crate versions and
changelogs. Compiler versions are compared against `flowlog-compiler-v*`
tags using `git_only`; changes to the libraries it ships also contribute
to its version and changelog. The workflow then synchronizes the compiler's
runtime requirement with the runtime version in that PR. Ordinary PR merges
do not publish packages ([`release_always = false`](../../release-plz.toml)).

1. Review the Release PR's versions, dependency requirements, and
   changelogs against the changes since the last release.
2. Review the compiler version, its changelog, and its generated runtime
   requirement in the same PR. If overriding a proposed version, refresh
   `Cargo.lock` and run `python3 .github/scripts/sync-compiler-runtime.py`.
3. Use a descriptive title, such as
   `chore(release): compiler 0.6.1, build 0.5.1`. Keep the `release-plz-`
   branch prefix: it identifies the PR as a release. If the bot replaces
   a PR, transfer any manual edits to its replacement.
4. Wait for review and all CI checks, then squash merge through the queue.
   The queue must merge **one PR per push** so publishing sees the Release PR.

After a Release PR merges, release-plz publishes library crates to
crates.io and creates their Git tags and GitHub Releases. It also creates
the compiler's tag and Release from its changelog, using `git_only` to skip
crates.io publishing for the compiler.

Publishing a `flowlog-compiler-v<version>` Release triggers the
[compiler asset workflow](../../.github/workflows/release-compiler.yml).
Each platform checks out that tag, builds the compiler, and uploads its
archive and checksum directly to the existing Release. The Release page
appears before its attachments; wait for the Linux, macOS, and Windows jobs
to finish before announcing the binaries. Compiler-only releases use this
same process.

Verify the workflow, crates.io versions, tags, and release assets afterward.

## Recovering a failed release

For library publishing failures, fix the cause and rerun the original
`release-plz` job. Published crate versions are skipped.

For missing compiler attachments, rerun the failed jobs in **Compiler
release assets**. Each job rebuilds the same tag and replaces its own
archive and checksum. The workflow can also be run manually with the tag
of an existing compiler Release; this recovers missing attachments even
when there is no workflow run to retry.

Source or workflow fixes need a new `release-plz-*` PR. Rerunning an old
job uses its original workflow and commit; changed published code needs a
new version. Existing tags remain the source of the corresponding binaries.

## Repository configuration

Keep `main` as the default branch, with required reviews, all CI checks,
and the squash merge queue. Keep CI's `merge_group` trigger and limit the
queue to one PR per merge; builds may run concurrently.

Renovate targets `main`. Its approval workflow handles rebases because
`main` dismisses stale reviews. Required secrets:

- `RENOVATE_APPROVE_TOKEN`: an actor whose review satisfies the
  Committers-team requirement.
- `RELEASE_PLZ_TOKEN`: repository contents and PR write access; its PRs
  must trigger CI and its Release events must trigger compiler builds.
- `CARGO_REGISTRY_TOKEN`: permission to publish the library crates.

Compiler asset uploads use `GITHUB_TOKEN` with contents write permission.
