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
changelogs. Ordinary PR merges do not publish packages
([`release_always = false`](../../release-plz.toml)).

1. Review the Release PR's versions, dependency requirements, and
   changelogs against the changes since the last release.
2. For a compiler release, manually update `flowlog-compiler/Cargo.toml`
   and its changelog in the same PR, then refresh `Cargo.lock`. Update the
   scaffold's `flowlog-runtime` requirement if generated code needs a newer
   runtime API. Library-only releases need no compiler version bump.
3. Use a descriptive title, such as
   `chore(release): compiler 0.6.1, build 0.5.1`. Keep the `release-plz-`
   branch prefix: it identifies the PR as a release. If the bot replaces
   a PR, transfer any manual edits to its replacement.
4. Wait for review and all CI checks, then squash merge through the queue.
   The queue must merge **one PR per push** so publishing sees the Release PR.

Publishing releases library crates first, then calls the
[compiler workflow](../../.github/workflows/release-compiler.yml) for the
same commit. A new compiler version produces Linux, macOS, and Windows
archives, checksums, and a `flowlog-compiler-v<version>` release using its
changelog. Compiler-only releases use this same process.

Verify the workflow, crates.io versions, tags, and release assets afterward.

## Recovering a failed release

Fix infrastructure issues, then rerun failed jobs from the original
`release-plz` run, including compiler jobs. Published crate versions and
existing compiler releases are skipped.

Source or workflow fixes need a new `release-plz-*` PR. Rerunning an old
job uses its original workflow and commit; changed published code needs a
new version. If a compiler release exists with missing assets, repair that
release explicitly because retries skip existing releases.

## Repository configuration

Keep `main` as the default branch, with required reviews, all CI checks,
and the squash merge queue. Keep CI's `merge_group` trigger and limit the
queue to one PR per merge; builds may run concurrently.

Renovate targets `main`. Its approval workflow handles rebases because
`main` dismisses stale reviews. Required secrets:

- `RENOVATE_APPROVE_TOKEN`: an actor whose review satisfies the
  Committers-team requirement.
- `RELEASE_PLZ_TOKEN`: repository contents and PR write access; its PRs
  must trigger CI.
- `CARGO_REGISTRY_TOKEN`: permission to publish the library crates.

Compiler publishing uses `GITHUB_TOKEN` with contents write permission.
