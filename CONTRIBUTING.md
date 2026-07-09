# Contributing

## Commit messages and PR titles

This repo follows [Conventional Commits](https://www.conventionalcommits.org/).
Releases are automated with [release-plz](https://release-plz.dev/) and
[git-cliff](https://git-cliff.org/): the type on your PR title drives which
CHANGELOG.md section (and GitHub Release note section) the change lands in,
so pick the type that matches the *user-visible* effect of the change, not
the mechanics of how you made it.

PR titles are squash-merged into `main`'s history and linted by CI
(`commit-lint`) against the format below - non-conforming titles fail the
check, but the check only runs on pull requests, never on pushes or merge
commits, so historical commits are unaffected.

### Format

```
<type>[optional scope][!]: <description>

[optional body]

[optional footer(s)]
```

### Types

| Type       | Use for                                                          | Changelog section |
|------------|-------------------------------------------------------------------|--------------------|
| `feat`     | a new user-facing capability                                     | Added              |
| `fix`      | a bug fix                                                         | Fixed              |
| `perf`     | a change that improves performance without changing behavior     | Performance        |
| `refactor` | a code change that neither fixes a bug nor adds a feature         | Changed            |
| `docs`     | documentation only                                                | Documentation      |
| `deps`     | a dependency bump (also `build(deps)` / `chore(deps)`, e.g. Dependabot) | Dependencies  |
| `security` | a security-relevant fix or hardening                              | Security           |
| `revert`   | reverts a previous commit                                         | Reverted           |
| `chore`    | maintenance with no user-visible effect                           | *(not published)*  |
| `ci`       | CI/workflow changes                                                | *(not published)*  |
| `test`     | adding or fixing tests                                             | *(not published)*  |
| `style`    | formatting, whitespace, no code meaning change                    | *(not published)*  |
| `build`    | build system/tooling changes (non-dependency)                     | *(not published)*  |

`chore`/`ci`/`test`/`style`/`build` are still linted (so CI stays useful for
non-release changes) but are intentionally excluded from CHANGELOG.md and
release notes - they're internal, not user-visible.

### Breaking changes

Mark a breaking change with a `!` right before the colon (e.g.
`feat!: drop legacy /msg endpoint`), or with a `BREAKING CHANGE:` footer
describing the break. Either form is grouped into the changelog's
**⚠ Breaking Changes** section ahead of everything else, regardless of the
commit's type.

```
fix(api)!: reject legacy bearer token format

BREAKING CHANGE: tokens minted before v0.2.0 are no longer accepted.
```

### Scope

An optional parenthesized scope narrows the type, e.g. `fix(server): ...`,
`feat(cli): ...`. Scope is not required.

### Examples

```
feat(cli): add `orbal-net inbox` subcommand
fix(server): retry on sqlite lock instead of failing the request
perf: avoid a per-poll allocation in the wait loop
docs: document the events protocol in README
chore(deps): bump ratatui to 0.29
```
