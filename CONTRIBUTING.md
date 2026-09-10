# Contributing

## Branching model

This project uses a lightweight, trunk-based flow (not GitFlow) -- there's
one target Kong version and one active line of development, so there's no
need for permanent `develop`/`release/*` branches:

- `main` is always the current state of the plugin; treat it as deployable.
- Make changes on a short-lived branch off `main`, named `feature/<short-desc>`
  or `fix/<short-desc>`, and open a pull request into `main`.
- Delete the branch once it's merged.

If this project ever needs to support multiple Kong major versions in
parallel, a `release/<version>` branch per supported line is the natural
point to introduce that -- not before.

## Releases

Releases are plain [SemVer](https://semver.org/) git tags on `main`
(`v1.0.0`, `v1.1.0`, ...), not a separate release branch:

1. Update `CHANGELOG.md` with the new version's changes.
2. Update the version in `kong-plugin/concurrency-limit-1.0.0-1.rockspec`
   if it changed (rockspec filenames/versions follow
   `<package>-<version>-<rockspec revision>`).
3. `git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`

## Before opening a PR

- Run `./scripts/test-lifecycle.sh` (normal + abnormal request lifecycle,
  ~a few minutes, brings up and tears down its own Docker stack).
- For anything touching the plugin's concurrency-limiting logic itself,
  consider a short validation soak run too:
  `python3 scripts/soak_test.py --validation --route mock-route --path /mock/anything`.
