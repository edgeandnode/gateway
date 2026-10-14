# Releasing

Releases are cut by hand. The only automated step is building and publishing the Docker image when a
version tag is pushed.

## Versioning

Versions follow [semver](https://semver.org) and are tagged as `vX.Y.Z`:

- minor bump (`v27.6.0` → `v27.7.0`) for new features and behavior changes
- patch bump (`v27.7.0` → `v27.7.1`) for fixes only

The source of truth for the version is `version` in `Cargo.toml`.

## Steps

1. **Open a release PR.** On a new branch, bump `version` in `Cargo.toml` and update `Cargo.lock`
   (running `cargo check` is enough). Make no other changes. Title the PR `release: vX.Y.Z`.

2. **Merge it to `main`.** CI (`.github/workflows/ci.yml`) must pass: formatting, `cargo check`,
   `clippy`, and unit tests.

3. **Create the GitHub Release.** Create the tag `vX.Y.Z` on the release commit and publish a GitHub
   Release for it with auto-generated notes. This can be done via \
   [GitHub UI](https://github.com/graphprotocol/gateway-x/releases/new) or terminal:

   ```sh
   gh release create vX.Y.Z --target main --title vX.Y.Z --generate-notes
   ```

4. **Check the Docker image.** Pushing the tag triggers `.github/workflows/docker-image.yml`, which
   builds `linux/amd64` and `linux/arm64` images and publishes a multi-arch manifest to
   `ghcr.io/edgeandnode/graph-gateway` with these tags:

   - `vX.Y.Z`
   - `sha-<short commit sha>`

   Confirm the workflow run succeeded and the `vX.Y.Z` tag exists in the registry.

## Docker image tags

| Trigger                          | Image tags              |
| -------------------------------- | ----------------------- |
| Push of a `v*` tag               | `vX.Y.Z`, `sha-<short>` |
| Push to `main`                   | `sha-<short>`           |
| Manual run (`workflow_dispatch`) | `sha-<short>`           |

No `latest` tag is published. Always deploy a pinned version tag.
