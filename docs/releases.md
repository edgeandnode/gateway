# Releases

1. Test the main branch using the [edgeandnode/local-network](https://github.com/edgeandnode/local-network)
   - Make sure that the gateway returns a correct response to a valid query
   - Run any other ad-hoc tests that are appropriate for the set of changes made since the last release
2. Open a PR for the new release on [edgeandnode/graph-gateway](https://github.com/edgeandnode/graph-gateway)
   - Version the release based on [SemVer](https://semver.org/). The following trigger a major version bump:
     - Breaking changes to the configuration file
   - Set the new version in `Cargo.toml`, and run `cargo update`
   - Include release notes for changes since the last release. See past releases for format.
   - Rebase & Merge the PR
   - Create a new release via GitHub
     - Include the release notes from the PR
     - Tag the commit with the version string, prefixed with a `v`

