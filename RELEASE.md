# Release Process

1. Test the main branch using the [edgeandnode/local-network-environments](https://github.com/edgeandnode/local-network-environments) (`theodus/bash-time` branch)
   - Make sure that the gateway returns a correct response to a valid query
   - Run any other ad-hoc tests that are appropriate for the set of changes made since the last release
2. Open a PR for the new release on [edgeandnode/local-network-environments](https://github.com/edgeandnode/local-network-environments)
   - Version the release based on [SemVer](https://semver.org/)
     - Note that the data science team relies on the logging format, so any changes to existing log events, spans, or fields require a major version bump
   - Set the new version in `Cargo.toml`
   - Include release notes for changes since the last release
   - Rebase & Merge the PR
   - Create a new release via GitHub
     - Include the release notes from the PR
     - Tag the commit with the version string, prefixed with a `v`
3. Open a PR on [edgeandnode/graph-network-infra](https://github.com/edgeandnode/graph-network-infra)
   - Update the staging (and testnet?) gateways to the new version
   - Update environment variables and Prometheus filters as necessary
4. Query the staging gateway at `https://gateway.staging.network.thegraph.com`
5. Open a PR on [edgeandnode/graph-network-infra](https://github.com/edgeandnode/graph-network-infra)
   - Update the mainnet gateway configs, making the same configuration changes as in step 3
