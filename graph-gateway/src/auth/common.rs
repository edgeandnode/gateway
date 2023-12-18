use std::sync::Arc;

use thegraph::types::{DeploymentId, SubgraphId};

use crate::topology::Deployment;

/// Check if the given deployments are authorized by the given authorized deployments.
pub fn are_deployments_authorized(
    authorized: &[DeploymentId],
    deployments: &[Arc<Deployment>],
) -> bool {
    deployments
        .iter()
        .any(|deployment| authorized.contains(&deployment.id))
}

/// Check if any of the given deployments are authorized by the given authorized subgraphs.
pub fn are_subgraphs_authorized(
    authorized: &[SubgraphId],
    deployments: &[Arc<Deployment>],
) -> bool {
    deployments.iter().any(|deployment| {
        deployment
            .subgraphs
            .iter()
            .any(|subgraph_id| authorized.contains(subgraph_id))
    })
}

/// Check if the query origin domain is authorized.
///
/// If the authorized domain starts with a `*`, it is considered a wildcard
/// domain. This means that any origin domain that ends with the wildcard
/// domain is considered authorized.
pub fn is_domain_authorized(authorized: &[&str], origin: &str) -> bool {
    fn match_domain(pattern: &str, origin: &str) -> bool {
        if pattern.starts_with('*') {
            origin.ends_with(pattern.trim_start_matches('*'))
        } else {
            origin == pattern
        }
    }

    authorized
        .iter()
        .any(|pattern| match_domain(pattern, origin))
}

#[cfg(test)]
mod tests {
    use super::is_domain_authorized;

    #[test]
    fn authorized_domains() {
        let authorized_domains = [
            "example.com",
            "localhost",
            "a.b.c",
            "*.d.e",
            "*-foo.vercel.app",
        ];

        let tests = [
            ("", false),
            ("example.com", true),
            ("subdomain.example.com", false),
            ("localhost", true),
            ("badhost", false),
            ("a.b.c", true),
            ("c", false),
            ("b.c", false),
            ("d.b.c", false),
            ("a", false),
            ("a.b", false),
            ("e", false),
            ("d.e", false),
            ("z.d.e", true),
            ("-foo.vercel.app", true),
            ("foo.vercel.app", false),
            ("bar-foo.vercel.app", true),
            ("bar.foo.vercel.app", false),
        ];

        for (input, expected) in tests {
            assert_eq!(
                expected,
                is_domain_authorized(&authorized_domains, input),
                "match '{input}'"
            );
        }
    }
}
