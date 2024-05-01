use std::borrow::Borrow;

use url::Url;

/// Builds the URL to query the version of the indexer.
///
/// # Panics
/// The function panics if the URL cannot be built.
pub fn version_url<U: Borrow<Url>>(url: U) -> Url {
    url.borrow()
        .join("version/")
        .expect("failed to build indexer version URL")
}

/// Builds the URL to the status endpoint of the indexer.
///
/// # Panics
/// The function panics if the URL cannot be built.
pub fn status_url<U: Borrow<Url>>(url: U) -> Url {
    url.borrow()
        .join("status/")
        .expect("failed to build indexer status URL")
}

/// Builds the URL to the cost model endpoint of the indexer.
///
/// # Panics
/// The function panics if the URL cannot be built.
pub fn cost_url<U: Borrow<Url>>(url: U) -> Url {
    url.borrow()
        .join("cost/")
        .expect("failed to build indexer cost URL")
}

/// Builds the URL to the GraphQL API endpoint of the indexer.
///
/// # Panics
/// The function panics if the URL cannot be built.
pub fn api_url<U: Borrow<Url>>(url: U) -> Url {
    url.borrow()
        .join("api/")
        .expect("failed to build indexer API URL")
}

#[cfg(test)]
mod tests {
    use url::Url;

    use super::{api_url, cost_url, status_url, version_url};

    /// Ensure the different URL builder functions accept owned and borrowed URL parameters.
    #[test]
    fn check_url_builders() {
        let url = Url::parse("http://localhost:8020").expect("Invalid URL");

        // Version URL
        let _ = version_url(&url);
        let _ = version_url(url.clone());

        // Status URL
        let _ = status_url(&url);
        let _ = status_url(url.clone());

        // Cost URL
        let _ = cost_url(&url);
        let _ = cost_url(url.clone());

        // API URL
        let _ = api_url(&url);
        let _ = api_url(url.clone());
    }
}
