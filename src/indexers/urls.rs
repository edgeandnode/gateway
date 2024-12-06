use std::borrow::Borrow;

use url::Url;

/// Builds the URL to the status endpoint of the indexer.
///
/// # Panics
/// The function panics if the URL cannot be built.
pub fn status_url<U: Borrow<Url>>(url: U) -> StatusUrl {
    let url = url
        .borrow()
        .join("status/")
        .expect("failed to build indexer status URL");
    StatusUrl(url)
}

/// Newtype wrapper around `Url` to provide type safety.
macro_rules! url_new_type {
    ($name:ident) => {
        /// Newtype wrapper around `Url` to provide type safety.
        #[derive(Clone, PartialEq, Eq, Hash)]
        pub struct $name(Url);

        impl $name {
            /// Return the internal representation.
            pub(super) fn into_inner(self) -> Url {
                self.0
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                std::fmt::Display::fmt(&self, f)
            }
        }

        impl AsRef<Url> for $name {
            fn as_ref(&self) -> &Url {
                &self.0
            }
        }

        impl std::ops::Deref for $name {
            type Target = Url;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
    };
}

url_new_type!(StatusUrl);

#[cfg(test)]
mod tests {
    use url::Url;

    use super::status_url;

    /// Ensure the different URL builder functions accept owned and borrowed URL parameters.
    #[test]
    fn check_url_builders() {
        let url = Url::parse("http://localhost:8020").expect("Invalid URL");

        // Status URL
        let _ = status_url(&url);
        let _ = status_url(url.clone());
    }
}
