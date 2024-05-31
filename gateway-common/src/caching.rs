//! Cache related types and traits.

use std::ops::Deref;

/// The [`Freshness`] type represents the freshness of the data returned by the resolver's cache:
/// all the data returned by the resolver is either [`Fresh`](Freshness::Fresh)
/// or [`Cached`](Freshness::Cached).
///
/// If the data is fresh, it means that the data was successfully fetched. Otherwise, after
/// failing to fetch the data, the data was returned from the cache.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Freshness<T> {
    /// The data is fresh.
    ///
    /// The data was successfully fetched from the indexer.
    Fresh(T),
    /// The data was fetched from the cache.
    ///
    /// In the case the resolver failed to fetch the data, it returned the cached data.
    Cached(T),
}

impl<T> Freshness<T> {
    /// Returns `true` if the data is fresh.
    ///
    /// See also [`is_cached`](Freshness::is_cached).
    #[must_use]
    #[inline]
    pub fn is_fresh(&self) -> bool {
        matches!(self, Freshness::Fresh(_))
    }

    /// Returns `true` if the data was fetched from the cache.
    ///
    /// See also [`is_fresh`](Freshness::is_fresh).
    #[must_use]
    #[inline]
    pub fn is_cached(&self) -> bool {
        matches!(self, Freshness::Cached(_))
    }

    /// If the data is fresh, returns `Some` with the data, otherwise returns `None`.
    ///
    /// See also [`as_cached`](Freshness::as_cached).
    #[must_use]
    #[inline]
    pub fn as_fresh(&self) -> Option<&T> {
        match self {
            Freshness::Fresh(data) => Some(data),
            Freshness::Cached(_) => None,
        }
    }

    /// If the data was fetched from the cache, returns `Some`, otherwise returns `None`.
    ///
    /// See also [`as_fresh`](Freshness::as_fresh).
    #[must_use]
    #[inline]
    pub fn as_cached(&self) -> Option<&T> {
        match self {
            Freshness::Fresh(_) => None,
            Freshness::Cached(data) => Some(data),
        }
    }

    /// Converts from `DataFreshness<T>` to `DataFreshness<&T>`.
    #[must_use]
    #[inline]
    pub fn as_ref(&self) -> Freshness<&T> {
        match *self {
            Freshness::Fresh(ref data) => Freshness::Fresh(data),
            Freshness::Cached(ref data) => Freshness::Cached(data),
        }
    }

    /// Returns the contained data, consuming the `self` value.
    #[must_use]
    #[inline]
    pub fn into_inner(self) -> T {
        match self {
            Freshness::Fresh(data) | Freshness::Cached(data) => data,
        }
    }

    /// Maps a `Freshness<T>` to `Fresnes<U>` by applying a function to a contained value.
    #[must_use]
    #[inline]
    pub fn map<U, F>(self, f: F) -> Freshness<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            Freshness::Fresh(data) => Freshness::Fresh(f(data)),
            Freshness::Cached(data) => Freshness::Cached(f(data)),
        }
    }
}

impl<T> Deref for Freshness<T> {
    type Target = T;

    #[must_use]
    #[inline]
    fn deref(&self) -> &Self::Target {
        match self {
            Freshness::Fresh(data) | Freshness::Cached(data) => data,
        }
    }
}
