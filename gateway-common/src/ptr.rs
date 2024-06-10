//! The [`Ptr`] type is a thin wrapper around `T` to enable cheap clone and comparisons.
//!
//! Internally it contains an [`Arc`] that is compared by address instead of by the implementation
//! of the pointed to value.

// The present piece of code is a modified version of the `eventuals` crate
// (https://crates.io/crates/eventuals) which is licensed under the MIT license.
//
// The original code can be found at:
// https://github.com/edgeandnode/eventuals/blob/2b552f5257a59c8e50f2adb74952059152307b8f/src/eventual/ptr.rs

use core::fmt;
use std::{
    borrow::Borrow,
    cmp::Ordering,
    convert::AsRef,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

use by_address::ByAddress;

/// a thin wrapper around T to enable cheap clone and comparisons.
#[repr(transparent)]
#[derive(Default)]
pub struct Ptr<T: ?Sized> {
    inner: ByAddress<Arc<T>>,
}

impl<T> Ptr<T> {
    /// Constructs a new `Ptr<T>`.
    #[inline]
    pub fn new(wrapped: T) -> Self {
        Self {
            inner: ByAddress(Arc::new(wrapped)),
        }
    }
}

impl<T: ?Sized> Deref for Ptr<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl<T: ?Sized> Borrow<T> for Ptr<T> {
    #[inline]
    fn borrow(&self) -> &T {
        self.inner.borrow()
    }
}

impl<T: ?Sized> AsRef<T> for Ptr<T> {
    #[inline]
    fn as_ref(&self) -> &T {
        Arc::as_ref(&self.inner)
    }
}

impl<T: ?Sized> Hash for Ptr<T> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state)
    }
}

impl<T: ?Sized> PartialEq for Ptr<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

impl<T: ?Sized> PartialOrd for Ptr<T> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: ?Sized> Ord for Ptr<T> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

impl<T: ?Sized> Eq for Ptr<T> {}

impl<T> Clone for Ptr<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T: ?Sized + fmt::Display> fmt::Display for Ptr<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: ?Sized + fmt::Debug> fmt::Debug for Ptr<T> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: ?Sized> fmt::Pointer for Ptr<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Arc::fmt(&self.inner, f)
    }
}

impl<T: ?Sized> From<ByAddress<Arc<T>>> for Ptr<T> {
    #[inline]
    fn from(inner: ByAddress<Arc<T>>) -> Self {
        Self { inner }
    }
}

impl<T: ?Sized> From<Arc<T>> for Ptr<T> {
    #[inline]
    fn from(arc: Arc<T>) -> Self {
        Self {
            inner: ByAddress(arc),
        }
    }
}

impl<T> From<T> for Ptr<T> {
    /// Converts a `T` into an `Ptr<T>`
    ///
    /// The conversion moves the value into a newly allocated `Ptr`. It is equivalent to
    /// calling `Ptr::new(t)`.
    #[inline]
    fn from(t: T) -> Self {
        Self::new(t)
    }
}
