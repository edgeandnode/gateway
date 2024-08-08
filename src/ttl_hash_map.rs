//! A hashmap with entries that expire after a given TTL.
//!
//! <div class="warning">
//! The hashmap expired entries are not automatically removed. You must call
//! [`cleanup`](TtlHashMap::cleanup) to remove the expired entries and release the unused memory.
//! </div>
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

/// The default TTL for entries is [`Duration::MAX`].
pub const DEFAULT_TTL: Duration = Duration::MAX;

/// A hashmap with entries that expire after a given TTL.
#[derive(Clone)]
pub struct TtlHashMap<K, V> {
    ttl: Duration,
    inner: HashMap<K, (Instant, V)>,
}

impl<K, V> Default for TtlHashMap<K, V> {
    fn default() -> Self {
        Self {
            ttl: DEFAULT_TTL,
            inner: Default::default(),
        }
    }
}

impl<K, V> TtlHashMap<K, V> {
    /// Create a new hashmap with the default TTL (see [`DEFAULT_TTL`]).
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a new hashmap with the given TTL.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            inner: Default::default(),
        }
    }

    /// Create a new hashmap with the given TTL and capacity.
    pub fn with_ttl_and_capacity(ttl: Duration, capacity: usize) -> Self {
        Self {
            ttl,
            inner: HashMap::with_capacity(capacity),
        }
    }
}

impl<K, V> TtlHashMap<K, V>
where
    K: Eq + std::hash::Hash,
{
    /// Insert a key-value pair into the hashmap.
    ///
    /// If the key already exists, the value is updated and the old value is returned.
    /// Otherwise, `None` is returned.
    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        let now = Instant::now();
        self.inner
            .insert(key, (now, value))
            .and_then(|(timestamp, value)| {
                if timestamp.elapsed() < self.ttl {
                    Some(value)
                } else {
                    None
                }
            })
    }

    /// Get the value associated with the key.
    ///
    /// If the key is found and the entry has not expired, the value is returned. Otherwise,
    /// `None` is returned.
    #[must_use]
    pub fn get(&self, key: &K) -> Option<&V> {
        self.inner.get(key).and_then(|(timestamp, value)| {
            if timestamp.elapsed() < self.ttl {
                Some(value)
            } else {
                None
            }
        })
    }

    /// Remove the key and its associated value from the hashmap.
    ///
    /// If the key is found and the entry has not expired, the value is returned. Otherwise,
    /// `None` is returned.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.inner.remove(key).and_then(|(timestamp, value)| {
            if timestamp.elapsed() < self.ttl {
                Some(value)
            } else {
                None
            }
        })
    }

    /// Returns the number of elements in the hashmap.
    ///
    /// This is the number of non-expired entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner
            .iter()
            .filter(|(_, (timestamp, _))| timestamp.elapsed() < self.ttl)
            .count()
    }

    /// Returns whether the hashmap is empty.
    ///
    /// This is true if the hashmap is actually empty, or all its entries are expired.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of elements in the hashmap, including the expired ones.
    ///
    /// This is the number of all entries, expired and non-expired.
    #[must_use]
    pub fn len_all(&self) -> usize {
        self.inner.len()
    }

    /// Returns the current capacity of the hashmap.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.inner.capacity()
    }

    /// Shrinks the capacity of the hashmap as much as possible.
    pub fn shrink_to_fit(&mut self) {
        self.inner.shrink_to_fit();
    }

    /// Clear the hashmap, removing all entries.
    pub fn clear(&mut self) {
        self.inner.clear();
    }

    /// Cleanup the hashmap, removing all expired entries.
    ///
    /// After removing all expired entries, the inner hashmap is shrunk to fit the new capacity,
    /// releasing the unused memory.
    pub fn cleanup(&mut self) {
        // Remove all expired entries
        self.inner
            .retain(|_, (timestamp, _)| timestamp.elapsed() < self.ttl);

        // Shrink the inner hashmap to fit the new size
        self.shrink_to_fit();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get_an_item() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::new();

        let key = "item";
        let value = 1337;

        //* When
        ttl_hash_map.insert(key, value);

        //* Then
        assert_eq!(ttl_hash_map.get(&key), Some(&value));
        assert_eq!(ttl_hash_map.len(), 1);
    }

    #[test]
    fn get_none_if_no_item_is_present() {
        //* Given
        let ttl_hash_map = TtlHashMap::<&str, ()>::new();

        let key = "item";

        //* When
        let value = ttl_hash_map.get(&key);

        //* Then
        assert_eq!(value, None);
    }

    #[test]
    fn get_none_if_the_item_is_expired() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl(Duration::from_millis(5));

        let key = "item";
        let value = 1337;

        // Pre-populate the map
        ttl_hash_map.insert(key, value);

        //* When
        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        //* Then
        assert_eq!(ttl_hash_map.get(&key), None);
        assert_eq!(ttl_hash_map.len(), 0);
    }

    #[test]
    fn report_the_correct_length() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl(Duration::from_millis(5));

        let key_1 = "expired_item_1";
        let value_1 = 1337;

        let key_2 = "expired_item_2";
        let value_2 = 42;

        let key_3 = "non_expired_item";
        let value_3 = 69;

        // Pre-populate the map
        ttl_hash_map.insert(key_1, value_1);
        ttl_hash_map.insert(key_2, value_2);

        //* When
        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        // Insert a new item with different key
        ttl_hash_map.insert(key_3, value_3);

        //* Then
        // One non-expired item and two expired items
        assert_eq!(ttl_hash_map.len(), 1);
        assert_eq!(ttl_hash_map.len_all(), 3);
        assert!(!ttl_hash_map.is_empty());
    }

    #[test]
    fn insert_an_item_and_return_the_old_value_if_not_expired() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::new();

        let key = "item";
        let old_value = 1337;
        let new_value = 42;

        // Pre-populate the map
        ttl_hash_map.insert(key, old_value);

        //* When
        let returned_old_value = ttl_hash_map.insert(key, new_value);

        //* Then
        assert_eq!(returned_old_value, Some(old_value));
        assert_eq!(ttl_hash_map.get(&key), Some(&new_value));
        assert_eq!(ttl_hash_map.len(), 1);
        assert_eq!(ttl_hash_map.len_all(), 1);
        assert!(!ttl_hash_map.is_empty());
    }

    #[test]
    fn insert_an_item_and_return_none_if_the_old_value_is_expired() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl(Duration::from_millis(5));

        let key = "item";
        let old_value = 1337;
        let new_value = 42;

        // Pre-populate the map
        ttl_hash_map.insert(key, old_value);

        //* When
        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        let returned_old_value = ttl_hash_map.insert(key, new_value);

        //* Then
        assert_eq!(returned_old_value, None);
        assert_eq!(ttl_hash_map.get(&key), Some(&new_value));
        assert_eq!(ttl_hash_map.len(), 1);
        assert_eq!(ttl_hash_map.len_all(), 1);
        assert!(!ttl_hash_map.is_empty());
    }

    #[test]
    fn remove_an_item_and_return_it_if_not_expired() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::new();

        let key = "item";
        let value = 1337;

        // Pre-populate the map
        ttl_hash_map.insert(key, value);

        //* When
        let removed_value = ttl_hash_map.remove(&key);

        //* Then
        assert_eq!(removed_value, Some(value));
        assert_eq!(ttl_hash_map.get(&key), None);
        assert_eq!(ttl_hash_map.len(), 0);
        assert_eq!(ttl_hash_map.len_all(), 0);
        assert!(ttl_hash_map.is_empty());
    }

    #[test]
    fn remove_an_item_and_return_none_if_expired() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl(Duration::from_millis(5));

        let key = "item";
        let value = 1337;

        // Pre-populate the map
        ttl_hash_map.insert(key, value);

        //* When
        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        let removed_value = ttl_hash_map.remove(&key);

        //* Then
        assert_eq!(removed_value, None);
        assert_eq!(ttl_hash_map.get(&key), None);
        assert_eq!(ttl_hash_map.len(), 0);
        assert_eq!(ttl_hash_map.len_all(), 0);
        assert!(ttl_hash_map.is_empty());
    }

    #[test]
    fn clear_the_hashmap() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl_and_capacity(Duration::from_millis(5), 5);

        let key_1 = "item_1";
        let value_1 = 1337;

        let key_2 = "item_2";
        let value_2 = 42;

        // Pre-populate the map
        ttl_hash_map.insert(key_1, value_1);
        ttl_hash_map.insert(key_2, value_2);

        //* When
        ttl_hash_map.clear();

        //* Then
        assert_eq!(ttl_hash_map.len(), 0);
        assert_eq!(ttl_hash_map.len_all(), 0);
        assert!(ttl_hash_map.is_empty());
    }

    #[test]
    fn cleanup_the_hashmap_and_shrink_to_fit() {
        //* Given
        let mut ttl_hash_map = TtlHashMap::with_ttl_and_capacity(Duration::from_millis(5), 5);

        let key = "non_expired_item";
        let value = 69;

        // Pre-populate the map with 100 items
        for i in 0..100 {
            ttl_hash_map.insert(format!("expired_item_{i}"), i);
        }

        //* When
        // Wait for the TTL to expire
        std::thread::sleep(Duration::from_millis(10));

        // Insert a new item with different key
        ttl_hash_map.insert(key.to_string(), value);

        // Remove expired entries and shrink the hashmap to fit the new size
        ttl_hash_map.cleanup();

        //* Then
        assert_eq!(ttl_hash_map.len(), 1);
        assert_eq!(ttl_hash_map.len_all(), 1);
        assert!(ttl_hash_map.capacity() < 100);
        assert!(!ttl_hash_map.is_empty());
    }
}
