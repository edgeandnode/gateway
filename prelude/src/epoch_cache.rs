use std::{borrow::Borrow, collections::HashMap, hash::Hash};

/// HashMap where entries are automatically removed once they reach a count of `C` epochs where they
/// have been unused. Any get or insert operations on the entry will reset its counter.
pub struct EpochCache<K: Eq + Hash, V, const C: u8>(HashMap<K, (V, u8)>);

impl<K: Eq + Hash, V, const C: u8> Default for EpochCache<K, V, C> {
    fn default() -> Self {
        Self(HashMap::default())
    }
}

impl<K: Eq + Hash, V, const C: u8> EpochCache<K, V, C> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// WARNING: This will not observe the use of the value for the current epoch.
    pub fn get_unobserved<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Eq + Hash + ?Sized,
        K: Borrow<Q>,
    {
        self.0.get(key).map(|v| &v.0)
    }

    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        Q: Eq + Hash + ?Sized,
        K: Borrow<Q>,
    {
        let value = self.0.get_mut(key)?;
        value.1 = 0;
        Some(&value.0)
    }

    pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut V>
    where
        Q: Eq + Hash + ?Sized,
        K: Borrow<Q>,
    {
        let value = self.0.get_mut(key)?;
        value.1 = 0;
        Some(&mut value.0)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.0.insert(key, (value, 0)).map(|(v, _)| v)
    }

    pub fn get_or_insert(&mut self, key: K, f: impl FnOnce(&K) -> V) -> &mut V {
        let entry = self.0.entry(key);
        &mut entry
            .and_modify(|(_, c)| *c = 0)
            .or_insert_with_key(|k| (f(k), 0))
            .0
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        self.0.remove(key).map(|(v, _)| v)
    }

    pub fn increment_epoch(&mut self) {
        self.0 = self
            .0
            .drain()
            .filter_map(
                |(k, (v, c))| {
                    if c < C {
                        Some((k, (v, c + 1)))
                    } else {
                        None
                    }
                },
            )
            .collect();
    }

    pub fn apply(&mut self, mut f: impl FnMut(&mut V)) {
        for (v, _) in self.0.values_mut() {
            f(v);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let mut retain0 = test_cache::<0>();
        assert_eq!(retain0.get(&0), Some(&0));
        assert_eq!(retain0.get(&1), Some(&1));
        assert_eq!(retain0.get(&2), None);
        assert_eq!(retain0.len(), 2);
        retain0.increment_epoch();
        assert_eq!(retain0.len(), 0);

        let mut retain1 = test_cache::<1>();
        retain1.increment_epoch();
        assert_eq!(retain1.len(), 2);
        assert_eq!(retain1.get(&1), Some(&1));
        retain1.increment_epoch();
        assert_eq!(retain1.len(), 1);
        retain1.increment_epoch();
        assert_eq!(retain1.len(), 0);

        let mut retain2 = test_cache::<2>();
        retain2.increment_epoch();
        assert_eq!(retain2.len(), 2);
        retain2.increment_epoch();
        assert_eq!(retain2.len(), 2);
        assert_eq!(retain2.get(&1), Some(&1));
        retain2.increment_epoch();
        assert_eq!(retain2.len(), 1);
        retain2.increment_epoch();
        assert_eq!(retain2.len(), 1);
        retain2.increment_epoch();
        assert_eq!(retain2.len(), 0);
    }

    fn test_cache<const C: u8>() -> EpochCache<u8, u8, C> {
        let mut cache = EpochCache::<u8, u8, C>::new();
        cache.insert(0, 0);
        cache.insert(1, 1);
        cache
    }
}
