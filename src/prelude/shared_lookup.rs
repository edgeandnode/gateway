use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::RwLock;

pub struct SharedLookup<K, V> {
	inner: RwLock<HashMap<K, Arc<RwLock<V>>>>,
}

impl<K, V> Default for SharedLookup<K, V> {
	fn default() -> Self {
		Self {
			inner: RwLock::default(),
		}
	}
}

impl<K, V> SharedLookup<K, V>
where
	K: Clone + Eq + Hash,
	V: Default,
{
	pub async fn snapshot<S, F: Fn(&V) -> S>(&self, f: F) -> Vec<(K, S)> {
		use futures::stream::{FuturesUnordered, StreamExt as _};
		let keys = self.inner.read().await.keys().cloned().collect::<Vec<K>>();
		keys
			.iter()
			.map(|k| {
				let key = k.clone();
				self.with_value(k, |v| (key, f(v)))
			})
			.collect::<FuturesUnordered<_>>()
			.collect()
			.await
	}
}

impl<K: Eq + Hash, V> SharedLookup<K, V> {
	pub async fn restore<I: IntoIterator<Item = (K, V)>>(&self, iter: I) {
		let mut inner = self.inner.write().await;
		inner.clear();
		for (k, v) in iter.into_iter() {
			inner.insert(k, Arc::new(RwLock::new(v)));
		}
	}
}

impl<K: Clone + Eq + Hash, V: Default> SharedLookup<K, V> {
	async fn get_or_create(&self, key: &K) -> Arc<RwLock<V>> {
		// Insert is relatively infrequent, so opportunistically attempt using the read lock.
		{
			let read = self.inner.read().await;
			if let Some(tracker) = read.get(&key) {
				return tracker.clone();
			}
		}

		// Creating the pair outside of the lock. It may end up being dropped due to race conditions,
		// but what we buy is that no external code happens within a lock - which can help to prevent
		// deadlocks and other issues.
		let key = key.clone();
		let value = Default::default();

		let mut write = self.inner.write().await;
		// Always returns the value that is actually in the dictionary
		write.entry(key).or_insert(value).clone()
	}

	/// Do not call this or with_value_mut recursively. It risks deadlock.
	pub async fn with_value<T>(&self, key: &K, f: impl FnOnce(&V) -> T) -> T {
		let value = self.get_or_create(key).await;
		let lock = value.read().await;
		f(&lock)
	}

	/// Do not call this or with_value recursively. It risks deadlock.
	pub async fn with_value_mut<T>(&self, key: &K, f: impl FnOnce(&mut V) -> T) -> T {
		let value = self.get_or_create(key).await;
		let mut lock = value.write().await;
		f(&mut lock)
	}
}
