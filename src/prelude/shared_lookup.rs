use crate::prelude::Reader;
use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    sync::Arc,
};
use tokio::sync::{RwLock, RwLockReadGuard};

#[derive(Clone)]
pub struct SharedLookup<K, R>(Arc<RwLock<HashMap<K, Arc<R>>>>);

pub struct SharedLookupWriter<K, R, W> {
    readers: Arc<RwLock<HashMap<K, Arc<R>>>>,
    writers: HashMap<K, W>,
}

impl<K, R> SharedLookup<K, R>
where
    K: Eq + Hash,
{
    pub fn new<W>() -> (SharedLookupWriter<K, R, W>, Self) {
        let readers = Self(Arc::default());
        let writers = SharedLookupWriter {
            readers: readers.0.clone(),
            writers: HashMap::new(),
        };
        (writers, readers)
    }

    pub async fn get(&self, key: &K) -> Option<Arc<R>> {
        self.0.read().await.get(key).cloned()
    }

    pub async fn snapshot<S>(&self) -> Vec<S>
    where
        S: for<'k, 'r> From<(&'k K, &'r R)>,
    {
        let outer = self.0.read().await;
        outer.iter().map(|(k, r)| S::from((k, &r))).collect()
    }

    #[inline]
    pub async fn read(&self) -> RwLockReadGuard<'_, HashMap<K, Arc<R>>> {
        self.0.read().await
    }
}

impl<K, R> SharedLookup<K, R>
where
    K: Clone,
{
    pub async fn keys(&self) -> Vec<K> {
        self.0.read().await.keys().cloned().collect()
    }
}

impl<K, R, W> SharedLookupWriter<K, R, W>
where
    K: Clone + Eq + Hash,
    R: Reader<Writer = W>,
{
    pub async fn write(&mut self, key: &K) -> &mut W {
        match self.writers.entry(key.clone()) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(v) => {
                let (writer, reader) = R::new();
                let reader = Arc::new(reader);
                self.readers.write().await.insert(key.clone(), reader);
                v.insert(writer)
            }
        }
    }

    #[cfg(test)]
    pub async fn restore<S>(&mut self, snapshot: Vec<S>)
    where
        S: Into<(K, R, W)>,
    {
        self.writers.clear();
        let mut readers = self.readers.write().await;
        readers.clear();
        for s in snapshot.into_iter() {
            let (k, r, w) = s.into();
            self.writers.insert(k.clone(), w);
            readers.insert(k, Arc::new(r));
        }
    }
}
