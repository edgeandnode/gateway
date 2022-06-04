use crate::prelude::Reader;
use std::{
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    sync::Arc,
};
use tokio::sync::RwLock;

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
}
