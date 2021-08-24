use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::{RwLock, RwLockReadGuard};

pub trait Reader {
    type Writer;
    fn new() -> (Self::Writer, Self);
}

#[derive(Clone)]
pub struct SharedLookup<K, R>(Arc<RwLock<HashMap<K, R>>>);

pub struct SharedLookupWriter<K, R, W> {
    readers: Arc<RwLock<HashMap<K, R>>>,
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

    pub async fn get(&self, key: &K) -> Option<RwLockReadGuard<'_, R>> {
        let outer = self.0.read().await;
        if !outer.contains_key(key) {
            return None;
        }
        Some(RwLockReadGuard::map(outer, |outer| outer.get(key).unwrap()))
    }

    pub async fn snapshot<S>(&self) -> Vec<S>
    where
        S: for<'k, 'r> From<(&'k K, &'r R)>,
    {
        self.0.read().await.iter().map(From::from).collect()
    }

    #[inline]
    pub async fn read(&self) -> RwLockReadGuard<'_, HashMap<K, R>> {
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
        self.add_entry(key).await;
        self.writers.get_mut(key).unwrap()
    }

    pub async fn read(&mut self, key: &K) -> RwLockReadGuard<'_, R> {
        self.add_entry(key).await;
        RwLockReadGuard::map(self.readers.read().await, |outer| outer.get(key).unwrap())
    }

    async fn add_entry(&mut self, key: &K) {
        if !self.writers.contains_key(key) {
            let (writer, reader) = R::new();
            self.writers.insert(key.clone(), writer);
            self.readers.write().await.insert(key.clone(), reader);
        }
    }

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
            readers.insert(k, r);
        }
    }
}
