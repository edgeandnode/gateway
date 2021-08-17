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

    pub async fn with_value<T, F>(&self, key: &K, f: F) -> Option<T>
    where
        F: FnOnce(&R) -> T,
    {
        self.0.read().await.get(key).map(|v| f(v))
    }

    // TODO: rm
    pub async fn with_value_mut<T, F>(&self, key: &K, f: F) -> Option<T>
    where
        F: FnOnce(&mut R) -> T,
    {
        self.0.write().await.get_mut(key).map(|v| f(v))
    }

    pub async fn get(&self, key: &K) -> Option<RwLockReadGuard<'_, R>> {
        let outer = self.0.read().await;
        // TODO: only get once
        match outer.get(key) {
            Some(_) => Some(RwLockReadGuard::map(outer, |outer| outer.get(key).unwrap())),
            None => None,
        }
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
    pub async fn update<F>(&mut self, key: &K, f: F)
    where
        F: FnOnce(&mut W),
    {
        let writer = match self.writers.get_mut(key) {
            Some(writer) => writer,
            None => {
                let (writer, reader) = R::new();
                self.writers.insert(key.clone(), writer);
                self.readers.write().await.insert(key.clone(), reader);
                self.writers.get_mut(key).unwrap()
            }
        };
        f(writer);
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
