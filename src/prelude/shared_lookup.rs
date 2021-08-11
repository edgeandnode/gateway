use std::{collections::HashMap, hash::Hash, sync::Arc};
use tokio::sync::RwLock;

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

    pub async fn snapshot<S>(&self) -> Vec<S>
    where
        S: for<'k, 'r> From<(&'k K, &'r R)>,
    {
        self.0.read().await.iter().map(From::from).collect()
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
