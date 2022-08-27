use std::{ops::Deref, sync::Arc};
use tokio::sync::RwLock;

struct DoubleBufferData<T> {
    values: [RwLock<T>; 2],
}

#[macro_export]
macro_rules! double_buffer {
    ($val:expr) => {{
        let (a, b) = ($val, $val);
        $crate::double_buffer::reader_writer_pair(a, b)
    }};
}

/// a & b must be equal.
/// The API takes two values and does not verify equality to avoid requiring implementing
/// traits such as [Clone, Eq, Default]
pub fn reader_writer_pair<T>(a: T, b: T) -> (DoubleBufferReader<T>, DoubleBufferWriter<T>) {
    let data = Arc::new(DoubleBufferData {
        values: [RwLock::new(a), RwLock::new(b)],
    });
    (
        DoubleBufferReader { data: data.clone() },
        DoubleBufferWriter { data },
    )
}

pub struct DoubleBufferReader<T> {
    data: Arc<DoubleBufferData<T>>,
}

impl<T> Clone for DoubleBufferReader<T> {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
        }
    }
}

pub struct DoubleBufferWriter<T> {
    data: Arc<DoubleBufferData<T>>,
}

impl<T> DoubleBufferWriter<T> {
    /// f must be pure (though, it may hold data).
    /// The function will be called twice, once to update
    /// each snapshot.
    pub async fn update<F>(&mut self, f: F)
    where
        F: Fn(&mut T),
    {
        for value in &self.data.values {
            let mut write = value.write().await;
            f(&mut write);
        }
    }
}

impl<T> DoubleBufferReader<T> {
    pub fn latest<'a>(&'a self) -> impl 'a + Deref<Target = T> {
        loop {
            // This is guaranteed to only move forward in time,
            // and is almost guaranteed to acquire the lock "immediately".
            // These guarantees come from the invariant that there is
            // a single writer and it can only be in a few possible states.
            for value in &self.data.values {
                if let Ok(data) = value.try_read() {
                    return data;
                }
            }
        }
    }
}
