use crate::{ethereum_client, indexer_selection::UnresolvedBlock};
use im;
use lazy_static::lazy_static;
use prelude::*;

#[derive(Clone)]
pub struct BlockResolver {
    network: String,
    cache: Eventual<Ptr<BlockCache>>,
    chain_client: mpsc::Sender<ethereum_client::Msg>,
    skip_latest: usize,
}

impl BlockResolver {
    pub fn new(
        network: String,
        cache: Eventual<Ptr<BlockCache>>,
        chain_client: mpsc::Sender<ethereum_client::Msg>,
    ) -> Self {
        Self {
            network,
            cache,
            chain_client,
            skip_latest: 0,
        }
    }

    #[cfg(test)]
    pub fn test(blocks: &[BlockPointer]) -> BlockResolver {
        let (mut cache_writer, cache) = BlockCache::new(blocks.len(), 0);
        let (dummy, _) = mpsc::channel(1);
        let resolver = BlockResolver::new("test".to_string(), cache, dummy);
        for block in blocks {
            cache_writer.insert(block.clone(), &[]);
        }
        resolver
    }

    pub fn latest_block(&self) -> Option<BlockPointer> {
        let cache = self.cache.value_immediate()?;
        let index = cache.head.len().saturating_sub(self.skip_latest + 1);
        cache.head.get(index).cloned()
    }

    pub fn skip_latest(&mut self, skip: usize) {
        self.skip_latest = skip;
    }

    pub async fn resolve_block(
        &self,
        unresolved: UnresolvedBlock,
    ) -> Result<BlockPointer, UnresolvedBlock> {
        let cache = self
            .cache
            .value_immediate()
            .ok_or_else(|| unresolved.clone())?;
        if let Some(block) = cache.fetch(unresolved.clone()) {
            with_metric(&METRICS.block_cache_hit, &[&self.network], |c| c.inc());
            return Ok(block);
        }
        with_metric(&METRICS.block_cache_miss, &[&self.network], |c| c.inc());
        match self.fetch_cache_miss(unresolved.clone()).await {
            Some(block) => {
                with_metric(&METRICS.block_resolution.ok, &[&self.network], |c| c.inc());
                Ok(block)
            }
            None => {
                tracing::error!("block resolver connection closed");
                with_metric(&METRICS.block_resolution.failed, &[&self.network], |c| {
                    c.inc()
                });
                Err(unresolved)
            }
        }
    }

    async fn fetch_cache_miss(&self, unresolved: UnresolvedBlock) -> Option<BlockPointer> {
        let _block_resolution_timer =
            with_metric(&METRICS.block_resolution.duration, &[&self.network], |h| {
                h.start_timer()
            });
        let (sender, receiver) = oneshot::channel();
        self.chain_client
            .send(ethereum_client::Msg::Request(unresolved, sender))
            .await
            .ok()?;
        receiver.await.ok()
    }
}

#[derive(Clone)]
pub struct BlockCache {
    head: im::Vector<BlockPointer>,
    hash_to_number: im::HashMap<Bytes32, u64>,
    number_to_hash: im::HashMap<u64, Bytes32>,
}

impl BlockCache {
    pub fn new(
        chain_head_length: usize,
        cache_size: usize,
    ) -> (BlockCacheWriter, Eventual<Ptr<BlockCache>>) {
        let (cache_writer, reader) = Eventual::new();
        let writer = BlockCacheWriter {
            chain_head_length,
            cache_size,
            cache: BlockCache {
                head: im::Vector::new(),
                hash_to_number: im::HashMap::new(),
                number_to_hash: im::HashMap::new(),
            },
            writer: cache_writer,
            last_update: im::OrdMap::new(),
        };
        (writer, reader)
    }

    pub fn fetch(&self, unresolved: UnresolvedBlock) -> Option<BlockPointer> {
        if let Some(block) = self.head.iter().find(|b| unresolved.matches(b)) {
            return Some(block.clone());
        }
        let (hash, number) = match unresolved {
            UnresolvedBlock::WithHash(hash) => (hash, *self.hash_to_number.get(&hash)?),
            UnresolvedBlock::WithNumber(number) => (*self.number_to_hash.get(&number)?, number),
        };
        Some(BlockPointer { hash, number })
    }
}

pub struct BlockCacheWriter {
    /// The amount of blocks behind chain head that are susceptible to reorgs.
    chain_head_length: usize,
    /// Size of cache containing confirmed blocks.
    cache_size: usize,
    cache: BlockCache,
    writer: EventualWriter<Ptr<BlockCache>>,
    last_update: im::OrdMap<Instant, u64>,
}

impl BlockCacheWriter {
    pub fn insert(&mut self, block: BlockPointer, uncles: &[Bytes32]) {
        // Remove uncles
        let head = std::mem::replace(&mut self.cache.head, im::Vector::new());
        self.cache.head = head
            .into_iter()
            .filter(|block| uncles.iter().all(|uncle| &block.hash != uncle))
            .collect();
        for uncle in uncles {
            let number = match self.cache.hash_to_number.remove(uncle) {
                Some(number) => number,
                None => continue,
            };
            self.cache.number_to_hash.remove(&number);
        }

        // Insert block
        let height = self.cache.head.last().map(|b| b.number).unwrap_or(0);
        if (block.number > height) || ((height - block.number) < self.chain_head_length as u64) {
            if self.cache.head.len() >= self.chain_head_length {
                self.cache.head.pop_front();
            }
            self.cache.head.insert_ord(block);
        } else {
            if self.cache.hash_to_number.len() >= self.cache_size {
                if let Some(number) = self.last_update.get_min().map(|(_, number)| number) {
                    if let Some(hash) = self.cache.number_to_hash.remove(number) {
                        self.cache.hash_to_number.remove(&hash);
                    }
                }
            }
            self.cache.hash_to_number.insert(block.hash, block.number);
            self.cache.number_to_hash.insert(block.number, block.hash);
            self.last_update.insert(Instant::now(), block.number);
        }

        // Broadcast new version
        self.writer.write(Ptr::new(self.cache.clone()));
    }
}

lazy_static! {
    static ref METRICS: Metrics = Metrics::new();
}

struct Metrics {
    block_resolution: ResponseMetricVecs,
    block_cache_hit: prometheus::IntCounterVec,
    block_cache_miss: prometheus::IntCounterVec,
}

impl Metrics {
    fn new() -> Self {
        Self {
            block_resolution: ResponseMetricVecs::new(
                "gateway_block_resolution",
                "block requests",
                &["network"],
            ),
            block_cache_hit: prometheus::register_int_counter_vec!(
                "block_cache_hit",
                "Number of block cache hits",
                &["network"]
            )
            .unwrap(),
            block_cache_miss: prometheus::register_int_counter_vec!(
                "block_cache_miss",
                "Number of cache misses",
                &["network"]
            )
            .unwrap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer_selection::test_utils::gen_blocks;

    /// Skipping some number of blocks from latest does not require all
    /// blocks between the latest and the skipped to.
    #[test]
    fn does_not_require_intermediates() {
        let blocks = gen_blocks(&[12, 7]);
        let resolver = BlockResolver::test(&blocks);
        assert_eq!(resolver.latest_block(), Some(blocks[0].clone()));
    }
}
