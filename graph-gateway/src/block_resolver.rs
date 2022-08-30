use crate::{ethereum_client, metrics::*};
use async_trait::async_trait;
use im;
pub use indexer_selection::BlockResolver as _;
use indexer_selection::{self, UnresolvedBlock};
use prelude::*;

#[derive(Clone)]
pub struct BlockResolver {
    network: String,
    cache: Eventual<Ptr<BlockCache>>,
    chain_client: mpsc::Sender<ethereum_client::Msg>,
    skip_latest: usize,
}

#[async_trait]
impl indexer_selection::BlockResolver for BlockResolver {
    fn latest_block(&self) -> Option<BlockPointer> {
        let cache = self.cache.value_immediate()?;
        let index = cache.head.len().saturating_sub(self.skip_latest + 1);
        cache.head.get(index).cloned()
    }

    fn skip_latest(&mut self, skip: usize) {
        self.skip_latest = skip;
    }

    async fn resolve_block(
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
        let result = self
            .fetch_cache_miss(unresolved.clone())
            .await
            .ok_or(unresolved);
        METRICS.block_resolution.check(&[&self.network], &result);
        result
    }
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

    async fn fetch_cache_miss(&self, unresolved: UnresolvedBlock) -> Option<BlockPointer> {
        let _timer = METRICS.block_resolution.start_timer(&[&self.network]);
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

#[cfg(test)]
mod tests {
    use super::*;
    use indexer_selection::test_utils::gen_blocks;

    /// Skipping some number of blocks from latest does not require all
    /// blocks between the latest and the skipped to.
    #[test]
    fn does_not_require_intermediates() {
        let blocks = gen_blocks(&[12, 7]);
        let resolver = BlockResolver::test(&blocks);
        assert_eq!(resolver.latest_block(), Some(blocks[0].clone()));
    }
}
