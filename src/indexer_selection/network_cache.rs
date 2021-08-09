use crate::indexer_selection::{
    utility::{concave_utility, SelectionFactor},
    BadIndexerReason, SelectionError, UnresolvedBlock,
};
use crate::prelude::*;
use graphql_parser::query as q;
use neon_utils::marshalling::codecs;
use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
};

#[derive(Default)]
pub struct NetworkCache {
    // TODO: SOA Vec
    networks: Vec<String>,
    caches: Vec<BlockCache>,
}

#[derive(Default)]
pub struct BlockCache {
    hash_to_number: HashMap<Bytes32, u64>,
    // A BTreeMap is used here to quickly refer to the top, even as items may be
    // removed.
    number_to_hash: BTreeMap<u64, Bytes32>,
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct BlockRequirements {
    // If specified, the subgraph must have indexed
    // up to at least this number.
    minimum_block: Option<u64>,
    // If true, the query has an unspecified block
    // which means the query benefits from syncing as far
    // in the future as possible.
    has_latest: bool,
}

#[derive(Clone, Debug, Default)]
pub struct DataFreshness {
    blocks_behind: Option<u64>,
    highest_reported_block: Option<u64>,
}

impl NetworkCache {
    pub fn freshness_requirements<'c>(
        &self,
        operations: &mut [q::OperationDefinition<'c, &'c str>],
        network: &str,
    ) -> Result<BlockRequirements, SelectionError> {
        let mut requirements = BlockRequirements::default();
        for top_level_field in Self::top_level_fields(operations)? {
            let mut has_latest = true;
            for arg in top_level_field.arguments.iter() {
                match arg {
                    ("block", q::Value::Object(fields)) => match fields.iter().next() {
                        Some((&"number", q::Value::Int(number))) => {
                            let number = number.as_i64().ok_or(SelectionError::BadInput)?;
                            let number =
                                u64::try_from(number).map_err(|_| SelectionError::BadInput)?;
                            requirements.minimum_block =
                                Some(requirements.minimum_block.unwrap_or_default().max(number));
                            has_latest = false;
                        }
                        Some((&"hash", q::Value::String(hash))) => {
                            let hash_bytes: [u8; 32] = codecs::decode(hash.as_str())
                                .map_err(|_| SelectionError::BadInput)?;
                            let hash = hash_bytes.into();
                            let number = self
                                .hash_to_number(network, &hash)
                                .ok_or(UnresolvedBlock::WithHash(hash))?;
                            requirements.minimum_block =
                                Some(requirements.minimum_block.unwrap_or_default().max(number));
                            has_latest = false;
                        }
                        _ => return Err(SelectionError::BadInput),
                    },
                    _ => {}
                }
            }
            requirements.has_latest = has_latest || requirements.has_latest;
        }
        Ok(requirements)
    }

    fn top_level_fields<'a, 'c>(
        ops: &'a mut [q::OperationDefinition<'c, &'c str>],
    ) -> Result<Vec<&'a mut q::Field<'c, &'c str>>, SelectionError> {
        fn top_level_fields_from_set<'a, 'c>(
            set: &'a mut q::SelectionSet<'c, &'c str>,
            result: &mut Vec<&'a mut q::Field<'c, &'c str>>,
        ) -> Result<(), SelectionError> {
            for item in set.items.iter_mut() {
                match item {
                    q::Selection::Field(field) => result.push(field),
                    q::Selection::FragmentSpread(_) | q::Selection::InlineFragment(_) => {
                        return Err(SelectionError::BadInput);
                    }
                }
            }
            Ok(())
        }
        let mut result = Vec::new();
        for op in ops.iter_mut() {
            match op {
                q::OperationDefinition::Query(query) => {
                    if query.directives.len() != 0 {
                        return Err(SelectionError::BadInput);
                    }
                    top_level_fields_from_set(&mut query.selection_set, &mut result)?;
                }
                q::OperationDefinition::SelectionSet(set) => {
                    top_level_fields_from_set(set, &mut result)?;
                }
                q::OperationDefinition::Mutation(_) | q::OperationDefinition::Subscription(_) => {
                    return Err(SelectionError::BadInput);
                }
            }
        }
        Ok(result)
    }

    fn hash_to_number(&self, network: &str, hash: &Bytes32) -> Option<u64> {
        let i = self.networks.iter().position(|v| v == network)?;
        self.caches[i].hash_to_number.get(hash).cloned()
    }

    pub fn set_block(&mut self, network: &str, block: BlockPointer) {
        let cache = self.block_cache(network);
        if let Some(prev) = cache.number_to_hash.insert(block.number, block.hash) {
            cache.hash_to_number.remove(&prev);
        }
        cache.hash_to_number.insert(block.hash, block.number);
    }

    fn block_cache(&mut self, network: &str) -> &mut BlockCache {
        let i = match self.networks.iter().position(|v| v == network) {
            Some(i) => i,
            None => {
                self.networks.push(network.to_string());
                self.caches.push(BlockCache::default());
                self.caches.len() - 1
            }
        };
        &mut self.caches[i]
    }
}

impl DataFreshness {
    pub fn set_blocks_behind(&mut self, blocks: u64, highest: u64) {
        self.blocks_behind = Some(blocks);
        self.highest_reported_block = Some(highest)
    }

    pub fn observe_indexing_behind(
        &mut self,
        freshness_requirements: &Result<BlockRequirements, SelectionError>,
        latest: u64,
    ) {
        let minimum_block = if let Ok(m) = freshness_requirements {
            assert!(
                !m.has_latest,
                "Observe indexing behind should only take deterministic queries"
            );

            if let Some(m) = m.minimum_block {
                m
            } else {
                // TODO: Give the indexer a harsh penalty here. The only way to reach this
                // would be if they returned that the block was unknown or not indexed
                // for a query with an empty selection set.
                // For now, resetting their indexing status will give them a temporary
                // ban that will expire when the indexing status API is queried.
                // This should suffice until there is a reputation enabled.
                self.highest_reported_block = None;
                self.blocks_behind = None;
                return;
            }
        } else {
            // If we get here it means that there was a re-org. We observed a block
            // hash in the query that we could no longer associate with a number. The Indexer
            // receives no penalty.
            return;
        };

        let blocks_behind = if let Some(blocks_behind) = self.blocks_behind {
            blocks_behind
        } else {
            // If we get here it means something else already dealt with this,
            // possibly with a temporary ban.
            return;
        };

        // There's two cases here. One is that they said they have a block, but don't.
        // The other is we assume they have a block, but don't.
        // This is disabled because there isn't yet implemented an appropriate
        // penalty and this can happen normally just for timing issues. Unfortunately
        // it's hard to tell which is which. This needs to be a penalty that is increasingly
        // severe so that malicious actors are penalized but its ok for occasional timing issues.
        // Reputation fits this, but isn't implemented yet.
        // TODO: if highest_reported_block > minimum_block
        // { penalize }

        // They are at least one block behind the assumed status (this
        // will usually be the case). In some cases for timing issues
        // they may have already reported they are even farther behind,
        // so we assume the worst of the two.
        let min_behind = latest.saturating_sub(minimum_block) + 1;
        self.blocks_behind = Some(latest.min(blocks_behind.max(min_behind)));
    }

    pub fn expected_utility(
        &self,
        requirements: &BlockRequirements,
        u_a: f64,
        latest_block: u64,
    ) -> Result<(SelectionFactor, u64), SelectionError> {
        let blocks_behind = self
            .blocks_behind
            .ok_or(BadIndexerReason::MissingIndexingStatus)?;
        // Require the Indexer to have synced at least up to the minimum block
        if let Some(minimum) = requirements.minimum_block {
            let our_latest = latest_block.saturating_sub(blocks_behind);
            if our_latest < minimum {
                return Err(BadIndexerReason::BehindMinimumBlock.into());
            }
        }
        // Add utility if the latest block is requested. Otherwise,
        // data freshness is not a utility, but a binary of minimum block.
        // (Note that it can be both).
        let factor = if requirements.has_latest {
            let utility = {
                if blocks_behind == 0 {
                    1.0
                } else {
                    let freshness = 1.0 / blocks_behind as f64;
                    concave_utility(freshness, u_a)
                }
            };
            SelectionFactor::one(utility)
        } else {
            SelectionFactor::zero()
        };
        Ok((factor, blocks_behind))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer_selection::Context;
    use crate::prelude::test_utils::bytes_from_id;
    use codecs::Encode as _;

    #[test]
    fn requirements_field() {
        requirements_test(
            &NetworkCache::default(),
            "query { a }",
            "",
            BlockRequirements {
                minimum_block: None,
                has_latest: true,
            },
        );
    }

    #[test]
    fn requirements_arg() {
        requirements_test(
            &NetworkCache::default(),
            "query { a(abc: true) }",
            "",
            BlockRequirements {
                minimum_block: None,
                has_latest: true,
            },
        );
    }

    #[test]
    fn requirements_block_number() {
        requirements_test(
            &NetworkCache::default(),
            "query { a(block: { number: 10}) }",
            "",
            BlockRequirements {
                minimum_block: Some(10),
                has_latest: false,
            },
        );
    }

    #[test]
    fn requirements_multiple_block_numbers() {
        requirements_test(
            &NetworkCache::default(),
            "query { a(block: { number: 10}) a(block: { number: 20}) }",
            "",
            BlockRequirements {
                minimum_block: Some(20),
                has_latest: false,
            },
        );
    }

    #[test]
    fn requirements_block_hash() {
        let mut network_cache = NetworkCache::default();
        let hash = test_utils::bytes_from_id(54321).into();
        network_cache.set_block("mainnet", BlockPointer { number: 50, hash });
        requirements_test(
            &network_cache,
            &format!("query {{ a(block: {{ hash: {:?}}}) }}", hash.encode()),
            "",
            BlockRequirements {
                minimum_block: Some(50),
                has_latest: false,
            },
        );
    }

    fn requirements_test(
        network_cache: &NetworkCache,
        query: &str,
        variables: &str,
        expect: BlockRequirements,
    ) {
        let mut context = Context::new(query, variables).unwrap();
        let requirements = network_cache
            .freshness_requirements(context.operations.as_mut_slice(), "mainnet")
            .unwrap();
        assert_eq!(requirements, expect);
    }

    /* TODO: Finish all this
    #[test]
    fn variable_as_block() {
        let mut network_cache = NetworkCache::default();
        let hash = test_utils::bytes_from_id(54321).into();
        network_cache.set_block("mainnet", BlockPointer { number: 50, hash });
        // FIXME: Variables
        requirements_test(
            &network_cache,
            "query BlockVars($block: Block) { a(block: $block) }",
            &format!("{}", hash.encode()),
            BlockRequirements {
                minimum_block: Some(50),
                has_latest: false,
            },
        );
    }

    #[test]
    fn variable_as_number() {
        todo!()
    }

    #[test]
    fn variable_as_hash() {
        todo!()
    }

    #[test]
    fn skipped_requirement() {
        todo!()
    }
    */

    #[test]
    fn can_get_latest() {
        let blocks = gen_blocks(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let cache = cache_with("", &blocks);
        assert_eq!(cache.latest_block("", 0), Ok(blocks[7].clone()));
    }

    #[test]
    fn missing() {
        let blocks = gen_blocks(&[0, 1, 2, 3, 5, 6, 7]);
        let cache = cache_with("", &blocks);
        assert_eq!(
            cache.latest_block("", 3),
            Err(UnresolvedBlock::WithNumber(4))
        );
    }

    #[test]
    fn missing_head() {
        let blocks = gen_blocks(&[0, 1, 2, 3]);
        let cache = cache_with("", &blocks);
        assert_eq!(cache.latest_block("", 7), Ok(blocks[0].clone()));
    }

    /// Skipping some number of blocks from latest does not require all
    /// blocks between the latest and the skipped to.
    #[test]
    fn does_not_require_intermediates() {
        let blocks = gen_blocks(&[12, 7]);
        let cache = cache_with("", &blocks);
        assert_eq!(cache.latest_block("", 5), Ok(blocks[1].clone()));
    }

    fn gen_blocks(numbers: &[u64]) -> Vec<BlockPointer> {
        numbers
            .iter()
            .map(|&number| BlockPointer {
                number,
                hash: bytes_from_id(number as usize).into(),
            })
            .collect()
    }

    fn cache_with(network: &str, blocks: &[BlockPointer]) -> NetworkCache {
        let mut cache = NetworkCache::default();
        for block in blocks.iter() {
            cache.set_block(network, block.clone());
        }
        cache
    }
}
