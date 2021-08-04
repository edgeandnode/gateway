use crate::indexer_selection::{SelectionError, UnresolvedBlock};
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer_selection::Context;
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
}
