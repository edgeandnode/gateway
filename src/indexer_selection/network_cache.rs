use crate::prelude::*;
use crate::{
    block_resolver::BlockResolver,
    indexer_selection::{
        utility::{concave_utility, SelectionFactor},
        BadIndexerReason, Context, SelectionError, UnresolvedBlock,
    },
};
use codecs::Encode as _;
use cost_model::QueryVariables;
use graphql_parser::query::{self as q, Number};
use neon_utils::marshalling::codecs;
use serde::{Deserialize, Serialize};
use single::Single as _;
use std::{collections::BTreeMap, convert::TryFrom};

pub struct NetworkCache;

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

#[derive(Debug, Eq, PartialEq)]
pub struct DeterministicQuery {
    pub blocks_behind: Option<u64>,
    pub query: String,
}

#[derive(Serialize, Deserialize)]
pub struct SerializableQuery {
    pub query: String,
    pub variables: QueryVariables,
}

impl BlockRequirements {
    fn parse_minimum_block(&mut self, number: &Number) -> Result<(), SelectionError> {
        let number = number
            .as_i64()
            .and_then(|n| u64::try_from(n).ok())
            .ok_or(SelectionError::BadInput)?;
        self.minimum_block = Some(self.minimum_block.unwrap_or_default().max(number));
        Ok(())
    }
}

// Creates this: { hash: "0xFF" }
fn block_hash_field<'a, T: q::Text<'a>>(hash: &Bytes32) -> BTreeMap<&'static str, q::Value<'a, T>> {
    BTreeMap::from_iter(std::iter::once(("hash", q::Value::String(hash.encode()))))
}

impl NetworkCache {
    pub async fn make_query_deterministic(
        context: &mut Context<'_>,
        block_resolver: &BlockResolver,
        latest_block: &BlockPointer,
        blocks_behind: u64,
    ) -> Result<DeterministicQuery, SelectionError> {
        // TODO: Ugh this code is a mess, and it's not even doing fragments yet.
        let mut query_requires_latest = false;
        let ops = &mut context.operations[..];
        for top_level_field in Self::top_level_fields(ops)? {
            let mut require_latest = true;
            for arg in top_level_field.arguments.iter_mut() {
                match arg {
                    ("block", block) => {
                        match block {
                            q::Value::Object(fields) => match fields.iter_mut().single() {
                                Ok((&"hash", _)) => require_latest = false,
                                Ok((&"number", number)) => {
                                    let number = Self::number(number, &context.variables)?;
                                    // Some, but not all, duplicated code
                                    // See also: ba6c90f1-3baf-45be-ac1c-f60733404436
                                    let block = block_resolver
                                        .resolve_block(UnresolvedBlock::WithNumber(number))
                                        .await?;
                                    require_latest = false;
                                    *fields = block_hash_field(&block.hash);
                                }
                                Ok((&"number_gte", number)) => {
                                    let number = Self::number(number, &context.variables)?;
                                    if latest_block.number < number {
                                        return Err(BadIndexerReason::BehindMinimumBlock.into());
                                    }
                                    *fields = block_hash_field(&latest_block.hash);
                                    require_latest = false;
                                    query_requires_latest = true;
                                }
                                _ => return Err(SelectionError::BadInput),
                            },
                            q::Value::Variable(name) => {
                                let var = context
                                    .variables
                                    .get(name)
                                    .ok_or(SelectionError::BadInput)?;
                                match var {
                                    q::Value::Object(fields) => match fields.iter().single() {
                                        Ok((name, _)) if name.as_str() == "hash" => {
                                            require_latest = false
                                        }
                                        Ok((name, number)) if name.as_str() == "number" => {
                                            let number = Self::number(number, &context.variables)?;
                                            // Some, but not all, duplicated code
                                            // See also: ba6c90f1-3baf-45be-ac1c-f60733404436
                                            let block = block_resolver
                                                .resolve_block(UnresolvedBlock::WithNumber(number))
                                                .await?;
                                            require_latest = false;
                                            let fields = block_hash_field(&block.hash);
                                            // This is different then the above which just replaces the
                                            // fields in the existing object, because we must not modify
                                            // the variable in case it's used elsewhere.
                                            *arg = (arg.0, q::Value::Object(fields));
                                        }
                                        Ok((name, number)) if name.as_str() == "number_gte" => {
                                            let number = Self::number(number, &context.variables)?;
                                            if latest_block.number < number {
                                                return Err(
                                                    BadIndexerReason::BehindMinimumBlock.into()
                                                );
                                            }
                                            let fields = block_hash_field(&latest_block.hash);
                                            // This is different then the above which just replaces the
                                            // fields in the existing object, because we must not modify
                                            // the variable in case it's used elsewhere.
                                            *arg = (arg.0, q::Value::Object(fields));
                                            require_latest = false;
                                            query_requires_latest = true;
                                        }
                                        _ => return Err(SelectionError::BadInput),
                                    },
                                    _ => return Err(SelectionError::BadInput),
                                }
                            }
                            _ => return Err(SelectionError::BadInput),
                        };
                    }
                    _ => {}
                }
            }
            if require_latest {
                let fields = block_hash_field(&latest_block.hash);
                let arg = ("block", q::Value::Object(fields));
                top_level_field.arguments.push(arg);
                query_requires_latest = true;
            }
        }
        let mut definitions = Vec::new();
        definitions.extend(
            context
                .fragments
                .iter()
                .cloned()
                .map(q::Definition::Fragment),
        );
        definitions.extend(
            context
                .operations
                .iter()
                .cloned()
                .map(q::Definition::Operation),
        );

        let query = q::Document { definitions };

        // TODO: (Performance) Could write these all to a string in one go to avoid an allocation and copy here.

        let query = SerializableQuery {
            query: query.to_string(),
            variables: context.variables.clone(),
        };

        // The query only maintains being behind if the latest block has
        // been requested. Otherwise it's not "behind", it's what is requested.
        let blocks_behind = if query_requires_latest {
            Some(blocks_behind)
        } else {
            None
        };

        let query = serde_json::to_string(&query).map_err(|_| SelectionError::BadInput)?;
        Ok(DeterministicQuery {
            blocks_behind,
            query,
        })
    }

    pub async fn freshness_requirements<'c>(
        operations: &mut [q::OperationDefinition<'c, &'c str>],
        block_resolver: &BlockResolver,
    ) -> Result<BlockRequirements, SelectionError> {
        let mut requirements = BlockRequirements::default();
        for top_level_field in Self::top_level_fields(operations)? {
            let mut has_latest = true;
            for arg in top_level_field.arguments.iter() {
                match arg {
                    ("block", q::Value::Object(fields)) => match fields.iter().single() {
                        Ok((&"number", q::Value::Int(number))) => {
                            requirements.parse_minimum_block(number)?;
                            has_latest = false;
                        }
                        Ok((&"number_gte", q::Value::Int(number))) => {
                            requirements.parse_minimum_block(number)?;
                        }
                        Ok((&"hash", q::Value::String(hash))) => {
                            let hash_bytes: [u8; 32] = codecs::decode(hash.as_str())
                                .map_err(|_| SelectionError::BadInput)?;
                            let hash = hash_bytes.into();
                            let number = block_resolver
                                .resolve_block(UnresolvedBlock::WithHash(hash))
                                .await?
                                .number;
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

    fn number<'t, T: q::Text<'t>>(
        number: &q::Value<'t, T>,
        variables: &QueryVariables,
    ) -> Result<u64, SelectionError> {
        let number = match number {
            q::Value::Int(i) => Ok(i.as_i64()),
            q::Value::Variable(name) => {
                let var = variables.get(name.as_ref());
                let var = match var {
                    Some(q::Value::Int(i)) => Ok(i.as_i64()),
                    _ => Err(SelectionError::BadInput),
                };
                var
            }
            _ => Err(SelectionError::BadInput),
        }?;
        number
            .and_then(|i| Some(u64::try_from(i)))
            .ok_or(SelectionError::BadInput)?
            .map_err(|_| SelectionError::BadInput)
    }
}

impl DataFreshness {
    pub fn blocks_behind(&self) -> Result<u64, BadIndexerReason> {
        self.blocks_behind
            .ok_or(BadIndexerReason::MissingIndexingStatus)
    }

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
        blocks_behind: u64,
    ) -> Result<SelectionFactor, SelectionError> {
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
        if requirements.has_latest {
            let utility = {
                if blocks_behind == 0 {
                    1.0
                } else {
                    let freshness = 1.0 / blocks_behind as f64;
                    concave_utility(freshness, u_a)
                }
            };
            Ok(SelectionFactor::one(utility))
        } else {
            Ok(SelectionFactor::zero())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block_resolver::BlockResolver,
        indexer_selection::{test_utils::gen_blocks, Context},
    };

    #[tokio::test]
    async fn requirements_field() {
        requirements_test(
            &BlockResolver::test(&[]),
            "query { a }",
            "",
            BlockRequirements {
                minimum_block: None,
                has_latest: true,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn requirements_arg() {
        requirements_test(
            &BlockResolver::test(&[]),
            "query { a(abc: true) }",
            "",
            BlockRequirements {
                minimum_block: None,
                has_latest: true,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn requirements_block_number() {
        requirements_test(
            &BlockResolver::test(&[]),
            "query { a(block: { number: 10}) }",
            "",
            BlockRequirements {
                minimum_block: Some(10),
                has_latest: false,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn requirements_multiple_block_numbers() {
        requirements_test(
            &BlockResolver::test(&[]),
            "query { a(block: { number: 10 }) a(block: { number: 20 }) }",
            "",
            BlockRequirements {
                minimum_block: Some(20),
                has_latest: false,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn requirements_block_hash() {
        let blocks = gen_blocks(&[54321]);
        let resolver = &BlockResolver::test(&blocks);
        requirements_test(
            &resolver,
            &format!(
                "query {{ a(block: {{ hash: {:?} }}) }}",
                hex::encode(&*blocks[0].hash)
            ),
            "",
            BlockRequirements {
                minimum_block: Some(54321),
                has_latest: false,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn block_number_gte_requirements() {
        requirements_test(
            &BlockResolver::test(&[]),
            "query { a(block: { number_gte: 10 }) }",
            "",
            BlockRequirements {
                minimum_block: Some(10),
                has_latest: true,
            },
        )
        .await;
    }

    #[tokio::test]
    async fn block_number_gte_determinism() {
        let blocks = gen_blocks(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let resolver = BlockResolver::test(&blocks);
        let mut context = Context::new("query { a(block: { number_gte: 4 }) }", "").unwrap();
        let blocks_behind = 2;
        let latest = resolver
            .resolve_block(UnresolvedBlock::WithNumber(7 - blocks_behind))
            .await
            .unwrap();
        let result =
            NetworkCache::make_query_deterministic(&mut context, &resolver, &latest, blocks_behind)
                .await;
        assert_eq!(
                result,
                Ok(DeterministicQuery {
                    blocks_behind: Some(blocks_behind),
                    query: "{\"query\":\"query {\\n  a(block: {hash: \\\"0x0500000000000000000000000000000000000000000000000000000000000000\\\"})\\n}\\n\",\"variables\":{}}".to_owned()
                })
            );
    }

    async fn requirements_test(
        block_resolver: &BlockResolver,
        query: &str,
        variables: &str,
        expect: BlockRequirements,
    ) {
        let mut context = Context::new(query, variables).unwrap();
        let requirements =
            NetworkCache::freshness_requirements(context.operations.as_mut_slice(), block_resolver)
                .await
                .unwrap();
        assert_eq!(requirements, expect);
    }
}
