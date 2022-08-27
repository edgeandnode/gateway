use crate::indexer_selection::{
    BadIndexerReason, BlockResolver, Context, SelectionError, UnresolvedBlock,
};
use cost_model::QueryVariables;
use graphql_parser::query::{self as q, Number};
use itertools::Itertools as _;
use prelude::*;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, convert::TryFrom};

#[derive(Default, Debug, Eq, PartialEq)]
pub struct BlockRequirements {
    // If specified, the subgraph must have indexed
    // up to at least this number.
    pub minimum_block: Option<u64>,
    // If true, the query has an unspecified block
    // which means the query benefits from syncing as far
    // in the future as possible.
    pub has_latest: bool,
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
    BTreeMap::from_iter(std::iter::once((
        "hash",
        q::Value::String(hash.to_string()),
    )))
}

pub async fn make_query_deterministic(
    context: &mut Context<'_>,
    block_resolver: &impl BlockResolver,
    latest_block: &BlockPointer,
    blocks_behind: u64,
) -> Result<DeterministicQuery, SelectionError> {
    // TODO: Ugh this code is a mess, and it's not even doing fragments yet.
    let mut query_requires_latest = false;
    let ops = &mut context.operations[..];
    for top_level_field in top_level_fields(ops)? {
        let mut require_latest = true;
        for arg in top_level_field.arguments.iter_mut() {
            match arg {
                ("block", block) => {
                    match block {
                        q::Value::Object(fields) => match fields.iter_mut().at_most_one() {
                            Ok(Some((&"hash", _))) => require_latest = false,
                            Ok(Some((&"number", number))) => {
                                let number = parse_number(number, &context.variables)?;
                                // Some, but not all, duplicated code
                                // See also: ba6c90f1-3baf-45be-ac1c-f60733404436
                                let block = block_resolver
                                    .resolve_block(UnresolvedBlock::WithNumber(number))
                                    .await?;
                                require_latest = false;
                                *fields = block_hash_field(&block.hash);
                            }
                            Ok(Some((&"number_gte", number))) => {
                                let number = parse_number(number, &context.variables)?;
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
                                q::Value::Object(fields) => match fields.iter().at_most_one() {
                                    Ok(Some((name, _))) if name.as_str() == "hash" => {
                                        require_latest = false
                                    }
                                    Ok(Some((name, number))) if name.as_str() == "number" => {
                                        let number = parse_number(number, &context.variables)?;
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
                                    Ok(Some((name, number))) if name.as_str() == "number_gte" => {
                                        let number = parse_number(number, &context.variables)?;
                                        if latest_block.number < number {
                                            return Err(BadIndexerReason::BehindMinimumBlock.into());
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
    block_resolver: &impl BlockResolver,
) -> Result<BlockRequirements, SelectionError> {
    let mut requirements = BlockRequirements::default();
    for top_level_field in top_level_fields(operations)? {
        let mut has_latest = true;
        for arg in top_level_field.arguments.iter() {
            match arg {
                ("block", q::Value::Object(fields)) => match fields.iter().at_most_one() {
                    Ok(Some((&"number", q::Value::Int(number)))) => {
                        requirements.parse_minimum_block(number)?;
                        has_latest = false;
                    }
                    Ok(Some((&"number_gte", q::Value::Int(number)))) => {
                        requirements.parse_minimum_block(number)?;
                    }
                    Ok(Some((&"hash", q::Value::String(hash)))) => {
                        let hash = hash
                            .as_str()
                            .parse::<Bytes32>()
                            .map_err(|_| SelectionError::BadInput)?;
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

fn parse_number<'t, T: q::Text<'t>>(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer_selection::{
        test_utils::{gen_blocks, TestBlockResolver},
        BlockResolver, Context,
    };

    #[tokio::test]
    async fn requirements_field() {
        requirements_test(
            &TestBlockResolver::new(vec![]),
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
            &TestBlockResolver::new(vec![]),
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
            &TestBlockResolver::new(vec![]),
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
            &TestBlockResolver::new(vec![]),
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
        let resolver = &TestBlockResolver::new(blocks.clone());
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
            &TestBlockResolver::new(vec![]),
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
        let resolver = TestBlockResolver::new(blocks.clone());
        let mut context = Context::new("query { a(block: { number_gte: 4 }) }", "").unwrap();
        let blocks_behind = 2;
        let latest = resolver
            .resolve_block(UnresolvedBlock::WithNumber(7 - blocks_behind))
            .await
            .unwrap();
        let result =
            make_query_deterministic(&mut context, &resolver, &latest, blocks_behind).await;
        assert_eq!(
                result,
                Ok(DeterministicQuery {
                    blocks_behind: Some(blocks_behind),
                    query: "{\"query\":\"query {\\n  a(block: {hash: \\\"0x0500000000000000000000000000000000000000000000000000000000000000\\\"})\\n}\\n\",\"variables\":{}}".to_owned()
                })
            );
    }

    async fn requirements_test(
        block_resolver: &TestBlockResolver,
        query: &str,
        variables: &str,
        expect: BlockRequirements,
    ) {
        let mut context = Context::new(query, variables).unwrap();
        let requirements =
            freshness_requirements(context.operations.as_mut_slice(), block_resolver)
                .await
                .unwrap();
        assert_eq!(requirements, expect);
    }
}
