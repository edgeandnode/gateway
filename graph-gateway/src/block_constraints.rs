use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{BlockHash, BlockNumber};
use anyhow::{anyhow, bail};
use cost_model::Context;
use graphql::graphql_parser::query::{
    Definition, Document, OperationDefinition, Selection, Text, Value,
};
use graphql::{IntoStaticValue as _, StaticValue};
use indexer_selection::UnresolvedBlock;
use itertools::Itertools as _;
use serde_json::{self, json};
use thegraph::types::BlockPointer;

use crate::errors::Error;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum BlockConstraint {
    Unconstrained,
    Hash(BlockHash),
    Number(BlockNumber),
    NumberGTE(BlockNumber),
}

impl BlockConstraint {
    pub fn into_unresolved(self) -> Option<UnresolvedBlock> {
        match self {
            Self::Unconstrained => None,
            Self::Hash(h) => Some(UnresolvedBlock::WithHash(h)),
            Self::Number(n) | Self::NumberGTE(n) => Some(UnresolvedBlock::WithNumber(n)),
        }
    }
}

pub fn block_constraints<'c>(
    context: &'c Context<'c, String>,
) -> Result<BTreeSet<BlockConstraint>, Error> {
    let mut constraints = BTreeSet::new();
    let vars = &context.variables;
    // ba6c90f1-3baf-45be-ac1c-f60733404436
    for operation in &context.operations {
        let (selection_set, defaults) = match operation {
            OperationDefinition::SelectionSet(selection_set) => {
                (selection_set, BTreeMap::default())
            }
            OperationDefinition::Query(query) if query.directives.is_empty() => {
                // Add default definitions for variables not set at top level.
                let defaults = query
                    .variable_definitions
                    .iter()
                    .filter(|d| !vars.0.contains_key(&d.name))
                    .filter_map(|d| Some((d.name.clone(), d.default_value.as_ref()?.to_graphql())))
                    .collect::<BTreeMap<String, StaticValue>>();
                (&query.selection_set, defaults)
            }
            OperationDefinition::Query(_)
            | OperationDefinition::Mutation(_)
            | OperationDefinition::Subscription(_) => {
                return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
            }
        };
        for selection in &selection_set.items {
            let selection_field = match selection {
                Selection::Field(field) => field,
                Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                    return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
                }
            };
            let constraint = match selection_field
                .arguments
                .iter()
                .find(|(k, _)| *k == "block")
            {
                Some((_, arg)) => {
                    field_constraint(vars, &defaults, arg).map_err(Error::BadQuery)?
                }
                None => BlockConstraint::Unconstrained,
            };
            constraints.insert(constraint);
        }
    }
    Ok(constraints)
}

pub fn make_query_deterministic(
    mut ctx: Context<'_, String>,
    resolved: &BTreeSet<BlockPointer>,
    latest: &BlockPointer,
) -> Result<String, Error> {
    let vars = &ctx.variables;
    // Similar walk as ba6c90f1-3baf-45be-ac1c-f60733404436. But now including variables, and
    // mutating as we go.
    for operation in &mut ctx.operations {
        let (selection_set, defaults) = match operation {
            OperationDefinition::SelectionSet(selection_set) => {
                (selection_set, BTreeMap::default())
            }
            OperationDefinition::Query(query) if query.directives.is_empty() => {
                // Add default definitions for variables not set at top level.
                let defaults = query
                    .variable_definitions
                    .iter()
                    .filter(|d| !vars.0.contains_key(&d.name))
                    .filter_map(|d| Some((d.name.clone(), d.default_value.as_ref()?.to_graphql())))
                    .collect::<BTreeMap<String, StaticValue>>();
                (&mut query.selection_set, defaults)
            }
            OperationDefinition::Query(_)
            | OperationDefinition::Mutation(_)
            | OperationDefinition::Subscription(_) => {
                return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
            }
        };
        for selection in &mut selection_set.items {
            let selection_field = match selection {
                Selection::Field(field) => field,
                Selection::FragmentSpread(_) | Selection::InlineFragment(_) => {
                    return Err(Error::BadQuery(anyhow!("unsupported GraphQL features")))
                }
            };
            match selection_field
                .arguments
                .iter_mut()
                .find(|(k, _)| *k == "block")
            {
                Some((_, arg)) => {
                    match field_constraint(vars, &defaults, arg).map_err(Error::BadQuery)? {
                        BlockConstraint::Hash(_) => (),
                        BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => {
                            *arg = deterministic_block(&latest.hash);
                        }
                        BlockConstraint::Number(number) => {
                            let block =
                                resolved
                                    .iter()
                                    .find(|b| b.number == number)
                                    .ok_or_else(|| {
                                        Error::Internal(anyhow!(
                                            "failed to resolve block: {number}"
                                        ))
                                    })?;
                            *arg = deterministic_block(&block.hash);
                        }
                    };
                }
                None => {
                    selection_field
                        .arguments
                        .push(("block".to_string(), deterministic_block(&latest.hash)));
                }
            };
        }
    }

    let Context {
        fragments,
        operations,
        ..
    } = ctx;
    let definitions = fragments
        .into_iter()
        .map(Definition::Fragment)
        .chain(operations.into_iter().map(Definition::Operation))
        .collect();

    serde_json::to_string(
        &json!({ "query": Document { definitions }.to_string(), "variables": ctx.variables }),
    )
    .map_err(|err| Error::Internal(anyhow!("failed to serialize query: {err}")))
}

fn deterministic_block<'c>(hash: &BlockHash) -> Value<'c, String> {
    Value::Object(BTreeMap::from_iter([(
        "hash".to_string(),
        Value::String(hash.to_string()),
    )]))
}

fn field_constraint(
    vars: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
    field: &Value<'_, String>,
) -> anyhow::Result<BlockConstraint> {
    match field {
        Value::Object(fields) => parse_constraint(vars, defaults, fields),
        Value::Variable(name) => match vars
            .get(name)
            .ok_or_else(|| anyhow!("missing variable: {name}"))?
        {
            Value::Object(fields) => parse_constraint(vars, defaults, fields),
            _ => Err(anyhow!("malformed block constraint")),
        },
        _ => Err(anyhow!("malformed block constraint")),
    }
}

fn parse_constraint<'c, T: Text<'c>>(
    vars: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
    fields: &BTreeMap<T::Value, Value<'c, T>>,
) -> anyhow::Result<BlockConstraint> {
    let field = fields
        .iter()
        .at_most_one()
        .map_err(|_| anyhow!("conflicting block constraints"))?;
    match field {
        None => Ok(BlockConstraint::Unconstrained),
        Some((k, v)) => match (k.as_ref(), v) {
            ("hash", hash) => parse_hash(hash, vars, defaults).map(BlockConstraint::Hash),
            ("number", number) => parse_number(number, vars, defaults).map(BlockConstraint::Number),
            ("number_gte", number) => {
                parse_number(number, vars, defaults).map(BlockConstraint::NumberGTE)
            }
            _ => Err(anyhow!("unexpected block constraint: {}", k.as_ref())),
        },
    }
}

fn parse_hash<'c, T: Text<'c>>(
    hash: &Value<'c, T>,
    variables: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<BlockHash> {
    match hash {
        Value::String(hash) => hash
            .parse()
            .map_err(|err| anyhow!("malformed block hash: {err}")),
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::String(hash)) => hash
                .parse()
                .map_err(|err| anyhow!("malformed block hash: {err}")),
            _ => Err(anyhow!("missing variable: {}", name.as_ref())),
        },
        _ => Err(anyhow!("malformed block constraint")),
    }
}

fn parse_number<'c, T: Text<'c>>(
    number: &Value<'c, T>,
    variables: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<BlockNumber> {
    let n = match number {
        Value::Int(n) => n,
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::Int(n)) => n,
            _ => bail!("missing variable: {}", name.as_ref()),
        },
        _ => bail!("malformed block number"),
    };
    n.as_i64()
        .and_then(|n| n.try_into().ok())
        .ok_or_else(|| anyhow!("block number out of range"))
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use prelude::test_utils::bytes_from_id;

    use super::*;

    #[test]
    fn tests() {
        use BlockConstraint::*;
        let hash: BlockHash = bytes_from_id(54321).into();
        let tests = [
            ("{ a }", Ok(vec![Unconstrained])),
            ("{ a(abc:true) }", Ok(vec![Unconstrained])),
            ("{ a(block:{number:10}) }", Ok(vec![Number(10)])),
            (
                "{ a(block:{number:10,number_gte:11}) }",
                Err("bad query: conflicting block constraints"),
            ),
            (
                "{ a(block:{number:1}) b(block:{number:2}) }",
                Ok(vec![Number(1), Number(2)]),
            ),
            (
                &format!("{{ a(block:{{hash:{:?}}})}}", hash.to_string()),
                Ok(vec![Hash(hash)]),
            ),
            (
                "{ a(block:{number_gte:1}) b }",
                Ok(vec![NumberGTE(1), Unconstrained]),
            ),
            (
                "query($n: Int = 1) { a(block:{number_gte:$n}) }",
                Ok(vec![NumberGTE(1)]),
            ),
        ];
        for (query, expected) in tests {
            let context = Context::new(query, "").unwrap();
            let constraints = block_constraints(&context).map_err(|e| e.to_string());
            let expected = expected
                .map(|v| BTreeSet::from_iter(v.iter().cloned()))
                .map_err(ToString::to_string);
            assert_eq!(constraints, expected);
        }
    }
}
