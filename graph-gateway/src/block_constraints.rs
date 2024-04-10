use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::{BlockHash, BlockNumber};
use anyhow::{anyhow, bail};
use cost_model::Context;
use gateway_common::utils::timestamp::unix_timestamp;
use gateway_framework::{
    blocks::{BlockConstraint, UnresolvedBlock},
    chain::Chain,
    errors::Error,
};
use graphql::{
    graphql_parser::query::{
        Definition, Document, Field, OperationDefinition, Selection, SelectionSet, Text, Value,
    },
    IntoStaticValue as _, StaticValue,
};
use itertools::Itertools as _;
use serde_json::{self, json};

#[derive(Debug)]
pub struct BlockRequirements {
    /// required block range, for exact block constraints (`number` & `hash`)
    pub range: Option<(BlockNumber, BlockNumber)>,
    /// maximum `number_gte` constraint
    pub number_gte: Option<BlockNumber>,
    /// does the query benefit from using the latest block (contains NumberGTE or Unconstrained)
    pub latest: bool,
}

pub fn resolve_block_requirements(
    chain: &Chain,
    context: &Context<'_, String>,
    manifest_min_block: BlockNumber,
) -> Result<BlockRequirements, Error> {
    let constraints = block_constraints(context).unwrap_or_default();

    let latest = constraints.iter().any(|c| match c {
        BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => true,
        BlockConstraint::Hash(_) | BlockConstraint::Number(_) => false,
    });
    let number_gte = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::NumberGTE(n) => Some(*n),
            _ => None,
        })
        .max();

    let exact_constraints: Vec<u64> = constraints
        .iter()
        .filter_map(|c| match c {
            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => None,
            BlockConstraint::Number(number) => Some(*number),
            // resolving block hashes is not guaranteed
            BlockConstraint::Hash(hash) => chain
                .find(&UnresolvedBlock::WithHash(*hash))
                .map(|b| b.number),
        })
        .collect();
    let min_block = exact_constraints.iter().min().cloned();
    let max_block = exact_constraints.iter().max().cloned();

    // Reject queries for blocks before the minimum start block in the manifest, but only if the
    // constraint is for an exact block. For example, we always want to allow `block_gte: 0`.
    let request_contains_invalid_blocks = exact_constraints
        .iter()
        .any(|number| *number < manifest_min_block);
    if request_contains_invalid_blocks {
        return Err(Error::BadQuery(anyhow!(
            "requested block {}, before minimum `startBlock` of manifest {}",
            min_block.unwrap_or_default(),
            manifest_min_block,
        )));
    }

    Ok(BlockRequirements {
        range: min_block.map(|min| (min, max_block.unwrap())),
        number_gte,
        latest,
    })
}

fn block_constraints(context: &Context<String>) -> Result<BTreeSet<BlockConstraint>, Error> {
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

pub fn rewrite_query(
    chain: &Chain,
    mut ctx: Context<String>,
    requirements: &BlockRequirements,
    blocks_behind: u64,
) -> Result<String, Error> {
    if !contains_introspection(&ctx) {
        let latest_block = requirements.latest.then_some(()).and_then(|_| {
            let mut block = chain.latest()?;
            if (blocks_behind > 0) || requirements.number_gte.is_some() {
                let number = block
                    .number
                    .saturating_sub(blocks_behind)
                    .max(requirements.number_gte.unwrap_or(0));
                block = chain.find(&UnresolvedBlock::WithNumber(number))?;
            }
            let now = unix_timestamp() / 1_000;
            if now.saturating_sub(block.timestamp) > 30 {
                return None;
            }
            Some(block.clone())
        });

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
                        .filter_map(|d| {
                            Some((d.name.clone(), d.default_value.as_ref()?.to_graphql()))
                        })
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
                            BlockConstraint::Number(number) => {
                                if let Some(block) =
                                    chain.find(&UnresolvedBlock::WithNumber(number))
                                {
                                    *arg = deterministic_block(&block.hash);
                                }
                            }
                            BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => {
                                if let Some(block) = &latest_block {
                                    *arg = deterministic_block(&block.hash);
                                }
                            }
                        };
                    }
                    None => {
                        if let Some(block) = &latest_block {
                            selection_field
                                .arguments
                                .push(("block".to_string(), deterministic_block(&block.hash)));
                        }
                    }
                };
            }
        }

        for operation in &mut ctx.operations {
            match operation {
                OperationDefinition::Query(q) => q.selection_set.items.push(probe()),
                OperationDefinition::SelectionSet(s) => s.items.push(probe()),
                _ => (),
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

fn contains_introspection(ctx: &Context<String>) -> bool {
    let is_introspection_field = |s: &Selection<'_, String>| match s {
        Selection::Field(f) => f.name.starts_with("__"),
        Selection::FragmentSpread(_) | Selection::InlineFragment(_) => false,
    };
    ctx.operations.iter().any(|op| match op {
        OperationDefinition::Query(q) => q.selection_set.items.iter().any(is_introspection_field),
        OperationDefinition::SelectionSet(s) => s.items.iter().any(is_introspection_field),
        OperationDefinition::Mutation(_) | OperationDefinition::Subscription(_) => false,
    })
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

fn probe<'c>() -> Selection<'c, String> {
    let selection_set = |items| SelectionSet {
        span: Default::default(),
        items,
    };
    let field = |name: &str, alias, selection_set| {
        Selection::Field(Field {
            position: Default::default(),
            alias,
            name: name.to_string(),
            arguments: vec![],
            directives: vec![],
            selection_set,
        })
    };
    field(
        "_meta",
        Some("_gateway_probe_".into()),
        selection_set(vec![field(
            "block",
            None,
            selection_set(vec![
                field("number", None, selection_set(vec![])),
                field("hash", None, selection_set(vec![])),
                field("timestamp", None, selection_set(vec![])),
            ]),
        )]),
    )
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use gateway_common::utils::testing::bytes_from_id;

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

    #[test]
    fn query_contains_introspection() {
        let query = "{ __schema { queryType { name } } }";
        let context = Context::new(query, "").unwrap();
        assert!(super::contains_introspection(&context));
    }
}
