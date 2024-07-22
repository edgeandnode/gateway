use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use alloy_primitives::{BlockHash, BlockNumber};
use anyhow::{anyhow, bail};
use cost_model::Context;
use gateway_framework::{
    blocks::{BlockConstraint, UnresolvedBlock},
    chain::Chain,
    errors::Error,
};
use graphql::{
    graphql_parser::query::{OperationDefinition, Selection, SelectionSet, Text, Value},
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
    context: &Context,
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

fn block_constraints(context: &Context) -> Result<BTreeSet<BlockConstraint>, Error> {
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
                let defaults: BTreeMap<String, StaticValue> = query
                    .variable_definitions
                    .iter()
                    .filter(|d| !vars.0.contains_key(d.name))
                    .filter_map(|d| {
                        Some((d.name.to_string(), d.default_value.as_ref()?.to_graphql()))
                    })
                    .collect();
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

pub fn rewrite_query<'q>(ctx: &Context<'q>) -> String {
    let mut buf: String = Default::default();
    for fragment in &ctx.fragments {
        write!(&mut buf, "{}", fragment).unwrap();
    }
    if contains_introspection(ctx) {
        for operation in &ctx.operations {
            write!(&mut buf, "{}", operation).unwrap();
        }
    } else {
        let serialize_selection_set =
            |buf: &mut String, selection_set: &SelectionSet<'q, &'q str>| {
                buf.push_str("{\n");
                for selection in &selection_set.items {
                    match selection {
                        Selection::Field(field) => {
                            write!(buf, "  {}", field).unwrap();
                        }
                        Selection::FragmentSpread(spread) => {
                            write!(buf, "  {}", spread).unwrap();
                        }
                        Selection::InlineFragment(fragment) => {
                            write!(buf, "  {}", fragment).unwrap();
                        }
                    };
                }
                buf.push_str("  _gateway_probe_: _meta { block { hash number timestamp } }\n}\n");
            };
        let serialize_operation =
            |buf: &mut String, operation: &OperationDefinition<'q, &'q str>| {
                match operation {
                    OperationDefinition::SelectionSet(selection_set) => {
                        serialize_selection_set(buf, selection_set);
                    }
                    OperationDefinition::Query(query) => {
                        buf.push_str("query");
                        if let Some(name) = query.name {
                            write!(buf, " {name}").unwrap();
                        }
                        if !query.variable_definitions.is_empty() {
                            write!(buf, "({}", query.variable_definitions[0]).unwrap();
                            for var in &query.variable_definitions[1..] {
                                write!(buf, ", {var}").unwrap();
                            }
                            buf.push(')');
                        }
                        debug_assert!(query.directives.is_empty());
                        buf.push(' ');
                        serialize_selection_set(buf, &query.selection_set);
                    }
                    OperationDefinition::Mutation(_) | OperationDefinition::Subscription(_) => (),
                };
            };
        for operation in &ctx.operations {
            serialize_operation(&mut buf, operation);
        }
    }

    serde_json::to_string(&json!({ "query": buf, "variables": ctx.variables })).unwrap()
}

fn contains_introspection(ctx: &Context<'_>) -> bool {
    fn selection_set_has_introspection<'q>(s: &SelectionSet<'q, &'q str>) -> bool {
        s.items.iter().any(|selection| match selection {
            Selection::Field(f) => f.name.starts_with("__"), // only check top level
            Selection::InlineFragment(_) | Selection::FragmentSpread(_) => false,
        })
    }
    ctx.operations.iter().any(|op| match op {
        OperationDefinition::Query(q) => selection_set_has_introspection(&q.selection_set),
        OperationDefinition::SelectionSet(s) => selection_set_has_introspection(s),
        OperationDefinition::Mutation(_) | OperationDefinition::Subscription(_) => false,
    })
}

fn field_constraint<'c, T: Text<'c>>(
    vars: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
    field: &Value<'c, T>,
) -> anyhow::Result<BlockConstraint> {
    match field {
        Value::Object(fields) => parse_constraint(vars, defaults, fields),
        Value::Variable(name) => match vars
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            None => Ok(BlockConstraint::Unconstrained),
            Some(Value::Object(fields)) => parse_constraint(vars, defaults, fields),
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
            ("hash", hash) => parse_hash(hash, vars, defaults).map(|h| {
                h.map(BlockConstraint::Hash)
                    .unwrap_or(BlockConstraint::Unconstrained)
            }),
            ("number", number) => parse_number(number, vars, defaults).map(|n| {
                n.map(BlockConstraint::Number)
                    .unwrap_or(BlockConstraint::Unconstrained)
            }),
            ("number_gte", number) => parse_number(number, vars, defaults).map(|n| {
                n.map(BlockConstraint::NumberGTE)
                    .unwrap_or(BlockConstraint::Unconstrained)
            }),
            _ => Err(anyhow!("unexpected block constraint: {}", k.as_ref())),
        },
    }
}

fn parse_hash<'c, T: Text<'c>>(
    hash: &Value<'c, T>,
    variables: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<Option<BlockHash>> {
    match hash {
        Value::String(hash) => hash
            .parse()
            .map(Some)
            .map_err(|err| anyhow!("malformed block hash: {err}")),
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::String(hash)) => hash
                .parse()
                .map(Some)
                .map_err(|err| anyhow!("malformed block hash: {err}")),
            _ => Ok(None),
        },
        _ => Err(anyhow!("malformed block constraint")),
    }
}

fn parse_number<'c, T: Text<'c>>(
    number: &Value<'c, T>,
    variables: &cost_model::QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<Option<BlockNumber>> {
    let n = match number {
        Value::Int(n) => n,
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::Int(n)) => n,
            _ => return Ok(None),
        },
        _ => bail!("malformed block number"),
    };
    n.as_i64()
        .map(|n| n.try_into().ok())
        .ok_or_else(|| anyhow!("block number out of range"))
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator as _;

    use alloy_primitives::hex;

    use super::*;

    #[test]
    fn tests() {
        use BlockConstraint::*;
        let hash: BlockHash =
            hex!("0000000000000000000000000000000000000000000000000000000000054321").into();
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
            (
                "query($n: Int) { a(block:{number_gte:$n}) }",
                Ok(vec![Unconstrained]),
            ),
            (
                "query($h: String) { a(block:{hash:$h}) }",
                Ok(vec![Unconstrained]),
            ),
            (
                "query($b: Block_height) { a(block:$b) }",
                Ok(vec![Unconstrained]),
            ),
            (
                "query($b: Block_height = {number_gte:0}) { a(block:$b) }",
                Ok(vec![NumberGTE(0)]),
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
        let examples = [
            "{ __schema { queryType { name } } }",
            "{ __type(name:\"Droid\") { name description } }",
        ];
        for example in examples {
            let context = Context::new(example, "").unwrap();
            assert!(super::contains_introspection(&context));
        }
    }
}
