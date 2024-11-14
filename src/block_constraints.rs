use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{anyhow, bail};
use graphql::{
    graphql_parser::{
        self,
        query::{Definition, OperationDefinition, Selection, Text, Value, VariableDefinition},
    },
    IntoStaticValue as _, QueryVariables, StaticValue,
};
use itertools::Itertools as _;
use serde::Deserialize;
use thegraph_core::{BlockHash, BlockNumber};

use crate::{blocks::BlockConstraint, chain::Chain, errors::Error};

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
    request_body: &str,
    manifest_min_block: BlockNumber,
) -> Result<BlockRequirements, Error> {
    let constraints = block_constraints(request_body).unwrap_or_default();

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
            BlockConstraint::Hash(hash) => chain.find(hash).map(|b| b.number),
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

fn block_constraints(request_body: &str) -> Result<BTreeSet<BlockConstraint>, Error> {
    #[derive(Deserialize)]
    struct ClientRequest {
        query: String,
        #[serde(default)]
        variables: Option<QueryVariables>,
    }
    let request: ClientRequest =
        serde_json::from_str(request_body).map_err(|err| Error::BadQuery(err.into()))?;
    let document =
        graphql_parser::parse_query(&request.query).map_err(|err| Error::BadQuery(err.into()))?;

    let mut constraints = BTreeSet::new();
    let vars = &request.variables.map(|vars| vars.0).unwrap_or_default();
    let operations = document.definitions.iter().filter_map(|def| match def {
        Definition::Operation(op) => Some(op),
        Definition::Fragment(_) => None,
    });
    for operation in operations {
        let (selection_set, defaults) = match operation {
            OperationDefinition::SelectionSet(selection_set) => {
                (selection_set, BTreeMap::default())
            }
            OperationDefinition::Query(query) if query.directives.is_empty() => {
                // Add default definitions for variables not set at top level.
                let defaults: BTreeMap<String, StaticValue> = query
                    .variable_definitions
                    .iter()
                    .filter(|d| !vars.contains_key(d.name))
                    .filter_map(|d: &VariableDefinition<'_, &'_ str>| {
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

fn field_constraint<'c>(
    vars: &HashMap<String, StaticValue>,
    defaults: &BTreeMap<String, StaticValue>,
    field: &Value<'c, &'c str>,
) -> anyhow::Result<BlockConstraint> {
    match field {
        Value::Object(fields) => parse_constraint(vars, defaults, fields),
        Value::Variable(name) => match vars.get(*name).or_else(|| defaults.get(*name)) {
            None => Ok(BlockConstraint::Unconstrained),
            Some(Value::Object(fields)) => parse_constraint(vars, defaults, fields),
            _ => Err(anyhow!("malformed block constraint")),
        },
        _ => Err(anyhow!("malformed block constraint")),
    }
}

fn parse_constraint<'c, T: Text<'c>>(
    vars: &HashMap<String, StaticValue>,
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
    vars: &HashMap<String, StaticValue>,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<Option<BlockHash>> {
    match hash {
        Value::String(hash) => hash
            .parse()
            .map(Some)
            .map_err(|err| anyhow!("malformed block hash: {err}")),
        Value::Variable(name) => match vars
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
    vars: &HashMap<String, StaticValue>,
    defaults: &BTreeMap<String, StaticValue>,
) -> anyhow::Result<Option<BlockNumber>> {
    let n = match number {
        Value::Int(n) => n,
        Value::Variable(name) => match vars
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
    use super::{block_constraints, BlockConstraint};

    #[test]
    fn tests() {
        use BlockConstraint::*;
        let tests = [
            (r#"{"query":"{ a }"}"#, Ok(vec![Unconstrained])),
            (r#"{"query":"{ a(abc:true) }"}"#, Ok(vec![Unconstrained])),
            (
                r#"{"query":"{ a(block:{number:10}) }"}"#,
                Ok(vec![Number(10)]),
            ),
            (
                r#"{"query":"{ a(block:{number:10,number_gte:11}) }"}"#,
                Err("bad query: conflicting block constraints"),
            ),
            (
                r#"{"query":"{ a(block:{number:1}) b(block:{number:2}) }"}"#,
                Ok(vec![Number(1), Number(2)]),
            ),
            (
                r#"{"query":"{ a(block:{hash:\"0x0000000000000000000000000000000000000000000000000000000000054321\"}) }"}"#,
                Ok(vec![Hash(
                    "0x0000000000000000000000000000000000000000000000000000000000054321"
                        .parse()
                        .unwrap(),
                )]),
            ),
            (
                r#"{"query":"{ a(block:{number_gte:1}) b }"}"#,
                Ok(vec![NumberGTE(1), Unconstrained]),
            ),
            (
                r#"{"query":"query($n: Int = 1) { a(block:{number_gte:$n}) }"}"#,
                Ok(vec![NumberGTE(1)]),
            ),
            (
                r#"{"query":"query($n: Int) { a(block:{number_gte:$n}) }"}"#,
                Ok(vec![Unconstrained]),
            ),
            (
                r#"{"query":"query($h: String) { a(block:{hash:$h}) }"}"#,
                Ok(vec![Unconstrained]),
            ),
            (
                r#"{"query":"query($b: Block_height) { a(block:$b) }"}"#,
                Ok(vec![Unconstrained]),
            ),
            (
                r#"{"query":"query($b: Block_height = {number_gte:0}) { a(block:$b) }"}"#,
                Ok(vec![NumberGTE(0)]),
            ),
            (
                r#"{"query":"query($b: Block_height) { a(block:$b) }","variables":{"b":{"number_gte":0}}}"#,
                Ok(vec![NumberGTE(0)]),
            ),
            (
                r#"{"query":"query($b: Int) { a(block:{number:$b}) }","variables":{"b":0}}"#,
                Ok(vec![Number(0)]),
            ),
        ];
        for (query, expected) in tests {
            let constraints = block_constraints(query).map_err(|e| e.to_string());
            let expected = expected
                .map(|v| v.iter().cloned().collect())
                .map_err(ToString::to_string);
            assert_eq!(constraints, expected);
        }
    }
}
