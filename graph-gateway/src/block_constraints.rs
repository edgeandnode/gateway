use indexer_selection::{Context, UnresolvedBlock};
use itertools::Itertools as _;
use prelude::graphql::graphql_parser::query::{
    Definition, Document, OperationDefinition, Selection, Text, Value,
};
use prelude::{
    graphql::{IntoStaticValue as _, QueryVariables, StaticValue},
    *,
};
use serde_json::{self, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum BlockConstraint {
    Unconstrained,
    Hash(Bytes32),
    Number(u64),
    NumberGTE(u64),
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

pub fn block_constraints<'c>(context: &'c Context<'c>) -> Option<BTreeSet<BlockConstraint>> {
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
            | OperationDefinition::Subscription(_) => return None,
        };
        for selection in &selection_set.items {
            let selection_field = match selection {
                Selection::Field(field) => field,
                Selection::FragmentSpread(_) | Selection::InlineFragment(_) => return None,
            };
            let constraint = match selection_field
                .arguments
                .iter()
                .find(|(k, _)| *k == "block")
            {
                Some((_, arg)) => field_constraint(vars, &defaults, arg)?,
                None => BlockConstraint::Unconstrained,
            };
            constraints.insert(constraint);
        }
    }
    Some(constraints)
}

pub fn make_query_deterministic(
    mut ctx: Context<'_>,
    resolved: &BTreeSet<BlockPointer>,
    latest: &BlockPointer,
) -> Option<String> {
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
            | OperationDefinition::Subscription(_) => return None,
        };
        for selection in &mut selection_set.items {
            let selection_field = match selection {
                Selection::Field(field) => field,
                Selection::FragmentSpread(_) | Selection::InlineFragment(_) => return None,
            };
            match selection_field
                .arguments
                .iter_mut()
                .find(|(k, _)| *k == "block")
            {
                Some((_, arg)) => {
                    match field_constraint(vars, &defaults, arg)? {
                        BlockConstraint::Hash(_) => (),
                        BlockConstraint::Unconstrained | BlockConstraint::NumberGTE(_) => {
                            *arg = deterministic_block(&latest.hash);
                        }
                        BlockConstraint::Number(number) => {
                            let block = resolved.iter().find(|b| b.number == number);
                            debug_assert!(block.is_some());
                            *arg = deterministic_block(&block?.hash);
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
    .ok()
}

fn deterministic_block<'c>(hash: &Bytes32) -> Value<'c, String> {
    Value::Object(BTreeMap::from_iter([(
        "hash".to_string(),
        Value::String(hash.to_string()),
    )]))
}

fn field_constraint(
    vars: &QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
    field: &Value<'_, String>,
) -> Option<BlockConstraint> {
    match field {
        Value::Object(fields) => parse_constraint(vars, defaults, fields),
        Value::Variable(name) => match vars.get(name)? {
            Value::Object(fields) => parse_constraint(vars, defaults, fields),
            _ => None,
        },
        _ => None,
    }
}

fn parse_constraint<'c, T: Text<'c>>(
    vars: &QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
    fields: &BTreeMap<T::Value, Value<'c, T>>,
) -> Option<BlockConstraint> {
    let field = fields.iter().at_most_one().ok()?;
    match field {
        None => Some(BlockConstraint::Unconstrained),
        Some((k, v)) => match (k.as_ref(), v) {
            ("hash", hash) => parse_hash(hash, vars, defaults).map(BlockConstraint::Hash),
            ("number", number) => parse_number(number, vars, defaults).map(BlockConstraint::Number),
            ("number_gte", number) => {
                parse_number(number, vars, defaults).map(BlockConstraint::NumberGTE)
            }
            _ => None,
        },
    }
}

fn parse_hash<'c, T: Text<'c>>(
    hash: &Value<'c, T>,
    variables: &QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> Option<Bytes32> {
    match hash {
        Value::String(hash) => hash.parse().ok(),
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::String(hash)) => hash.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn parse_number<'c, T: Text<'c>>(
    number: &Value<'c, T>,
    variables: &QueryVariables,
    defaults: &BTreeMap<String, StaticValue>,
) -> Option<u64> {
    let n = match number {
        Value::Int(n) => n,
        Value::Variable(name) => match variables
            .get(name.as_ref())
            .or_else(|| defaults.get(name.as_ref()))
        {
            Some(Value::Int(n)) => n,
            _ => return None,
        },
        _ => return None,
    };
    n.as_i64()?.try_into().ok()
}

#[cfg(test)]
mod tests {
    use std::iter::FromIterator;

    use super::*;
    use prelude::test_utils::bytes_from_id;

    #[test]
    fn tests() {
        use BlockConstraint::*;
        let hash = Bytes32::from(bytes_from_id(54321));
        let tests = [
            ("{ a }", Some(vec![Unconstrained])),
            ("{ a(abc:true) }", Some(vec![Unconstrained])),
            ("{ a(block:{number:10}) }", Some(vec![Number(10)])),
            ("{ a(block:{number:10,number_gte:11}) }", None),
            (
                "{ a(block:{number:1}) b(block:{number:2}) }",
                Some(vec![Number(1), Number(2)]),
            ),
            (
                &format!("{{ a(block:{{hash:{:?}}})}}", hash.to_string()),
                Some(vec![Hash(hash)]),
            ),
            (
                "{ a(block:{number_gte:1}) b }",
                Some(vec![NumberGTE(1), Unconstrained]),
            ),
            (
                "query($n: Int = 1) { a(block:{number_gte:$n}) }",
                Some(vec![NumberGTE(1)]),
            ),
        ];
        for (query, expected) in tests {
            let context = Context::new(query, "").unwrap();
            let constraints = block_constraints(&context);
            let expected = expected.map(|v| BTreeSet::from_iter(v.iter().cloned()));
            assert_eq!(constraints, expected);
        }
    }
}
