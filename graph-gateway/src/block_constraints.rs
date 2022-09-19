use graphql_parser::query::{Definition, Document, OperationDefinition, Selection, Text, Value};
use indexer_selection::{cost_model::QueryVariables, Context, UnresolvedBlock};
use itertools::Itertools as _;
use prelude::*;
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
        let selection_set = match operation {
            OperationDefinition::SelectionSet(selection_set) => selection_set,
            OperationDefinition::Query(query) if query.directives.is_empty() => {
                &query.selection_set
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
                Some((_, arg)) => field_constraint(vars, arg)?,
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
        let selection_set = match operation {
            OperationDefinition::SelectionSet(selection_set) => selection_set,
            OperationDefinition::Query(query) if query.directives.is_empty() => {
                &mut query.selection_set
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
                    match field_constraint(vars, &arg)? {
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
                        .push(("block", deterministic_block(&latest.hash)));
                }
            };
        }
    }

    let mut query = Document {
        definitions: Vec::new(),
    };
    query
        .definitions
        .extend(ctx.fragments.iter().cloned().map(Definition::Fragment));
    query
        .definitions
        .extend(ctx.operations.iter().cloned().map(Definition::Operation));

    serde_json::to_string(&json!({ "query": query.to_string(), "variables": ctx.variables })).ok()
}

fn deterministic_block<'c>(hash: &Bytes32) -> Value<'c, &'c str> {
    Value::Object(BTreeMap::from_iter([(
        "hash",
        Value::String(hash.to_string()),
    )]))
}

fn field_constraint<'c>(
    vars: &QueryVariables,
    field: &Value<'c, &'c str>,
) -> Option<BlockConstraint> {
    // TODO: The GraphQL spec is format agnostic, and may support recursive variables.
    // Be careful not to allow a stack overflow from malicious inputs.
    match field {
        Value::Object(fields) => parse_constraint(vars, fields),
        Value::Variable(name) => match vars.get(name)? {
            Value::Object(fields) => parse_constraint(vars, fields),
            _ => None,
        },
        _ => None,
    }
}

fn parse_constraint<'c, T: Text<'c>>(
    vars: &QueryVariables,
    fields: &BTreeMap<T::Value, Value<'c, T>>,
) -> Option<BlockConstraint> {
    let field = fields.iter().at_most_one().ok()?;
    match field {
        None => Some(BlockConstraint::Unconstrained),
        Some((k, v)) => match (k.as_ref(), v) {
            ("hash", hash) => parse_hash(hash, vars).map(BlockConstraint::Hash),
            ("number", number) => parse_number(number, vars).map(BlockConstraint::Number),
            ("number_gte", number) => parse_number(number, vars).map(BlockConstraint::NumberGTE),
            _ => None,
        },
    }
}

fn parse_hash<'c, T: Text<'c>>(hash: &Value<'c, T>, variables: &QueryVariables) -> Option<Bytes32> {
    // TODO: The GraphQL spec is format agnostic, and may support recursive variables.
    // Be careful not to allow a stack overflow from malicious inputs.
    match hash {
        Value::String(hash) => hash.parse().ok(),
        Value::Variable(name) => match variables.get(name.as_ref()) {
            Some(Value::String(hash)) => hash.parse().ok(),
            _ => return None,
        },
        _ => return None,
    }
}

fn parse_number<'c, T: Text<'c>>(number: &Value<'c, T>, variables: &QueryVariables) -> Option<u64> {
    // TODO: The GraphQL spec is format agnostic, and may support recursive variables.
    // Be careful not to allow a stack overflow from malicious inputs.
    let n = match number {
        Value::Int(n) => n,
        Value::Variable(name) => match variables.get(name.as_ref()) {
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
        ];
        for (query, expected) in tests {
            let context = Context::new(query, "").unwrap();
            let constraints = block_constraints(&context);
            let expected = expected.map(|v| BTreeSet::from_iter(v.iter().cloned()));
            assert_eq!(constraints, expected);
        }
    }
}
