use anyhow::anyhow;
use cost_model::Context;
use gateway_framework::errors::Error;
use graphql::graphql_parser::query::{OperationDefinition, Query, SelectionSet};

#[derive(Clone, Copy)]
pub enum SqlFieldBehavior {
    RejectSql,
    AcceptSqlOnly,
}

pub fn validate_query(ctx: &Context, behavior: SqlFieldBehavior) -> Result<(), Error> {
    for operation in &ctx.operations {
        match operation {
            OperationDefinition::SelectionSet(selection_set)
            | OperationDefinition::Query(Query { selection_set, .. }) => {
                if !selection_set_is_valid(selection_set, behavior) {
                    use SqlFieldBehavior::*;
                    match behavior {
                        RejectSql => return Err(Error::BadQuery(anyhow!("Query contains SQL"))),
                        AcceptSqlOnly => {
                            let invalid_fields = selection_set
                                .items
                                .iter()
                                .filter_map(|selection| {
                                    if let graphql::graphql_parser::query::Selection::Field(field) =
                                        selection
                                    {
                                        return Some(field.name);
                                    }
                                    None
                                })
                                .collect::<Vec<_>>();
                            return Err(Error::BadQuery(anyhow!(
                                "Fields [{}] are not SQL",
                                invalid_fields.join(", ")
                            )));
                        }
                    }
                }
            }
            _ => continue,
        }
    }
    Ok(())
}

fn selection_set_is_valid<'q>(
    selection_set: &SelectionSet<'q, &'q str>,
    behavior: SqlFieldBehavior,
) -> bool {
    let field_is_valid = |field_name: &str| match behavior {
        SqlFieldBehavior::RejectSql => field_name != "sql",
        SqlFieldBehavior::AcceptSqlOnly => field_name == "sql",
    };

    selection_set.items.iter().all(|selection| {
        matches!(selection, graphql::graphql_parser::query::Selection::Field(field) if field_is_valid(field.name))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reject_sql() -> SqlFieldBehavior {
        SqlFieldBehavior::RejectSql
    }

    fn accept_sql_only() -> SqlFieldBehavior {
        SqlFieldBehavior::AcceptSqlOnly
    }

    fn create_context(query: &str) -> Context {
        let variables = r#"{}"#;
        Context::new(query, variables).unwrap()
    }

    #[test]
    fn test_single_selection_set_reject_sql() {
        let query = r#"
            query {
                sql(input: { query: "SELECT * FROM users" }) {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_err());
    }

    #[test]
    fn test_single_selection_set_without_query_reject_sql() {
        let query = r#"
            {
                sql(input: { query: "SELECT * FROM users" }) {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_err());
    }

    #[test]
    fn test_no_sql_single_selection_set_reject_sql() {
        let query = r#"
            query {
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }

    #[test]
    fn test_no_sql_single_selection_set_without_query_reject_sql() {
        let query = r#"
            {
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }

    #[test]
    fn test_multi_selection_set_reject_sql() {
        let query = r#"
            query {
                sql(input: { query: "SELECT * FROM users" }) {
                    id
                    name
                }
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_err());
    }

    #[test]
    fn test_no_sql_multi_selection_set_reject_sql() {
        let query = r#"
            query {
                tokens {
                    id
                    name
                }
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }

    #[test]
    fn test_multi_selection_set_without_query_reject_sql() {
        let query = r#"
            {
                sql(input: { query: "SELECT * FROM users" }) {
                    id
                    name
                }
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_err());
    }

    #[test]
    fn test_no_sql_multi_selection_set_without_query_reject_sql() {
        let query = r#"
            {
                tokens {
                    id
                    name
                }
                users {
                    id
                    name
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }

    #[test]
    fn test_single_selection_set_accept_sql_only() {
        let query = r#"
        query {
            sql(input: { query: "SELECT * FROM users" }) {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_ok());
    }

    #[test]
    fn test_single_selection_set_without_query_accept_sql_only() {
        let query = r#"
        {
            sql(input: { query: "SELECT * FROM users" }) {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_ok());
    }

    #[test]
    fn test_no_sql_single_selection_set_accept_sql_only() {
        let query = r#"
        query {
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_no_sql_single_selection_set_without_query_accept_sql_only() {
        let query = r#"
        {
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_multi_selection_set_accept_sql_only() {
        let query = r#"
        query {
            sql(input: { query: "SELECT * FROM users" }) {
                id
                name
            }
            sql(input: { query: "SELECT * FROM tokens" }) {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_ok());
    }

    #[test]
    fn test_with_graphql_multi_selection_set_accept_sql_only() {
        let query = r#"
        query {
            sql(input: { query: "SELECT * FROM users" }) {
                id
                name
            }
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_no_sql_multi_selection_set_accept_sql_only() {
        let query = r#"
        query {
            tokens {
                id
                name
            }
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_with_graphql_multi_selection_set_without_query_accept_sql_only() {
        let query = r#"
        {
            sql(input: { query: "SELECT * FROM users" }) {
                id
                name
            }
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_no_sql_multi_selection_set_without_query_accept_sql_only() {
        let query = r#"
        {
            tokens {
                id
                name
            }
            users {
                id
                name
            }
        }
    "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, accept_sql_only()).is_err());
    }

    #[test]
    fn test_no_sql_single_selection_set_reject_sql_with_nested_sql_field() {
        let query = r#"
            query {
                users {
                    sql
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }

    #[test]
    fn test_no_sql_single_selection_set_without_query_reject_sql_with_nested_sql_field() {
        let query = r#"
            {
                users {
                    sql
                }
            }
        "#;
        let ctx = create_context(query);
        assert!(validate_query(&ctx, reject_sql()).is_ok());
    }
}
