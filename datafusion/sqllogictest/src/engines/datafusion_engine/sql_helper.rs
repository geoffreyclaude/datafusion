use sqlparser::ast::{CastKind, DataType, Expr, Ident, ObjectName, Query, SelectItem, SetExpr, TableFactor, VisitMut, VisitorMut, CharacterLength};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::ops::ControlFlow;
use log::info;

/// Sanitizes a SQL query by lowercasing and quoting all unquoted identifiers.
pub(crate) fn sanitize_sql_idents(sql: &str) -> Result<String, sqlparser::parser::ParserError> {
    let dialect = GenericDialect;
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    statements
        .iter_mut()
        .for_each(|stmt| { stmt.visit(&mut IdentifierQuoter); });

    let sanitized_sql = statements
        .into_iter()
        .map(|stmt| stmt.to_string())
        .collect::<Vec<_>>()
        .join("; ");

    Ok(sanitized_sql)
}

/// Lowercases and quotes an identifier if it's not already quoted.
fn quote_and_lowercase_ident(ident: &mut Ident) {
    if ident.quote_style.is_none() {
        ident.value = ident.value.to_lowercase();
        ident.quote_style = Some('"');
    }
}

/// VisitorMut implementation that sanitizes identifiers by quoting and lowercasing.
struct IdentifierQuoter;

impl VisitorMut for IdentifierQuoter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        info!("post_visit_expr: {:?}", expr);
        match expr {
            Expr::Identifier(ident) => quote_and_lowercase_ident(ident),
            Expr::CompoundIdentifier(idents) => idents.iter_mut().for_each(quote_and_lowercase_ident),
            Expr::CompositeAccess { key, .. } => quote_and_lowercase_ident(key),
            Expr::Named { name, .. } => quote_and_lowercase_ident(name),
            Expr::MatchAgainst { columns, .. } => columns.iter_mut().for_each(quote_and_lowercase_ident),
            Expr::Function(function) => {function.name.0.iter_mut().for_each(quote_and_lowercase_ident)},
            Expr::Array(array) => array.named = true,
            Expr::Cast{kind, data_type, ..} => {
                if kind == &CastKind::DoubleColon {
                    *kind = CastKind::Cast;
                }
                *data_type = match &data_type {
                    DataType::UnsignedTinyInt(val) => DataType::TinyInt(*val),
                    DataType::UnsignedBigInt(val) => DataType::BigInt(*val),
                    DataType::UnsignedSmallInt(val) => DataType::SmallInt(*val),
                    DataType::UnsignedInt(val) => DataType::Int(*val),
                    DataType::UnsignedInteger(val) => DataType::Integer(*val),
                    DataType::UnsignedMediumInt(val) => DataType::MediumInt(*val),
                    DataType::UnsignedInt2(val) => DataType::Int2(*val),
                    DataType::UnsignedInt4(val) => DataType::Int4(*val),
                    DataType::UnsignedInt8(val) => DataType::Int8(*val),
                    DataType::String(val) => {
                        if let Some(val) = val {
                            DataType::Varchar(Some(CharacterLength::IntegerLength {
                                length: *val,
                                unit: None,
                            }))
                        } else {
                            DataType::Varchar(None)
                        }
                    }
                    _ => data_type.clone(),
                };
            },
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        info!("post_visit_relation: {:?}", relation);
        relation.0.iter_mut().for_each(quote_and_lowercase_ident);
        ControlFlow::Continue(())
    }

    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<Self::Break> {
        info!("post_visit_table_factor: {:?}", table_factor);
        let alias = match table_factor {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::JsonTable { alias, .. }
            | TableFactor::OpenJsonTable { alias, .. }
            | TableFactor::NestedJoin { alias, .. }
            | TableFactor::Pivot { alias, .. }
            | TableFactor::Unpivot { alias, .. }
            | TableFactor::MatchRecognize { alias, .. } => alias.as_mut(),
        };

        if let Some(alias) = alias {
            quote_and_lowercase_ident(&mut alias.name);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        info!("post_visit_query: {:?}", query);
        // Sanitize CTEs (WITH ...).
        if let Some(with) = &mut query.with {
            with.cte_tables.iter_mut().for_each(|cte| {
                quote_and_lowercase_ident(&mut cte.alias.name);
                cte.alias.columns.iter_mut().for_each(|col| {
                    quote_and_lowercase_ident(&mut col.name);
                });
                if let Some(from_ident) = &mut cte.from {
                    quote_and_lowercase_ident(from_ident);
                }
            });
        }

        // Sanitize projection columns in a plain SELECT.
        if let SetExpr::Select(select) = query.body.as_mut() {
            select.projection.iter_mut().for_each(|item| {
                if let SelectItem::ExprWithAlias { alias, .. } = item {
                    quote_and_lowercase_ident(alias);
                }
            });
        }

        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::sanitize_sql_idents;

    #[test]
    fn handles_empty_query() {
        let input = "";
        assert_eq!(sanitize_sql_idents(input).unwrap(), "");
    }

    #[test]
    fn sanitizes_identifiers_in_simple_query() {
        let input = "SELECT Column1 FROM SomeTable";
        assert_eq!(sanitize_sql_idents(input).unwrap(), r#"SELECT "column1" FROM "sometable""#);
    }

    #[test]
    fn preserves_already_quoted_identifiers() {
        let input = r#"SELECT "QuotedIdent", unquoted FROM MyTable"#;
        assert_eq!(sanitize_sql_idents(input).unwrap(), r#"SELECT "QuotedIdent", "unquoted" FROM "mytable""#);
    }

    #[test]
    fn sanitizes_nested_queries_and_joins() {
        let input = "SELECT A.col FROM TableA A JOIN (SELECT col FROM TableB) AS B ON A.id = B.id";
        let expected = r#"SELECT "a"."col" FROM "tablea" AS "a" JOIN (SELECT "col" FROM "tableb") AS "b" ON "a"."id" = "b"."id""#;
        assert_eq!(sanitize_sql_idents(input).unwrap(), expected);
    }

    #[test]
    fn sanitizes_cte_names_and_columns() {
        let input = "WITH MyCTE (Col1, Col2) AS (SELECT A, B FROM Table1) SELECT Col1 FROM MyCTE";
        let expected = r#"WITH "mycte" ("col1", "col2") AS (SELECT "a", "b" FROM "table1") SELECT "col1" FROM "mycte""#;
        assert_eq!(sanitize_sql_idents(input).unwrap(), expected);
    }
}
