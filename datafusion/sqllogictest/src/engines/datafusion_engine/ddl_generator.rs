use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::prelude::*;
use std::sync::Arc;

/// Map an Arrow data type to a Calcite type (as a string).
fn map_data_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::UInt8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::UInt16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::UInt32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt64 => "BIGINT".to_string(),
        DataType::Float16 => "REAL".to_string(), // Arrow’s Float16 -> Calcite REAL
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Utf8 => "VARCHAR".to_string(),
        DataType::Utf8View => "VARCHAR".to_string(),
        DataType::LargeUtf8 => "VARCHAR".to_string(),
        DataType::Binary => "VARBINARY".to_string(),
        DataType::LargeBinary => "VARBINARY".to_string(),
        DataType::Date32 => "DATE".to_string(),
        DataType::Date64 => "TIMESTAMP".to_string(), // Use TIMESTAMP for millisecond dates
        DataType::Time32(_) => "TIME".to_string(),
        DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, tz_opt) => {
            if tz_opt.is_some() {
                "TIMESTAMP WITH TIME ZONE".to_string()
            } else {
                "TIMESTAMP".to_string()
            }
        },
        DataType::Duration(_) => "INTERVAL".to_string(),
        DataType::Interval(interval_unit) => {
            use datafusion::arrow::datatypes::IntervalUnit;
            match interval_unit {
                IntervalUnit::YearMonth => "INTERVAL YEAR TO MONTH".to_string(),
                IntervalUnit::DayTime => "INTERVAL DAY TO SECOND".to_string(),
                // Fallback for other interval units.
                _ => "INTERVAL".to_string(),
            }
        },
        DataType::Decimal128(precision, scale) => format!("DECIMAL({}, {})", precision, scale),
        DataType::Decimal256(precision, scale) => format!("DECIMAL({}, {})", precision, scale),
        DataType::FixedSizeBinary(_) => "BINARY".to_string(),
        DataType::FixedSizeList(field, _) => {
            // Represent as an ARRAY of the element type.
            format!("{} ARRAY", map_data_type(field.data_type()))
        }
        DataType::List(field) => {
            format!("{} ARRAY", map_data_type(field.data_type()))
        }
        DataType::LargeList(field) => {
            format!("{} ARRAY", map_data_type(field.data_type()))
        }
        DataType::Struct(fields) => {
            // Represent a struct as a ROW type.
            let field_defs: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name(), map_data_type(f.data_type())))
                .collect();
            format!("ROW({})", field_defs.join(", "))
        }
        DataType::Dictionary(_, value_type) => map_data_type(value_type),
        DataType::Map(_, _) => "MAP".to_string(),
        DataType::Union(_, _) => "ANY".to_string(),
        DataType::Null => "UNKNOWN".to_string(),
        // Fallback if an unrecognized type is encountered.
        _ => "UNKNOWN".to_string(),
    }
}

/// Generate a Calcite-style "CREATE TABLE" statement from the table name and Arrow schema.
fn generate_create_table(table_name: &str, schema: &Schema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|field| {
            format!("\"{}\" {}", field.name(), map_data_type(field.data_type()))
        })
        .collect();
    format!("CREATE TABLE \"{}\" ({});", table_name, columns.join(", "))
}

/// Given a DataFusion SessionContext, iterate over all catalogs, schemas, and tables,
/// and return a vector of Calcite DDL strings (one per table).
pub async fn generate_all_table_ddls(
    ctx: &SessionContext,
) -> datafusion::error::Result<Vec<String>> {
    let mut ddls = Vec::new();

    // Iterate over all catalogs in the SessionContext.
    for catalog_name in ctx.catalog_names() {
        if let Some(catalog) = ctx.catalog(&catalog_name) {
            // Iterate over all schemas in the catalog.
            for schema_name in catalog.schema_names() {
                if let Some(schema_provider) = catalog.schema(&schema_name) {
                    // For each table name in the schema:
                    for table_name in schema_provider.table_names() {
                        // Obtain the table provider (async).
                        if catalog_name == "datafusion" && schema_name == "public" && table_name == "temp" {
                            // datafusion.public.temp is a temporary table that will panic on calling schema()
                            continue;
                        }
                        if let Some(table_provider) = schema_provider.table(&table_name).await? {
                            let arrow_schema: Arc<Schema> = table_provider.schema();
                            let ddl = generate_create_table(&table_name, &arrow_schema);
                            ddls.push(ddl);
                        }
                    }
                }
            }
        }
    }

    Ok(ddls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_map_data_type() {
        assert_eq!(map_data_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(map_data_type(&DataType::Int8), "TINYINT");
        assert_eq!(map_data_type(&DataType::Int16), "SMALLINT");
        assert_eq!(map_data_type(&DataType::Int32), "INTEGER");
        assert_eq!(map_data_type(&DataType::Int64), "BIGINT");
        assert_eq!(map_data_type(&DataType::Float32), "REAL");
        assert_eq!(map_data_type(&DataType::Float64), "DOUBLE");
        assert_eq!(map_data_type(&DataType::Utf8), "VARCHAR");
        assert_eq!(
            map_data_type(&DataType::Decimal128(10, 2)),
            "DECIMAL(10, 2)"
        );
    }

    #[test]
    fn test_generate_create_table() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("salary", DataType::Float64, false),
        ]);
        let ddl = generate_create_table("employees", &schema);
        assert_eq!(
            ddl,
            "CREATE TABLE employees (id INTEGER, name VARCHAR, salary DOUBLE);"
        );
    }
}
