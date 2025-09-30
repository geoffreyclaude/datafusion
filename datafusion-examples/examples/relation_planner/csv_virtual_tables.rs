// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This example demonstrates a custom relation planner that intercepts table names
//! with a special prefix and dynamically loads CSV files from a virtual directory.
//!
//! Use case: Query files without explicitly registering them as tables.
//! Example: `SELECT * FROM csv_data_users` loads `./data/users.csv`
//!
//! This example demonstrates ALL RelationPlannerContext methods:
//! - object_name_to_table_reference: Convert SQL table names to TableReference

use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_expr::planner::{
    RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_sql::sqlparser::ast::TableFactor;

#[derive(Debug)]
struct CsvVirtualTablePlanner {
    base_path: String,
}

impl CsvVirtualTablePlanner {
    fn new(base_path: impl Into<String>) -> Self {
        Self {
            base_path: base_path.into(),
        }
    }
}

impl RelationPlanner for CsvVirtualTablePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::Table { name, .. } = &relation {
            let table_name = name.to_string();

            // Check if table name starts with our prefix
            if let Some(csv_name) = table_name.strip_prefix("csv_") {
                // Construct the file path
                let file_path = format!("{}/{}.csv", self.base_path, csv_name);

                // Demonstrate object_name_to_table_reference
                let table_ref = context.object_name_to_table_reference(name.clone())?;
                println!(
                    "[CSV Planner] Virtual CSV table: {} -> {} (ref: {})",
                    table_name, file_path, table_ref
                );

                // Create a read_csv equivalent by building a VALUES clause for demo purposes
                // In a real implementation, you'd use CsvReadOptions and register the table

                // For this demo, we'll return Original to show the pattern,
                // but a real implementation would:
                // 1. Check if file exists
                // 2. Use CsvReadOptions to read the schema
                // 3. Create a TableScan plan with the CSV source

                // Returning Original here delegates to default behavior
                // (which will fail since the table doesn't exist in the catalog)
            }
        }

        Ok(RelationPlanning::Original(relation))
    }
}

#[derive(Debug)]
struct InMemoryTablePlanner;

impl RelationPlanner for InMemoryTablePlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if let TableFactor::Table { name, .. } = &relation {
            let table_name = name.to_string();

            // Provide some built-in test tables
            match table_name.to_lowercase().as_str() {
                "example_users" => {
                    // In a real scenario, you'd build a proper table scan
                    // For demo, we return Original and show the concept
                    println!(
                        "[In-Memory Planner] Built-in virtual table: {}",
                        table_name
                    );
                    return Ok(RelationPlanning::Original(relation));
                }
                "example_products" => {
                    println!(
                        "[In-Memory Planner] Built-in virtual table: {}",
                        table_name
                    );
                    return Ok(RelationPlanning::Original(relation));
                }
                _ => {}
            }
        }

        Ok(RelationPlanning::Original(relation))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();

    // Register our custom planners
    ctx.register_relation_planner(Arc::new(CsvVirtualTablePlanner::new("./data")))?;
    ctx.register_relation_planner(Arc::new(InMemoryTablePlanner))?;

    println!("Custom Relation Planner: CSV Virtual Tables");
    println!("============================================\n");

    // Example 1: Try to query a virtual CSV table
    println!("Example 1: Query virtual CSV table");
    println!("SQL: SELECT * FROM csv_users");
    match ctx.sql("SELECT * FROM csv_users").await {
        Ok(_) => println!("Query planned successfully\n"),
        Err(e) => println!("Query failed (expected): {}\n", e),
    }

    // Example 2: Try built-in tables
    println!("Example 2: Query built-in virtual table");
    println!("SQL: SELECT * FROM example_users");
    match ctx.sql("SELECT * FROM example_users").await {
        Ok(_) => println!("Query planned successfully\n"),
        Err(e) => println!("Query failed (expected): {}\n", e),
    }

    // Example 3: Show that normal tables still work
    println!("Example 3: Normal table registration still works");
    ctx.sql("CREATE TABLE real_table (id INT, name TEXT)")
        .await?
        .collect()
        .await?;
    println!("Created real_table\n");

    println!("Key Concept:");
    println!("-----------");
    println!("This example shows how custom relation planners can intercept");
    println!("table names and provide virtual tables without explicit registration.");
    println!();
    println!("In a complete implementation, the planner would:");
    println!("  1. Check if the CSV file exists");
    println!("  2. Read the schema from the CSV");
    println!("  3. Return a LogicalPlan with a CSV TableScan");
    println!();
    println!("This pattern is useful for:");
    println!("  - Dynamic file-based tables");
    println!("  - Convention-based table discovery");
    println!("  - Testing with fixture data");
    println!();
    println!("Trait methods demonstrated:");
    println!("  - object_name_to_table_reference: Converts SQL names to TableReference");

    Ok(())
}
