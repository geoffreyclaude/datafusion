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

use arrow::compute::concat_batches;
use arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::error::Result;
use datafusion::physical_plan;
use datafusion::physical_plan::{displayable, ExecutionPlan};
use datafusion::prelude::*;
use datafusion_benchmarks::tpch::TPCH_TABLES;
use datafusion_common::utils::get_available_parallelism;
use datafusion_common::DEFAULT_PARQUET_EXTENSION;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fs, io};
use datafusion_common::instant::Instant;

fn create_external_table_query(year: i32, scratch_dir: &str) -> String {
    format!(
        r#"
        CREATE EXTERNAL TABLE lineitem_ship_{}(
            l_shipdate date,
            l_commitdate date,
            l_shipmode varchar,
            l_quantity int
        )
        STORED AS parquet
        LOCATION '{}{}/'
        WITH ORDER(l_shipdate)
    "#,
        year, scratch_dir, year
    )
}

fn create_insert_query(year: i32) -> String {
    format!(
        r#"
    INSERT INTO lineitem_ship_{}
    SELECT l_shipdate, l_commitdate, l_shipmode, l_quantity
    FROM lineitem
    WHERE EXTRACT(YEAR FROM l_shipdate) = {}
    ORDER BY l_shipdate
"#,
        year, year
    )
}

const TOP_K_QUERY: &str = r#"
    SELECT l_shipdate, l_commitdate, l_quantity
    FROM lineitem_ship_1992 WHERE l_shipmode IN ('MAIL', 'AIR')
    ORDER BY l_shipdate, l_commitdate, l_quantity
    LIMIT 10
"#;

const TOP_K_UNION_QUERY: &str = r#"
    SELECT l_shipdate, l_commitdate, l_quantity
    FROM (
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1992 WHERE l_shipmode IN ('MAIL', 'AIR')
        UNION ALL
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1993 WHERE l_shipmode IN ('MAIL', 'AIR')
        UNION ALL
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1994 WHERE l_shipmode IN ('MAIL', 'AIR')
        UNION ALL
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1995 WHERE l_shipmode IN ('MAIL', 'AIR')
        UNION ALL
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1996 WHERE l_shipmode IN ('MAIL', 'AIR')
        UNION ALL
        SELECT l_shipdate, l_commitdate, l_quantity FROM lineitem_ship_1997 WHERE l_shipmode IN ('KOALA')
    )
    ORDER BY l_shipdate, l_commitdate, l_quantity
    LIMIT 10
"#;

#[tokio::main]
async fn main() -> Result<()> {
    let mut session_config = SessionConfig::new();
    session_config
        .options_mut()
        .execution
        .parquet
        .pushdown_filters = true;
    let ctx = &SessionContext::new_with_config(session_config);

    init(ctx).await?;

    let start_time = Instant::now();
    run_query(ctx, TOP_K_QUERY).await?;
    println!("TopK query execution time: {:?}", start_time.elapsed());

    Ok(())
}

async fn init(ctx: &SessionContext) -> Result<()> {
    register_tpch_tables(ctx).await?;

    // Retrieve the "scratch/topk" directory path.
    let scratch_dir = scratch_dir();
    let scratch_path = Path::new(scratch_dir.as_str());

    let scratch_parent = scratch_path
        .parent()
        .expect("Failed to get parent directory of scratch dir");

    // Create the parent "scratch" directory if it doesn't exist.
    if !scratch_parent.exists() {
        fs::create_dir(scratch_parent)
            .expect("Failed to create parent scratch directory");
    }

    for year in 1992..=1997 {
        let year_path = scratch_path.join(format!("{}", year));
        if !year_path.as_path().exists() {
            fs::create_dir(year_path.as_path())
                .expect("Failed to create parent scratch directory");
        }
        let create_external_query =
            create_external_table_query(year, scratch_dir.as_str());
        run_query(ctx, create_external_query.as_str()).await?;
        if is_directory_empty(year_path.as_path()).unwrap_or(true) {
            let insert_query = create_insert_query(year);
            run_query(ctx, insert_query.as_str()).await?;
        }
    }

    Ok(())
}

/// Returns true if the given directory does not exist or is empty.
fn is_directory_empty(path: &Path) -> io::Result<bool> {
    if !path.exists() {
        return Ok(true);
    }
    let mut entries = fs::read_dir(path)?;
    Ok(entries.next().is_none())
}

async fn run_query(ctx: &SessionContext, sql: &str) -> Result<()> {
    println!("--------------------------------------------------------");
    println!("Query:\n{}\n", sql.trim());

    let df = ctx.sql(sql).await?;
    println!("Logical plan:\n{}\n", df.logical_plan().display_indent());

    let physical_plan = df.create_physical_plan().await?;
    println!(
        "Physical plan:\n{}\n",
        displayable(physical_plan.as_ref()).indent(true)
    );

    let mut root_node = physical_plan.clone();
    loop {
        let children = root_node.children();
        if children.is_empty() {
            break;
        }
        root_node = children[0].clone();
    }
    println!("Root Node:\n{:?}\n", root_node);

    let results =
        physical_plan::collect(physical_plan.clone(), ctx.state().task_ctx()).await?;
    let results = if results.len() <= 1 {
        results
    } else {
        vec![concat_batches(&results[0].schema(), &results)?]
    };
    println!("Results:\n{}\n", pretty_format_batches(&results)?);

    // Walk the physical plan and dump execution metrics for each node.
    dump_execution_metrics(&physical_plan, 0);

    Ok(())
}

/// Recursively walks the physical plan and prints each node's debug info and metrics.
fn dump_execution_metrics(plan: &Arc<dyn ExecutionPlan>, indent: usize) {
    let indent_str = " ".repeat(indent);

    // Print any available metrics for this node.
    if let Some(metrics) = plan.metrics() {
        let metrics = metrics.aggregate_by_name().sorted_for_display();
        println!("{}{} Metrics: {}", indent_str, plan.name(), metrics);
    } else {
        println!("{}{} No metrics available", indent_str, plan.name());
    }

    // Recursively dump metrics for each child node.
    for child in plan.children() {
        dump_execution_metrics(child, indent + 2);
    }
}

async fn register_tpch_tables(ctx: &SessionContext) -> Result<()> {
    for table in TPCH_TABLES {
        let table_provider = { get_table(ctx, table).await? };
        ctx.register_table(*table, table_provider)?;
    }
    Ok(())
}

async fn get_table(ctx: &SessionContext, table: &str) -> Result<Arc<dyn TableProvider>> {
    let target_partitions = get_available_parallelism();

    let format = Arc::new(
        ParquetFormat::default()
            .with_options(ctx.state().table_options().parquet.clone()),
    );

    let path = format!("{}/{}", tpch_data(), table);

    let state = ctx.state();

    let options = ListingOptions::new(format)
        .with_file_extension(DEFAULT_PARQUET_EXTENSION)
        .with_target_partitions(target_partitions)
        .with_collect_stat(state.config().collect_statistics());

    let table_path = ListingTableUrl::parse(path)?;
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .infer_schema(&state)
        .await?;

    Ok(Arc::new(ListingTable::try_new(config)?))
}

pub fn tpch_data() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("./data/tpch_sf10")
        .display()
        .to_string()
}

pub fn scratch_dir() -> String {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("./scratch/topk/")
        .display()
        .to_string()
}
