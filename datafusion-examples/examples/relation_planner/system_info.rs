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

//! This example exposes host metrics and identifiers as a virtual table
//! powered by a custom relation planner.
//!
//! Usage examples:
//!   - `SELECT * FROM system_info()` returns machine identity, CPU stats,
//!     memory usage, and load averages.
//!   - `SELECT * FROM TABLE(system_info())` demonstrates planning via the
//!     `TABLE()` syntax.
//!   - Metrics appear both as formatted strings and as raw numbers via the
//!     `numeric_value` column so they can participate in analytics.

use std::{sync::Arc, thread, time::Duration};

use datafusion::prelude::*;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::logical_plan::{builder::LogicalPlanBuilder, LogicalPlan};
use datafusion_expr::planner::{
    PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning,
};
use datafusion_expr::Expr;
use datafusion_sql::sqlparser::ast::{
    self, FunctionArg, FunctionArgExpr, ObjectNamePart, TableFactor,
};
use sysinfo::{CpuExt, System, SystemExt};

#[derive(Debug)]
struct SystemInfoPlanner;

impl RelationPlanner for SystemInfoPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        match relation {
            TableFactor::Function {
                lateral,
                name,
                args,
                alias,
            } => {
                if let Some(planned) = try_plan_system_info(&name, &args, alias.clone())?
                {
                    return Ok(RelationPlanning::Planned(planned));
                }

                Ok(RelationPlanning::Original(TableFactor::Function {
                    lateral,
                    name,
                    args,
                    alias,
                }))
            }
            TableFactor::TableFunction { expr, alias } => {
                if let ast::Expr::Function(func) = &expr {
                    if let ast::FunctionArguments::List(list) = &func.args {
                        if let Some(planned) =
                            try_plan_system_info(&func.name, &list.args, alias.clone())?
                        {
                            return Ok(RelationPlanning::Planned(planned));
                        }
                    }
                }

                Ok(RelationPlanning::Original(TableFactor::TableFunction {
                    expr,
                    alias,
                }))
            }
            other => Ok(RelationPlanning::Original(other)),
        }
    }
}

fn try_plan_system_info(
    name: &ast::ObjectName,
    args: &[FunctionArg],
    alias: Option<ast::TableAlias>,
) -> Result<Option<PlannedRelation>> {
    if !is_system_info(name) {
        return Ok(None);
    }

    if !args.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "system_info expects no arguments, got {}",
            args.len()
        )));
    }

    let plan = build_system_info_plan()?;
    Ok(Some(PlannedRelation::new(plan, alias)))
}

fn is_system_info(name: &ast::ObjectName) -> bool {
    name.0
        .last()
        .and_then(ObjectNamePart::as_ident)
        .is_some_and(|ident| ident.value.eq_ignore_ascii_case("system_info"))
}

#[allow(clippy::too_many_lines)]
fn build_system_info_plan() -> Result<LogicalPlan> {
    let mut system = System::new_all();
    system.refresh_all();
    system.refresh_cpu();
    thread::sleep(Duration::from_millis(200));
    system.refresh_cpu();

    let hostname = system.host_name().unwrap_or_else(|| "unknown".to_string());
    let os_version = system
        .long_os_version()
        .or_else(|| system.os_version())
        .unwrap_or_else(|| "unknown".to_string());
    let kernel_version = system
        .kernel_version()
        .unwrap_or_else(|| "unknown".to_string());
    let username = current_username();
    let architecture = std::env::consts::ARCH.to_string();
    let uptime_seconds = system.uptime() as f64;
    let boot_time = system.boot_time() as f64;

    let cpu_brand = system.global_cpu_info().brand().to_string();
    let cpu_frequency_mhz = system.global_cpu_info().frequency() as f64;
    let cpu_usage_percent = system.global_cpu_info().cpu_usage() as f64;
    let physical_cores = system.physical_core_count().map(|v| v as f64);
    let logical_cpus = system.cpus().len() as f64;

    let total_memory_mib = kib_to_mib(system.total_memory());
    let used_memory_mib = kib_to_mib(system.used_memory());
    let free_memory_mib = (total_memory_mib - used_memory_mib).max(0.0);
    let total_swap_mib = kib_to_mib(system.total_swap());
    let used_swap_mib = kib_to_mib(system.used_swap());

    let load = system.load_average();
    let processes = system.processes().len() as f64;

    let mut rows = Vec::new();

    push_metric(&mut rows, "identity", "hostname", hostname, None, None);
    push_metric(&mut rows, "identity", "username", username, None, None);
    push_metric(
        &mut rows,
        "identity",
        "architecture",
        architecture,
        None,
        None,
    );
    push_metric(
        &mut rows,
        "identity",
        "operating_system",
        os_version,
        None,
        None,
    );
    push_metric(
        &mut rows,
        "identity",
        "kernel_version",
        kernel_version,
        None,
        None,
    );
    push_metric(
        &mut rows,
        "identity",
        "uptime_seconds",
        format_number(uptime_seconds, 0),
        Some(uptime_seconds),
        Some("seconds"),
    );
    push_metric(
        &mut rows,
        "identity",
        "boot_time_seconds",
        format_number(boot_time, 0),
        Some(boot_time),
        Some("seconds"),
    );

    push_metric(&mut rows, "cpu", "cpu_model", cpu_brand, None, None);
    push_metric(
        &mut rows,
        "cpu",
        "logical_cpus",
        format_number(logical_cpus, 0),
        Some(logical_cpus),
        Some("count"),
    );
    push_metric(
        &mut rows,
        "cpu",
        "physical_cores",
        physical_cores
            .map(|value| format_number(value, 0))
            .unwrap_or_else(|| "unknown".to_string()),
        physical_cores,
        Some("count"),
    );
    push_metric(
        &mut rows,
        "cpu",
        "cpu_frequency_mhz",
        format_number(cpu_frequency_mhz, 0),
        Some(cpu_frequency_mhz),
        Some("MHz"),
    );
    push_metric(
        &mut rows,
        "cpu",
        "cpu_usage_percent",
        format_number(cpu_usage_percent, 2),
        Some(cpu_usage_percent),
        Some("percent"),
    );

    push_metric(
        &mut rows,
        "memory",
        "total_memory_mib",
        format_number(total_memory_mib, 2),
        Some(total_memory_mib),
        Some("MiB"),
    );
    push_metric(
        &mut rows,
        "memory",
        "used_memory_mib",
        format_number(used_memory_mib, 2),
        Some(used_memory_mib),
        Some("MiB"),
    );
    push_metric(
        &mut rows,
        "memory",
        "free_memory_mib",
        format_number(free_memory_mib, 2),
        Some(free_memory_mib),
        Some("MiB"),
    );
    push_metric(
        &mut rows,
        "memory",
        "total_swap_mib",
        format_number(total_swap_mib, 2),
        Some(total_swap_mib),
        Some("MiB"),
    );
    push_metric(
        &mut rows,
        "memory",
        "used_swap_mib",
        format_number(used_swap_mib, 2),
        Some(used_swap_mib),
        Some("MiB"),
    );

    push_metric(
        &mut rows,
        "load",
        "load_average_1m",
        format_number(load.one, 2),
        Some(load.one),
        None,
    );
    push_metric(
        &mut rows,
        "load",
        "load_average_5m",
        format_number(load.five, 2),
        Some(load.five),
        None,
    );
    push_metric(
        &mut rows,
        "load",
        "load_average_15m",
        format_number(load.fifteen, 2),
        Some(load.fifteen),
        None,
    );

    push_metric(
        &mut rows,
        "process",
        "process_count",
        format_number(processes, 0),
        Some(processes),
        Some("count"),
    );

    LogicalPlanBuilder::values(rows)?.build()
}

fn push_metric(
    rows: &mut Vec<Vec<Expr>>,
    category: &str,
    metric: &str,
    value: impl Into<String>,
    numeric_value: Option<f64>,
    unit: Option<&str>,
) {
    rows.push(vec![
        literal_utf8(category),
        literal_utf8(metric),
        literal_utf8(value.into()),
        literal_f64(numeric_value),
        literal_utf8_opt(unit.map(ToOwned::to_owned)),
    ]);
}

fn literal_utf8(value: impl Into<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(Some(value.into())), None)
}

fn literal_utf8_opt(value: Option<String>) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value), None)
}

fn literal_f64(value: Option<f64>) -> Expr {
    Expr::Literal(ScalarValue::Float64(value), None)
}

fn format_number(value: f64, decimals: usize) -> String {
    format!("{value:.decimals$}")
}

fn kib_to_mib(value: u64) -> f64 {
    value as f64 / 1024.0
}

fn current_username() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_relation_planner(Arc::new(SystemInfoPlanner))?;

    println!("Custom Relation Planner: System Metrics");
    println!("====================================\n");

    run_query(
        &ctx,
        "System identity",
        "SELECT metric, value FROM system_info() WHERE category = 'identity' ORDER BY metric",
    )
    .await?;

    run_query(
        &ctx,
        "CPU metrics",
        "SELECT metric, ROUND(numeric_value, 2) AS numeric_value, unit FROM system_info() WHERE category = 'cpu' ORDER BY metric",
    )
    .await?;

    run_query(
        &ctx,
        "Memory overview",
        "SELECT metric, value, unit FROM system_info() WHERE category = 'memory' ORDER BY metric",
    )
    .await?;

    run_query(
        &ctx,
        "Load averages via TABLE()",
        "SELECT metric, value FROM TABLE(system_info()) WHERE category = 'load' ORDER BY metric",
    )
    .await?;

    println!("Memory utilization computed from numeric_value (used/total * 100):");
    ctx.sql(
        "SELECT ROUND(100 * SUM(CASE WHEN metric = 'used_memory_mib' THEN numeric_value ELSE 0 END) \
         / NULLIF(SUM(CASE WHEN metric = 'total_memory_mib' THEN numeric_value ELSE 0 END), 0), 2) AS memory_used_percent \
         FROM system_info()",
    )
    .await?
    .show()
    .await?;

    Ok(())
}

async fn run_query(ctx: &SessionContext, title: &str, sql: &str) -> Result<()> {
    println!("{title}");
    println!("----------");
    println!("{sql}\n");

    ctx.sql(sql).await?.show().await?;
    println!();

    Ok(())
}
