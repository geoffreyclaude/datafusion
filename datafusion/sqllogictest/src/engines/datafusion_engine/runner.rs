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

use std::{path::PathBuf, time::Duration};
use std::sync::Arc;
use super::{error::Result, normalize, DFSqlLogicTestError};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use indicatif::ProgressBar;
use log::Level::{Debug, Info};
use log::{debug, log_enabled, warn};
use sqllogictest::DBOutput;
use tokio::time::Instant;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use crate::engines::datafusion_engine::ddl_generator::generate_all_table_ddls;
use crate::engines::datafusion_engine::sql_helper::sanitize_sql_idents;
use crate::engines::datafusion_engine::substrait::to_substrait;
use crate::engines::output::{DFColumnType, DFOutput};

pub struct DataFusion {
    ctx: SessionContext,
    relative_path: PathBuf,
    pb: ProgressBar,
    use_substrait: bool,
}

const SUBSTRAIT_IGNORED_PREFIXES: &[&str] = &["copy", "create", "describe", "drop", "explain", "insert", "set", "show"];

impl DataFusion {
    pub fn new(ctx: SessionContext, relative_path: PathBuf, pb: ProgressBar, use_substrait: bool) -> Self {
        Self {
            ctx,
            relative_path,
            pb,
            use_substrait,
        }
    }

    fn update_slow_count(&self) {
        let msg = self.pb.message();
        let split: Vec<&str> = msg.split(" ").collect();
        let mut current_count = 0;

        if split.len() > 2 {
            // third match will be current slow count
            current_count = split[2].parse::<i32>().unwrap();
        }

        current_count += 1;

        self.pb
            .set_message(format!("{} - {} took > 500 ms", split[0], current_count));
    }

    async fn build_execution_plan_from_sql(&self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {
        let logical_plan = self.ctx.sql(sql).await?;
        logical_plan.create_physical_plan().await.map_err(DFSqlLogicTestError::DataFusion)
    }

    async fn build_execution_plan(&mut self, sql: &str) -> Result<Arc<dyn ExecutionPlan>> {

        if !self.use_substrait {
            return self.build_execution_plan_from_sql(sql).await;
        }

        if SUBSTRAIT_IGNORED_PREFIXES.iter().any(|p| sql.to_lowercase().starts_with(p)) {
            self.build_execution_plan_from_sql(sql).await
        } else {
            let ddls = generate_all_table_ddls(&self.ctx).await?;
            //let sql = sanitize_sql_idents(&sql)?;
            let substrait_plan = to_substrait(ddls.as_slice(), &sql)?;
            let session_state = &self.ctx.state();

            let logical_plan = from_substrait_plan(session_state, &substrait_plan).await?;
            let logical_plan = session_state.optimize(&logical_plan)?;

            session_state.create_physical_plan(&logical_plan).await.map_err(DFSqlLogicTestError::DataFusion)
        }
    }

    async fn run_query(&mut self, sql: &str) -> Result<DFOutput> {

        let plan = self.build_execution_plan(sql).await?;

        let task_ctx = self.ctx.state().task_ctx();
        let stream = execute_stream(plan, task_ctx)?;
        let types = normalize::convert_schema_to_types(stream.schema().fields());
        let results: Vec<RecordBatch> = collect(stream).await?;
        let rows = normalize::convert_batches(results)?;

        if rows.is_empty() && types.is_empty() {
            Ok(DBOutput::StatementComplete(0))
        } else {
            Ok(DBOutput::Rows { types, rows })
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for DataFusion {
    type Error = DFSqlLogicTestError;
    type ColumnType = DFColumnType;

    async fn run(&mut self, sql: &str) -> Result<DFOutput> {
        if log_enabled!(Debug) {
            debug!(
                "[{}] Running query: \"{}\"",
                self.relative_path.display(),
                sql
            );
        }

        let start = Instant::now();
        let result = self.run_query(sql).await;
        let duration = start.elapsed();

        if duration.gt(&Duration::from_millis(500)) {
            self.update_slow_count();
        }

        self.pb.inc(1);

        if log_enabled!(Info) && duration.gt(&Duration::from_secs(2)) {
            warn!(
                "[{}] Running query took more than 2 sec ({duration:?}): \"{sql}\"",
                self.relative_path.display()
            );
        }

        result
    }

    /// Engine name of current database.
    fn engine_name(&self) -> &str {
        "DataFusion"
    }

    /// [`DataFusion`] calls this function to perform sleep.
    ///
    /// The default implementation is `std::thread::sleep`, which is universal to any async runtime
    /// but would block the current thread. If you are running in tokio runtime, you should override
    /// this by `tokio::time::sleep`.
    async fn sleep(dur: Duration) {
        tokio::time::sleep(dur).await;
    }

    async fn shutdown(&mut self) {}
}
