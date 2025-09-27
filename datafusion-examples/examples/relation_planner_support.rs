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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DataFusionError, Result, TableReference};
use datafusion::logical_expr::{
    AggregateUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion::sql::parser::DFParser;
use datafusion::sql::planner::{ContextProvider, SqlToRel};

/// Minimal [`ContextProvider`] used by the relation planner examples.
///
/// The provider stores an in-memory map of [`TableReference`] values to
/// [`TableSource`] implementations so that [`SqlToRel`] can resolve table
/// scans while the custom relation planners rewrite individual relations.
pub struct InMemoryContextProvider {
    options: ConfigOptions,
    tables: HashMap<TableReference, Arc<dyn TableSource>>,
}

impl InMemoryContextProvider {
    /// Create a new provider with the given options and no registered tables.
    pub fn new(options: ConfigOptions) -> Self {
        Self {
            options,
            tables: HashMap::new(),
        }
    }

    /// Register a [`TableSource`] for the provided [`TableReference`].
    pub fn register_table(
        &mut self,
        reference: TableReference,
        table: Arc<dyn TableSource>,
    ) {
        self.tables.insert(reference, table);
    }

    /// Convenience helper to register a bare table name (e.g. "my_table").
    pub fn register_bare_table(&mut self, name: &str, table: Arc<dyn TableSource>) {
        self.register_table(TableReference::from(name.to_owned()), table);
    }
}

impl Default for InMemoryContextProvider {
    fn default() -> Self {
        Self::new(ConfigOptions::new())
    }
}

impl ContextProvider for InMemoryContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        self.tables
            .get(&name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("table '{name}' not found")))
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Parse `sql` and create a [`LogicalPlan`] using the provided `sql_to_rel` planner.
///
/// Examples use this helper to keep the focus on relation planner behavior
/// rather than SQL parsing boilerplate.
pub fn plan_sql<'a>(
    sql_to_rel: &SqlToRel<'a, InMemoryContextProvider>,
    sql: &str,
) -> Result<LogicalPlan> {
    let mut statements = DFParser::parse_sql(sql)?;
    let statement = statements.pop_front().ok_or_else(|| {
        DataFusionError::Plan(format!("SQL string contained no statements: {sql}"))
    })?;
    sql_to_rel.statement_to_plan(statement)
}
