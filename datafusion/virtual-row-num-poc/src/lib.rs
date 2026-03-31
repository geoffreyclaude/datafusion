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

//! Proof-of-concept wrapper-only Parquet provider that exposes a stable
//! virtual `__virtual_row_num` column.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaBuilder, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use datafusion_catalog::{ScanArgs, ScanResult, Session, TableProvider};
use datafusion_catalog_listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion_common::stats::Precision;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, Statistics, exec_err, internal_err, plan_err,
};
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::{PartitionedFile, TableSchema};
use datafusion_datasource_parquet::metadata::DFParquetMetadata;
use datafusion_datasource_parquet::{ParquetAccessPlan, RowGroupAccess};
use datafusion_execution::TaskContext;
use datafusion_expr::expr::{Between, BinaryExpr, Cast, TryCast};
use datafusion_expr::{Expr, Operator, TableProviderFilterPushDown, TableType};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Distribution, EquivalenceProperties, Partitioning};
use datafusion_physical_expr_common::sort_expr::{LexOrdering, PhysicalSortExpr};
use datafusion_physical_plan::ExecutionPlanProperties;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use futures::StreamExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::ParquetMetaData;
use tokio::sync::OnceCell;

/// Name of the virtual row number column exposed by this provider.
pub const VIRTUAL_ROW_NUM_COLUMN: &str = "__virtual_row_num";

/// A proof-of-concept wrapper around [`ListingTable`] for Parquet reads that
/// adds a stable 0-based virtual row number column.
#[derive(Debug)]
pub struct VirtualRowNumProvider {
    inner: ListingTable,
    base_table_schema: TableSchema,
    extended_schema: SchemaRef,
    inventory: OnceCell<Arc<FileInventory>>,
}

impl VirtualRowNumProvider {
    /// Creates a new Parquet-only virtual row number provider.
    pub async fn try_new(
        state: &dyn Session,
        config: ListingTableConfig,
    ) -> Result<Self> {
        let mut config = if config.file_schema.is_some() {
            config
        } else {
            config.infer_schema(state).await?
        };

        let options = config.options.as_ref().ok_or_else(|| {
            DataFusionError::Internal("No ListingOptions provided".into())
        })?;
        ensure_parquet_format(options)?;

        let partition_fields = options
            .table_partition_cols
            .iter()
            .map(|(name, data_type)| Arc::new(Field::new(name, data_type.clone(), false)))
            .collect::<Vec<_>>();

        let file_schema = Arc::clone(
            config
                .file_schema
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("No schema provided".into()))?,
        );
        let base_table_schema =
            TableSchema::new(Arc::clone(&file_schema), partition_fields);

        if let Some(options) = config.options.as_mut() {
            options.collect_stat = true;
        }

        let inner = ListingTable::try_new(config)?;
        let extended_schema = build_extended_schema(base_table_schema.table_schema())?;

        Ok(Self {
            inner,
            base_table_schema,
            extended_schema,
            inventory: OnceCell::new(),
        })
    }

    async fn inventory(&self, state: &dyn Session) -> Result<Arc<FileInventory>> {
        let inventory = self
            .inventory
            .get_or_try_init(|| async { self.build_inventory(state).await.map(Arc::new) })
            .await?;
        Ok(Arc::clone(inventory))
    }

    async fn build_inventory(&self, state: &dyn Session) -> Result<FileInventory> {
        let files = self
            .inner
            .list_files_for_scan(state, &[], None)
            .await?
            .file_groups
            .into_iter()
            .flat_map(FileGroup::into_inner)
            .collect::<Vec<_>>();

        build_inventory_from_files(files)
    }

    fn base_schema(&self) -> &SchemaRef {
        self.base_table_schema.table_schema()
    }

    fn row_num_index(&self) -> usize {
        self.base_schema().fields().len()
    }

    async fn build_scan_plan(
        &self,
        state: &dyn Session,
        projection: Option<&[usize]>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let inventory = self.inventory(state).await?;
        let window = compute_row_window(filters, limit);
        let bounded_window = inventory.bound_window(window);
        if bounded_window.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.extended_schema,
                projection,
            )?)));
        }

        let projected_base_indices =
            projected_base_indices(projection, self.row_num_index());
        let projected_base_schema =
            project_base_schema(self.base_schema(), projected_base_indices.as_deref())?;
        let selected_files = self
            .selected_files(state, &inventory, bounded_window)
            .await?;

        if selected_files.is_empty() {
            return Ok(Arc::new(EmptyExec::new(project_schema(
                &self.extended_schema,
                projection,
            )?)));
        }

        let file_source = self.inner.options().format.file_source(TableSchema::new(
            Arc::clone(self.base_table_schema.file_schema()),
            self.base_table_schema.table_partition_cols().clone(),
        ));

        let object_store_url = self
            .inner
            .table_paths()
            .first()
            .ok_or_else(|| DataFusionError::Internal("No table path configured".into()))?
            .object_store();

        let mut builder = FileScanConfigBuilder::new(object_store_url, file_source)
            .with_file_groups(vec![FileGroup::new(selected_files)])
            .with_preserve_order(true)
            .with_statistics(Statistics::new_unknown(self.base_schema().as_ref()));

        if let Some(indices) = projected_base_indices.clone() {
            builder = builder.with_projection_indices(Some(indices))?;
        }

        let child = self
            .inner
            .options()
            .format
            .create_physical_plan(state, builder.build())
            .await?;

        let wrapper = Arc::new(VirtualRowNumExec::new(
            child,
            bounded_window.start,
            &projected_base_schema,
        ));

        if let Some(projection) = projection {
            project_output(
                wrapper,
                projection,
                projected_base_indices.as_deref(),
                self.row_num_index(),
            )
        } else {
            Ok(wrapper)
        }
    }

    async fn selected_files(
        &self,
        state: &dyn Session,
        inventory: &FileInventory,
        window: BoundedRowRange,
    ) -> Result<Vec<PartitionedFile>> {
        let store = self.object_store(state)?;
        let mut files = Vec::new();
        for entry in &inventory.entries {
            if entry.row_end <= window.start {
                continue;
            }
            if entry.row_start >= window.end {
                break;
            }

            let local_start = window.start.saturating_sub(entry.row_start);
            let local_end = (window.end.min(entry.row_end)) - entry.row_start;
            let row_count = entry.row_count();

            let mut file = entry.file.clone();
            if local_start != 0 || local_end != row_count {
                let metadata =
                    parquet_metadata(store.as_ref(), &file, self.inner.options()).await?;
                let access_plan =
                    build_access_plan(metadata.as_ref(), local_start, local_end)?;
                file = file.with_extensions(Arc::new(access_plan));
            }
            files.push(file);
        }
        Ok(files)
    }

    fn object_store(&self, state: &dyn Session) -> Result<Arc<dyn ObjectStore>> {
        let object_store_url = self
            .inner
            .table_paths()
            .first()
            .ok_or_else(|| DataFusionError::Internal("No table path configured".into()))?
            .object_store();
        state.runtime_env().object_store(&object_store_url)
    }
}

/// Registers a [`VirtualRowNumProvider`] into the provided [`SessionContext`].
pub async fn register_virtual_row_num_table(
    session_ctx: &SessionContext,
    name: &str,
    config: ListingTableConfig,
) -> Result<()> {
    let state = session_ctx.state();
    let provider = Arc::new(VirtualRowNumProvider::try_new(&state, config).await?);
    session_ctx.register_table(name, provider)?;
    Ok(())
}

#[async_trait]
impl TableProvider for VirtualRowNumProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.extended_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.build_scan_plan(state, projection.map(Vec::as_slice), filters, limit)
            .await
    }

    async fn scan_with_args<'a>(
        &self,
        state: &dyn Session,
        args: ScanArgs<'a>,
    ) -> Result<ScanResult> {
        let filters = args.filters().unwrap_or(&[]);
        let plan = self
            .build_scan_plan(state, args.projection(), filters, args.limit())
            .await?;
        Ok(plan.into())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if parse_rownum_filter(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct FileInventory {
    entries: Vec<FileInventoryEntry>,
    total_rows: u64,
}

impl FileInventory {
    fn bound_window(&self, window: RowRange) -> BoundedRowRange {
        let start = window.start.min(self.total_rows);
        let end = window.end.unwrap_or(self.total_rows).min(self.total_rows);
        BoundedRowRange { start, end }
    }
}

#[derive(Debug, Clone)]
struct FileInventoryEntry {
    file: PartitionedFile,
    row_start: u64,
    row_end: u64,
}

impl FileInventoryEntry {
    fn row_count(&self) -> u64 {
        self.row_end - self.row_start
    }
}

fn build_inventory_from_files(mut files: Vec<PartitionedFile>) -> Result<FileInventory> {
    files.sort_by(|left, right| left.path().as_ref().cmp(right.path().as_ref()));

    let mut entries = Vec::with_capacity(files.len());
    let mut next_row_start = 0_u64;
    for file in files {
        let stats = file.statistics.as_ref().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Missing statistics for parquet file {}",
                file.path()
            ))
        })?;
        let row_count = match stats.num_rows {
            Precision::Exact(value) => u64::try_from(value).map_err(|_| {
                DataFusionError::Internal(format!(
                    "Row count does not fit in u64 for parquet file {}",
                    file.path()
                ))
            })?,
            other => {
                return exec_err!(
                    "Exact row count required for parquet file {} but found {other:?}",
                    file.path()
                );
            }
        };
        let row_end = next_row_start
            .checked_add(row_count)
            .ok_or_else(|| DataFusionError::Internal("row number overflow".into()))?;
        entries.push(FileInventoryEntry {
            file,
            row_start: next_row_start,
            row_end,
        });
        next_row_start = row_end;
    }

    Ok(FileInventory {
        entries,
        total_rows: next_row_start,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowRange {
    start: u64,
    end: Option<u64>,
}

impl RowRange {
    fn all() -> Self {
        Self {
            start: 0,
            end: None,
        }
    }

    fn empty_at(start: u64) -> Self {
        Self {
            start,
            end: Some(start),
        }
    }

    fn exact(value: u64) -> Self {
        Self {
            start: value,
            end: value.checked_add(1),
        }
    }

    fn intersect(self, other: Self) -> Self {
        let start = self.start.max(other.start);
        let end = match (self.end, other.end) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (None, None) => None,
        };
        Self { start, end }
    }

    fn with_limit(self, limit: usize) -> Self {
        let Some(limit) = u64::try_from(limit).ok() else {
            return self;
        };
        let limited_end = self.start.checked_add(limit);
        let end = match (self.end, limited_end) {
            (Some(existing), Some(limited)) => Some(existing.min(limited)),
            (Some(existing), None) => Some(existing),
            (None, limited) => limited,
        };
        Self {
            start: self.start,
            end,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundedRowRange {
    start: u64,
    end: u64,
}

impl BoundedRowRange {
    fn is_empty(self) -> bool {
        self.start >= self.end
    }
}

fn compute_row_window(filters: &[Expr], limit: Option<usize>) -> RowRange {
    let range = filters
        .iter()
        .filter_map(parse_rownum_filter)
        .fold(RowRange::all(), RowRange::intersect);

    if let Some(limit) = limit {
        range.with_limit(limit)
    } else {
        range
    }
}

fn parse_rownum_filter(expr: &Expr) -> Option<RowRange> {
    match strip_wrappers(expr) {
        Expr::Between(Between {
            expr,
            negated: false,
            low,
            high,
        }) if is_rownum_column(expr) => {
            let low = literal_to_i128(low)?;
            let high = literal_to_i128(high)?;
            Some(
                range_from_operator(Operator::GtEq, low)
                    .intersect(range_from_operator(Operator::LtEq, high)),
            )
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) if *op == Operator::And => {
            let left = parse_rownum_filter(left)?;
            let right = parse_rownum_filter(right)?;
            Some(left.intersect(right))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            parse_binary_rownum_filter(left, *op, right)
        }
        _ => None,
    }
}

fn parse_binary_rownum_filter(
    left: &Expr,
    op: Operator,
    right: &Expr,
) -> Option<RowRange> {
    if is_rownum_column(left) {
        let literal = literal_to_i128(right)?;
        return Some(range_from_operator(op, literal));
    }

    if is_rownum_column(right) {
        let literal = literal_to_i128(left)?;
        return Some(range_from_operator(reverse_operator(op)?, literal));
    }

    None
}

fn reverse_operator(op: Operator) -> Option<Operator> {
    match op {
        Operator::Eq => Some(Operator::Eq),
        Operator::Gt => Some(Operator::Lt),
        Operator::GtEq => Some(Operator::LtEq),
        Operator::Lt => Some(Operator::Gt),
        Operator::LtEq => Some(Operator::GtEq),
        _ => None,
    }
}

fn range_from_operator(op: Operator, value: i128) -> RowRange {
    const MAX_ROW_NUM: i128 = u64::MAX as i128;

    match op {
        Operator::Eq => {
            if (0..=MAX_ROW_NUM).contains(&value) {
                RowRange::exact(value as u64)
            } else {
                RowRange::empty_at(0)
            }
        }
        Operator::Gt => {
            if value < 0 {
                RowRange::all()
            } else if value >= MAX_ROW_NUM {
                RowRange::empty_at(u64::MAX)
            } else {
                RowRange {
                    start: (value + 1) as u64,
                    end: None,
                }
            }
        }
        Operator::GtEq => {
            if value <= 0 {
                RowRange::all()
            } else if value > MAX_ROW_NUM {
                RowRange::empty_at(u64::MAX)
            } else {
                RowRange {
                    start: value as u64,
                    end: None,
                }
            }
        }
        Operator::Lt => {
            if value <= 0 {
                RowRange::empty_at(0)
            } else if value > MAX_ROW_NUM {
                RowRange::all()
            } else {
                RowRange {
                    start: 0,
                    end: Some(value as u64),
                }
            }
        }
        Operator::LtEq => {
            if value < 0 {
                RowRange::empty_at(0)
            } else if value >= MAX_ROW_NUM {
                RowRange::all()
            } else {
                RowRange {
                    start: 0,
                    end: Some((value as u64) + 1),
                }
            }
        }
        _ => RowRange::all(),
    }
}

fn strip_wrappers(expr: &Expr) -> &Expr {
    match expr {
        Expr::Alias(alias) => strip_wrappers(alias.expr.as_ref()),
        Expr::Cast(Cast { expr, .. }) => strip_wrappers(expr),
        Expr::TryCast(TryCast { expr, .. }) => strip_wrappers(expr),
        _ => expr,
    }
}

fn is_rownum_column(expr: &Expr) -> bool {
    matches!(strip_wrappers(expr), Expr::Column(column) if column.name == VIRTUAL_ROW_NUM_COLUMN)
}

fn literal_to_i128(expr: &Expr) -> Option<i128> {
    match strip_wrappers(expr) {
        Expr::Literal(value, _) => scalar_to_i128(value),
        _ => None,
    }
}

fn scalar_to_i128(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

fn build_extended_schema(base_schema: &SchemaRef) -> Result<SchemaRef> {
    let mut builder = SchemaBuilder::from(base_schema.as_ref());
    builder.push(Field::new(VIRTUAL_ROW_NUM_COLUMN, DataType::UInt64, false));
    Ok(Arc::new(builder.finish()))
}

fn project_schema(schema: &SchemaRef, projection: Option<&[usize]>) -> Result<SchemaRef> {
    if let Some(projection) = projection {
        Ok(Arc::new(schema.as_ref().project(projection)?))
    } else {
        Ok(Arc::clone(schema))
    }
}

fn projected_base_indices(
    projection: Option<&[usize]>,
    row_num_index: usize,
) -> Option<Vec<usize>> {
    projection.map(|projection| {
        let mut seen = HashSet::new();
        projection
            .iter()
            .copied()
            .filter(|index| *index < row_num_index)
            .filter(|index| seen.insert(*index))
            .collect()
    })
}

fn project_base_schema(
    base_schema: &SchemaRef,
    projected_base_indices: Option<&[usize]>,
) -> Result<SchemaRef> {
    if let Some(indices) = projected_base_indices {
        Ok(Arc::new(base_schema.as_ref().project(indices)?))
    } else {
        Ok(Arc::clone(base_schema))
    }
}

fn project_output(
    input: Arc<dyn ExecutionPlan>,
    projection: &[usize],
    projected_base_indices: Option<&[usize]>,
    original_row_num_index: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let input_schema = input.schema();
    let input_fields = input_schema.fields();
    let row_num_position = input_fields.len() - 1;

    let base_positions = if let Some(indices) = projected_base_indices {
        indices
            .iter()
            .enumerate()
            .map(|(position, index)| (*index, position))
            .collect::<HashMap<_, _>>()
    } else {
        (0..row_num_position)
            .map(|index| (index, index))
            .collect::<HashMap<_, _>>()
    };

    let projection_exprs = projection
        .iter()
        .map(|index| {
            let input_index = if *index == original_row_num_index {
                row_num_position
            } else {
                base_positions.get(index).copied().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Projection index {index} not available in virtual row num scan"
                    ))
                })?
            };
            let field = input_schema.field(input_index);
            Ok(ProjectionExpr {
                expr: Arc::new(Column::new(field.name(), input_index)),
                alias: field.name().to_string(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Arc::new(ProjectionExec::try_new(projection_exprs, input)?))
}

async fn parquet_metadata(
    store: &dyn ObjectStore,
    file: &PartitionedFile,
    options: &ListingOptions,
) -> Result<Arc<ParquetMetaData>> {
    let metadata_size_hint = file.metadata_size_hint.or_else(|| {
        options
            .format
            .as_any()
            .downcast_ref::<datafusion_datasource_parquet::file_format::ParquetFormat>()
            .and_then(|format| format.metadata_size_hint())
    });

    DFParquetMetadata::new(store, &file.object_meta)
        .with_metadata_size_hint(metadata_size_hint)
        .fetch_metadata()
        .await
        .map_err(|e| {
            DataFusionError::Context(
                format!("Failed to fetch parquet metadata for {}", file.path()),
                Box::new(e),
            )
        })
}

fn build_access_plan(
    metadata: &ParquetMetaData,
    file_start: u64,
    file_end: u64,
) -> Result<ParquetAccessPlan> {
    let row_groups = metadata.row_groups();
    let mut access_plan = ParquetAccessPlan::new_none(row_groups.len());
    let mut next_row_group_start = 0_u64;

    for (index, row_group) in row_groups.iter().enumerate() {
        let row_group_rows = u64::try_from(row_group.num_rows()).map_err(|_| {
            DataFusionError::Internal("Row group row count does not fit in u64".into())
        })?;
        let row_group_end = next_row_group_start + row_group_rows;
        let overlap_start = file_start.max(next_row_group_start);
        let overlap_end = file_end.min(row_group_end);

        if overlap_start < overlap_end {
            if overlap_start == next_row_group_start && overlap_end == row_group_end {
                access_plan.set(index, RowGroupAccess::Scan);
            } else {
                access_plan.set(
                    index,
                    RowGroupAccess::Selection(contiguous_row_selection(
                        overlap_start - next_row_group_start,
                        overlap_end - next_row_group_start,
                        row_group_rows,
                    )),
                );
            }
        }

        next_row_group_start = row_group_end;
    }

    Ok(access_plan)
}

fn contiguous_row_selection(start: u64, end: u64, total_rows: u64) -> RowSelection {
    let mut selectors = Vec::new();
    if start > 0 {
        selectors.push(RowSelector::skip(start as usize));
    }
    let select_count = end - start;
    if select_count > 0 {
        selectors.push(RowSelector::select(select_count as usize));
    }
    let trailing = total_rows - end;
    if trailing > 0 {
        selectors.push(RowSelector::skip(trailing as usize));
    }
    RowSelection::from(selectors)
}

fn ensure_parquet_format(options: &ListingOptions) -> Result<()> {
    if options
        .format
        .as_any()
        .is::<datafusion_datasource_parquet::file_format::ParquetFormat>()
    {
        Ok(())
    } else {
        plan_err!("VirtualRowNumProvider only supports Parquet listing tables")
    }
}

/// Unary execution plan that appends the stable virtual row number column.
#[derive(Debug)]
pub struct VirtualRowNumExec {
    input: Arc<dyn ExecutionPlan>,
    start_row_num: u64,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl VirtualRowNumExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        start_row_num: u64,
        base_schema: &SchemaRef,
    ) -> Self {
        let schema = build_extended_schema(base_schema).expect("schema extension");
        let ordering = LexOrdering::new([PhysicalSortExpr {
            expr: Arc::new(Column::new(
                VIRTUAL_ROW_NUM_COLUMN,
                schema.fields().len() - 1,
            )),
            options: SortOptions {
                descending: false,
                nulls_first: false,
            },
        }])
        .expect("non-empty ordering");
        let cache = Arc::new(PlanProperties::new(
            EquivalenceProperties::new_with_orderings(Arc::clone(&schema), [ordering]),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        ));

        Self {
            input,
            start_row_num,
            schema,
            cache,
        }
    }

    /// Returns the wrapped child plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for VirtualRowNumExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VirtualRowNumExec: start_row_num={}, row_num_column={}",
                    self.start_row_num, VIRTUAL_ROW_NUM_COLUMN
                )
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "start_row_num={}", self.start_row_num)?;
                write!(f, "row_num_column={VIRTUAL_ROW_NUM_COLUMN}")
            }
        }
    }
}

impl ExecutionPlan for VirtualRowNumExec {
    fn name(&self) -> &'static str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &dyn datafusion_physical_plan::PhysicalExpr,
        )
            -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
        let mut recursion = datafusion_common::tree_node::TreeNodeRecursion::Continue;
        if let Some(ordering) = self.cache.output_ordering() {
            for sort_expr in ordering {
                recursion = recursion.visit_sibling(|| f(sort_expr.expr.as_ref()))?;
            }
        }
        Ok(recursion)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!(
                "VirtualRowNumExec expected 1 child but got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            self.start_row_num,
            &Arc::new(Schema::new(
                self.schema.fields()[..self.schema.fields().len() - 1].to_vec(),
            )),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!(
                "VirtualRowNumExec expects a single output partition but received {partition}"
            );
        }

        if self.input.output_partitioning().partition_count() != 1 {
            return exec_err!(
                "VirtualRowNumExec requires a single input partition but received {}",
                self.input.output_partitioning().partition_count()
            );
        }

        let schema = Arc::clone(&self.schema);
        let stream_schema = Arc::clone(&schema);
        let mut next_row_num = self.start_row_num;
        let stream = self.input.execute(0, context)?.map(move |batch| {
            let batch = batch?;
            let row_count = u64::try_from(batch.num_rows()).map_err(|_| {
                DataFusionError::Internal("Batch row count does not fit in u64".into())
            })?;
            let row_nums =
                UInt64Array::from_iter_values(next_row_num..next_row_num + row_count);
            next_row_num += row_count;

            let mut columns = batch.columns().to_vec();
            columns.push(Arc::new(row_nums) as ArrayRef);
            Ok(RecordBatch::try_new(Arc::clone(&schema), columns)?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::path::Path;

    use arrow::array::{BinaryArray, Int32Array, RecordBatchOptions};
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::assert_batches_eq;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_datasource::ListingTableUrl;
    use datafusion_datasource::file_scan_config::FileScanConfig;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
    use datafusion_physical_plan::metrics::{MetricValue, MetricsSet};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    #[tokio::test]
    async fn parses_supported_rownum_filters() {
        let gt = parse_rownum_filter(
            &datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                .gt_eq(datafusion_expr::lit(4_i64)),
        )
        .unwrap();
        assert_eq!(
            gt,
            RowRange {
                start: 4,
                end: None
            }
        );

        let between = parse_rownum_filter(&Expr::Between(Between::new(
            Box::new(datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)),
            false,
            Box::new(datafusion_expr::lit(2_i64)),
            Box::new(datafusion_expr::lit(5_i64)),
        )))
        .unwrap();
        assert_eq!(
            between,
            RowRange {
                start: 2,
                end: Some(6)
            }
        );

        let and_filter = datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
            .gt(datafusion_expr::lit(2_i64))
            .and(
                datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .lt_eq(datafusion_expr::lit(9_i64)),
            );
        let range = parse_rownum_filter(&and_filter).unwrap();
        assert_eq!(
            range,
            RowRange {
                start: 3,
                end: Some(10)
            }
        );
    }

    #[tokio::test]
    async fn rejects_unsupported_rownum_filters() {
        assert!(
            parse_rownum_filter(
                &datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .eq(datafusion_expr::col("value"))
            )
            .is_none()
        );

        assert!(
            parse_rownum_filter(
                &datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .gt(datafusion_expr::lit(2_i64))
                    .or(datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                        .lt(datafusion_expr::lit(9_i64)))
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn builds_inventory_in_lexicographic_path_order() -> Result<()> {
        let files = vec![
            partitioned_file_with_rows("b.parquet", 2),
            partitioned_file_with_rows("a.parquet", 3),
            partitioned_file_with_rows("c.parquet", 4),
        ];

        let inventory = build_inventory_from_files(files)?;
        let paths = inventory
            .entries
            .iter()
            .map(|entry| entry.file.path().to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths, vec!["a.parquet", "b.parquet", "c.parquet"]);
        assert_eq!(inventory.entries[0].row_start, 0);
        assert_eq!(inventory.entries[0].row_end, 3);
        assert_eq!(inventory.entries[1].row_start, 3);
        assert_eq!(inventory.entries[1].row_end, 5);
        assert_eq!(inventory.entries[2].row_start, 5);
        assert_eq!(inventory.entries[2].row_end, 9);
        Ok(())
    }

    #[tokio::test]
    async fn builds_partial_access_plan_for_boundary_file() -> Result<()> {
        let tempdir = TempDir::new()?;
        let schema = write_test_dataset(tempdir.path())?;
        let file_path = tempdir.path().join("b.parquet");
        let object_meta = std::fs::metadata(&file_path)?;
        let file = PartitionedFile::new(
            file_path.to_string_lossy().into_owned(),
            object_meta.len(),
        );
        let store = object_store::local::LocalFileSystem::new();
        let metadata = parquet_metadata(
            &store,
            &file,
            &ListingOptions::new(Arc::new(
                datafusion_datasource_parquet::file_format::ParquetFormat::new(),
            )),
        )
        .await?;
        let access_plan = build_access_plan(metadata.as_ref(), 1, 4)?;
        let expected = ParquetAccessPlan::new(vec![
            RowGroupAccess::Selection(RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(1),
            ])),
            RowGroupAccess::Scan,
        ]);
        assert_eq!(access_plan, expected);
        assert_eq!(schema.fields().len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn virtual_row_num_exec_appends_values_across_batches() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            false,
        )]));
        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
        )?;
        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![3])) as ArrayRef],
        )?;
        let input = Arc::new(TestBatchesExec::new(
            Arc::clone(&schema),
            vec![batch1, batch2],
        ));
        let exec = VirtualRowNumExec::new(input, 5, &schema);
        let ctx = SessionContext::new();
        let batches =
            datafusion::physical_plan::collect(Arc::new(exec), ctx.task_ctx()).await?;
        assert_batches_eq!(
            [
                "+-------+-------------------+",
                "| value | __virtual_row_num |",
                "+-------+-------------------+",
                "| 1     | 5                 |",
                "| 2     | 6                 |",
                "| 3     | 7                 |",
                "+-------+-------------------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn cursor_query_returns_reproducible_results() -> Result<()> {
        let tempdir = TempDir::new()?;
        write_test_dataset(tempdir.path())?;
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(4),
        );
        register_virtual_row_num_table(
            &ctx,
            "virtual_row_num_parquet",
            ListingTableConfig::new(ListingTableUrl::parse(
                tempdir.path().to_string_lossy().as_ref(),
            )?)
            .with_listing_options(ListingOptions::new(Arc::new(
                datafusion_datasource_parquet::file_format::ParquetFormat::new(),
            ))),
        )
        .await?;

        let df = ctx
            .sql(
                "SELECT __virtual_row_num, id \
                 FROM virtual_row_num_parquet \
                 WHERE __virtual_row_num > 4 \
                 ORDER BY __virtual_row_num \
                 LIMIT 3",
            )
            .await?;
        let batches = df.collect().await?;
        assert_batches_eq!(
            [
                "+-------------------+----+",
                "| __virtual_row_num | id |",
                "+-------------------+----+",
                "| 5                 | 21 |",
                "| 6                 | 22 |",
                "| 7                 | 23 |",
                "+-------------------+----+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn mixed_filter_keeps_sparse_row_numbers() -> Result<()> {
        let tempdir = TempDir::new()?;
        write_test_dataset(tempdir.path())?;
        let ctx = SessionContext::new();
        register_virtual_row_num_table(
            &ctx,
            "virtual_row_num_parquet",
            ListingTableConfig::new(ListingTableUrl::parse(
                tempdir.path().to_string_lossy().as_ref(),
            )?)
            .with_listing_options(ListingOptions::new(Arc::new(
                datafusion_datasource_parquet::file_format::ParquetFormat::new(),
            ))),
        )
        .await?;

        let batches = ctx
            .sql(
                "SELECT __virtual_row_num, value \
                 FROM virtual_row_num_parquet \
                 WHERE __virtual_row_num > 2 AND value % 2 = 0 \
                 ORDER BY __virtual_row_num",
            )
            .await?
            .collect()
            .await?;

        assert_batches_eq!(
            [
                "+-------------------+-------+",
                "| __virtual_row_num | value |",
                "+-------------------+-------+",
                "| 3                 | 8     |",
                "| 5                 | 2     |",
                "| 7                 | 4     |",
                "| 9                 | 10    |",
                "| 11                | 12    |",
                "+-------------------+-------+",
            ],
            &batches
        );
        Ok(())
    }

    #[tokio::test]
    async fn explain_query_uses_virtual_row_num_exec_without_sort() -> Result<()> {
        let tempdir = TempDir::new()?;
        write_test_dataset(tempdir.path())?;
        let ctx = SessionContext::new();
        register_virtual_row_num_table(
            &ctx,
            "virtual_row_num_parquet",
            ListingTableConfig::new(ListingTableUrl::parse(
                tempdir.path().to_string_lossy().as_ref(),
            )?)
            .with_listing_options(ListingOptions::new(Arc::new(
                datafusion_datasource_parquet::file_format::ParquetFormat::new(),
            ))),
        )
        .await?;

        let batches = ctx
            .sql(
                "EXPLAIN SELECT __virtual_row_num, id \
                 FROM virtual_row_num_parquet \
                 WHERE __virtual_row_num > 4 \
                 ORDER BY __virtual_row_num \
                 LIMIT 3",
            )
            .await?
            .collect()
            .await?;

        let plan = pretty_format_batches(&batches)?.to_string();
        assert!(plan.contains("VirtualRowNumExec"));
        assert!(!plan.contains("SortExec"));
        assert!(!plan.contains("SortPreservingMergeExec"));
        Ok(())
    }

    #[tokio::test]
    async fn virtual_row_num_scan_reads_only_one_needed_row_group() -> Result<()> {
        let fixture = LargePayloadFixture::new().await?;

        let full_plan = fixture
            .provider
            .scan(
                &fixture.ctx.state(),
                Some(&vec![fixture.payload_index, fixture.row_num_index]),
                &[],
                None,
            )
            .await?;
        let (_, full_bytes_scanned) =
            collect_plan_and_bytes_scanned(&full_plan, &fixture.ctx).await?;

        let filtered_plan = fixture
            .provider
            .scan(
                &fixture.ctx.state(),
                Some(&vec![fixture.payload_index, fixture.row_num_index]),
                &[datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .eq(datafusion_expr::lit(1_i64))],
                None,
            )
            .await?;
        let (batches, filtered_bytes_scanned) =
            collect_plan_and_bytes_scanned(&filtered_plan, &fixture.ctx).await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(
            file_access_plan(filtered_plan.as_ref())?,
            &ParquetAccessPlan::new(vec![
                RowGroupAccess::Skip,
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
                RowGroupAccess::Skip,
            ])
        );
        assert!(
            filtered_bytes_scanned < full_bytes_scanned,
            "filtered scan should read fewer bytes than full scan: filtered={filtered_bytes_scanned}, full={full_bytes_scanned}"
        );
        assert!(
            filtered_bytes_scanned * 2 < full_bytes_scanned,
            "single-row-group scan should be substantially smaller than full scan: filtered={filtered_bytes_scanned}, full={full_bytes_scanned}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn virtual_row_num_scan_reads_only_two_needed_row_groups() -> Result<()> {
        let fixture = LargePayloadFixture::new().await?;

        let full_plan = fixture
            .provider
            .scan(
                &fixture.ctx.state(),
                Some(&vec![fixture.payload_index, fixture.row_num_index]),
                &[],
                None,
            )
            .await?;
        let (_, full_bytes_scanned) =
            collect_plan_and_bytes_scanned(&full_plan, &fixture.ctx).await?;

        let filtered_plan = fixture
            .provider
            .scan(
                &fixture.ctx.state(),
                Some(&vec![fixture.payload_index, fixture.row_num_index]),
                &[datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .gt_eq(datafusion_expr::lit(1_i64))
                    .and(
                        datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                            .lt_eq(datafusion_expr::lit(2_i64)),
                    )],
                None,
            )
            .await?;
        let (batches, filtered_bytes_scanned) =
            collect_plan_and_bytes_scanned(&filtered_plan, &fixture.ctx).await?;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(
            file_access_plan(filtered_plan.as_ref())?,
            &ParquetAccessPlan::new(vec![
                RowGroupAccess::Skip,
                RowGroupAccess::Scan,
                RowGroupAccess::Scan,
                RowGroupAccess::Skip,
            ])
        );
        assert!(
            filtered_bytes_scanned < full_bytes_scanned,
            "two-row-group scan should read fewer bytes than full scan: filtered={filtered_bytes_scanned}, full={full_bytes_scanned}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn planning_uses_partial_file_access_for_cursor_page() -> Result<()> {
        let tempdir = TempDir::new()?;
        write_test_dataset(tempdir.path())?;
        let ctx = SessionContext::new();
        let provider = Arc::new(
            VirtualRowNumProvider::try_new(
                &ctx.state(),
                ListingTableConfig::new(ListingTableUrl::parse(
                    tempdir.path().to_string_lossy().as_ref(),
                )?)
                .with_listing_options(ListingOptions::new(Arc::new(
                    datafusion_datasource_parquet::file_format::ParquetFormat::new(),
                ))),
            )
            .await?,
        );

        let plan = provider
            .scan(
                &ctx.state(),
                None,
                &[datafusion_expr::col(VIRTUAL_ROW_NUM_COLUMN)
                    .gt(datafusion_expr::lit(4_i64))],
                Some(3),
            )
            .await?;
        let wrapper = plan
            .as_any()
            .downcast_ref::<VirtualRowNumExec>()
            .expect("virtual row num wrapper");
        let data_source_exec = wrapper
            .input()
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .expect("datasource exec");
        let file_scan = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .expect("file scan config");
        assert_eq!(file_scan.file_groups.len(), 1);
        let files = file_scan.file_groups[0].files();
        assert_eq!(files.len(), 1);
        assert!(
            files[0]
                .extensions
                .as_ref()
                .and_then(|ext| ext.downcast_ref::<ParquetAccessPlan>())
                .is_some()
        );
        Ok(())
    }

    fn partitioned_file_with_rows(path: &str, rows: usize) -> PartitionedFile {
        let stats = Statistics {
            num_rows: Precision::Exact(rows),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        PartitionedFile::new(path, 0).with_statistics(Arc::new(stats))
    }

    struct LargePayloadFixture {
        _tempdir: TempDir,
        ctx: SessionContext,
        provider: Arc<VirtualRowNumProvider>,
        payload_index: usize,
        row_num_index: usize,
    }

    impl LargePayloadFixture {
        async fn new() -> Result<Self> {
            let tempdir = TempDir::new()?;
            write_large_row_group_dataset(tempdir.path())?;
            let ctx = SessionContext::new();
            let provider = Arc::new(
                VirtualRowNumProvider::try_new(
                    &ctx.state(),
                    ListingTableConfig::new(ListingTableUrl::parse(
                        tempdir.path().to_string_lossy().as_ref(),
                    )?)
                    .with_listing_options(ListingOptions::new(Arc::new(
                        datafusion_datasource_parquet::file_format::ParquetFormat::new(),
                    ))),
                )
                .await?,
            );

            Ok(Self {
                _tempdir: tempdir,
                payload_index: provider.schema().index_of("payload")?,
                row_num_index: provider.row_num_index(),
                ctx,
                provider,
            })
        }
    }

    async fn collect_plan_and_bytes_scanned(
        plan: &Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> Result<(Vec<RecordBatch>, usize)> {
        let batches =
            datafusion::physical_plan::collect(Arc::clone(plan), ctx.task_ctx()).await?;
        let metrics = parquet_metrics(plan.as_ref()).ok_or_else(|| {
            DataFusionError::Internal("Parquet metrics not found".into())
        })?;
        let bytes_scanned = metric_value(&metrics, "bytes_scanned")?;
        Ok((batches, bytes_scanned))
    }

    fn parquet_metrics(plan: &dyn ExecutionPlan) -> Option<MetricsSet> {
        if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
            return data_source_exec.metrics();
        }

        for child in plan.children() {
            if let Some(metrics) = parquet_metrics(child.as_ref()) {
                return Some(metrics);
            }
        }

        None
    }

    fn metric_value(metrics: &MetricsSet, metric_name: &str) -> Result<usize> {
        let Some(metric) = metrics.sum_by_name(metric_name) else {
            return internal_err!(
                "Expected metric '{metric_name}' not found in {metrics:#?}"
            );
        };

        Ok(match metric {
            MetricValue::PruningMetrics {
                pruning_metrics, ..
            } => pruning_metrics.pruned(),
            _ => metric.as_usize(),
        })
    }

    fn file_access_plan(plan: &dyn ExecutionPlan) -> Result<&ParquetAccessPlan> {
        let data_source_exec = find_data_source_exec(plan).ok_or_else(|| {
            DataFusionError::Internal("DataSourceExec not found".into())
        })?;
        let file_scan = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .ok_or_else(|| {
                DataFusionError::Internal("FileScanConfig not found".into())
            })?;

        let files = file_scan
            .file_groups
            .first()
            .map(FileGroup::files)
            .ok_or_else(|| DataFusionError::Internal("No file groups found".into()))?;
        let file = files.first().ok_or_else(|| {
            DataFusionError::Internal("No files found in file group".into())
        })?;

        file.extensions
            .as_ref()
            .and_then(|ext| ext.downcast_ref::<ParquetAccessPlan>())
            .ok_or_else(|| {
                DataFusionError::Internal("ParquetAccessPlan extension not found".into())
            })
    }

    fn find_data_source_exec(plan: &dyn ExecutionPlan) -> Option<&DataSourceExec> {
        if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
            return Some(data_source_exec);
        }

        for child in plan.children() {
            if let Some(data_source_exec) = find_data_source_exec(child.as_ref()) {
                return Some(data_source_exec);
            }
        }

        None
    }

    fn write_test_dataset(path: &Path) -> Result<SchemaRef> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]));
        write_parquet_file(
            &path.join("b.parquet"),
            Arc::clone(&schema),
            &[(20, 1), (21, 2), (22, 3), (23, 4)],
        )?;
        write_parquet_file(
            &path.join("a.parquet"),
            Arc::clone(&schema),
            &[(10, 5), (11, 6), (12, 7), (13, 8)],
        )?;
        write_parquet_file(
            &path.join("c.parquet"),
            Arc::clone(&schema),
            &[(30, 9), (31, 10), (32, 11), (33, 12)],
        )?;
        Ok(schema)
    }

    fn write_parquet_file(
        path: &Path,
        schema: SchemaRef,
        rows: &[(i32, i32)],
    ) -> Result<()> {
        let ids = rows.iter().map(|(id, _)| *id).collect::<Vec<_>>();
        let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
            &RecordBatchOptions::new(),
        )?;
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn write_large_row_group_dataset(path: &Path) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Binary, false),
        ]));
        let ids = Int32Array::from(vec![0, 1, 2, 3]);
        let payloads = BinaryArray::from_iter_values(
            (0_u8..4).map(|seed| large_payload(seed, 256 * 1024)),
        );
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(ids) as ArrayRef, Arc::new(payloads) as ArrayRef],
        )?;
        let props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_row_count(Some(1))
            .build();
        let file = File::create(path.join("payload.parquet"))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
    }

    fn large_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|offset| seed.wrapping_add((offset % 251) as u8))
            .collect()
    }

    #[derive(Debug)]
    struct TestBatchesExec {
        batches: Vec<RecordBatch>,
        cache: Arc<PlanProperties>,
    }

    impl TestBatchesExec {
        fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
            Self {
                batches,
                cache: Arc::new(PlanProperties::new(
                    EquivalenceProperties::new(schema),
                    Partitioning::UnknownPartitioning(1),
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                )),
            }
        }
    }

    impl DisplayAs for TestBatchesExec {
        fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "TestBatchesExec")
        }
    }

    impl ExecutionPlan for TestBatchesExec {
        fn name(&self) -> &'static str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if children.is_empty() {
                Ok(self)
            } else {
                internal_err!("TestBatchesExec does not accept children")
            }
        }

        fn execute(
            &self,
            partition: usize,
            _context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if partition != 0 {
                return exec_err!("invalid partition {partition}");
            }
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                self.schema(),
                futures::stream::iter(self.batches.clone().into_iter().map(Ok)),
            )))
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(
                &dyn datafusion_physical_plan::PhysicalExpr,
            )
                -> Result<datafusion_common::tree_node::TreeNodeRecursion>,
        ) -> Result<datafusion_common::tree_node::TreeNodeRecursion> {
            Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
        }
    }
}
