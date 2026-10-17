use crate::AdapterEngine;
use crate::adapter::adapter_impl::AdapterImpl;
use crate::connection::{AdapterConnectionFactory, ConnectionGuard};
use crate::errors::{AdapterError, AdapterErrorKind};
use crate::sql_types::make_arrow_field_v2;
use crate::{AdapterResult, errors::AsyncAdapterResult, metadata::*, record_batch::RecordBatchExt};
use arrow_schema::Schema;
use dbt_adbc::QueryCtx;
use dbt_common::cancellation::{Cancellable, CancellationToken};

use arrow_array::{Array, BooleanArray, Decimal128Array, RecordBatch, StringArray};

use crate::metadata::redshift::REDSHIFT_GET_RELATIONS_SQL;
use crate::relation::Relation;
use dbt_adapter_core::{AdapterType, ExecutionPhase};
use dbt_adapter_engine::MapReduce;
use dbt_schemas::dbt_types::RelationType;
use dbt_schemas::schemas::common::ResolvedQuoting;
use dbt_schemas::schemas::{
    legacy_catalog::{CatalogNodeStats, CatalogTable, ColumnMetadata, TableMetadata},
    relations::base::{BaseRelation, RelationPattern},
};
use indexmap::IndexMap;
use minijinja::State;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

pub struct PostgresMetadataAdapter {
    adapter: AdapterImpl,
}

impl PostgresMetadataAdapter {
    pub fn new(engine: Arc<dyn AdapterEngine>) -> Self {
        let adapter = AdapterImpl::new(engine, None);
        Self { adapter }
    }
}

impl MetadataAdapter for PostgresMetadataAdapter {
    fn adapter_type(&self) -> AdapterType {
        self.adapter.adapter_type()
    }

    fn build_schemas_from_stats_sql(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, CatalogTable>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;
        let data_types = stats_sql_result.column_values::<StringArray>("table_type")?;
        let comments = stats_sql_result.column_values::<StringArray>("table_comment")?;
        let table_owners = stats_sql_result.column_values::<StringArray>("table_owner")?;

        let mut result = BTreeMap::<String, CatalogTable>::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);
            let data_type = data_types.value(i);
            let comment = comments.value(i);
            let owner = table_owners.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let entry = result.entry(fully_qualified_name.clone());

            if matches!(entry, Entry::Vacant(_)) {
                let node_metadata = TableMetadata {
                    materialization_type: data_type.to_string(),
                    schema: schema.to_string(),
                    name: table.to_string(),
                    database: Some(catalog.to_string()),
                    comment: match comment {
                        "" => None,
                        _ => Some(comment.to_string()),
                    },
                    owner: Some(owner.to_string()),
                };

                let no_stats = CatalogNodeStats {
                    id: "has_stats".to_string(),
                    label: "Has Stats?".to_string(),
                    value: serde_json::Value::Bool(false),
                    description: Some(
                        "Indicates whether there are statistics for this table".to_string(),
                    ),
                    include: false,
                };

                let node = CatalogTable {
                    metadata: node_metadata,
                    columns: IndexMap::new(),
                    stats: BTreeMap::from([("has_stats".to_string(), no_stats)]),
                    unique_id: None,
                };
                result.insert(fully_qualified_name.clone(), node);
            }
        }
        Ok(result)
    }

    fn build_columns_from_get_columns(
        &self,
        stats_sql_result: Arc<RecordBatch>,
    ) -> AdapterResult<BTreeMap<String, BTreeMap<String, ColumnMetadata>>> {
        if stats_sql_result.num_rows() == 0 {
            return Ok(BTreeMap::new());
        }

        let table_catalogs = stats_sql_result.column_values::<StringArray>("table_database")?;
        let table_schemas = stats_sql_result.column_values::<StringArray>("table_schema")?;
        let table_names = stats_sql_result.column_values::<StringArray>("table_name")?;

        let column_names = stats_sql_result.column_values::<StringArray>("column_name")?;
        let column_indices = stats_sql_result.column_values::<Decimal128Array>("column_index")?;
        let column_types = stats_sql_result.column_values::<StringArray>("column_type")?;
        let column_comments = stats_sql_result.column_values::<StringArray>("column_comment")?;

        let mut columns_by_relation = BTreeMap::new();

        for i in 0..table_catalogs.len() {
            let catalog = table_catalogs.value(i);
            let schema = table_schemas.value(i);
            let table = table_names.value(i);

            let fully_qualified_name = format!("{catalog}.{schema}.{table}").to_lowercase();

            let column_name = column_names.value(i);
            let column_index = column_indices.value(i);
            let column_type = column_types.value(i);
            let column_comment = column_comments.value(i);

            let column = ColumnMetadata {
                name: column_name.to_string(),
                index: column_index,
                data_type: column_type.to_string(),
                comment: match column_comment {
                    "" => None,
                    _ => Some(column_comment.to_string()),
                },
            };

            columns_by_relation
                .entry(fully_qualified_name.clone())
                .or_insert(BTreeMap::new())
                .insert(column_name.to_string(), column);
        }
        Ok(columns_by_relation)
    }

    fn list_relations_schemas_inner(
        &self,
        unique_id: Option<String>,
        phase: Option<ExecutionPhase>,
        relations: &[Arc<dyn BaseRelation>],
        item_span_operation_id: Option<&str>,
        token: CancellationToken,
    ) -> AsyncAdapterResult<'_, HashMap<String, AdapterResult<Arc<Schema>>>> {
        type Acc = HashMap<String, AdapterResult<Arc<Schema>>>;

        let factory = Box::new(AdapterConnectionFactory::new(self.adapter.engine().clone()));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();
        let map_f = move |conn: &mut ConnectionGuard<'_>,
                          relation: &Arc<dyn BaseRelation>|
              -> AdapterResult<Arc<Schema>> {
            let schema = relation.schema_as_str()?;
            let identifier = relation.identifier_as_str()?;

            // PostgreSQL has no DESCRIBE, so read the column set from the system
            // catalogue. `format_type` reassembles the full declared type
            // (length/precision included) the way `psql \d` does, `col_description`
            // recovers any COMMENT, and `attnum > 0 AND NOT attisdropped` skips the
            // system and dropped columns. `attnum` order is the table's column
            // order. The connection is scoped to one database, so no catalog
            // predicate is needed.
            let sql = format!(
                "SELECT
    a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
    NOT a.attnotnull AS is_nullable,
    COALESCE(pg_catalog.col_description(a.attrelid, a.attnum), '') AS remarks
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = '{schema}'
  AND c.relname = '{identifier}'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum"
            );

            let mut ctx = QueryCtx::new_metadata().with_desc("Get table schema");
            if let Some(node_id) = unique_id.clone() {
                ctx = ctx.with_node_id(&node_id);
            }
            if let Some(phase) = phase {
                ctx = ctx.with_phase(phase.as_str());
            }
            let (_, table) = adapter.query(&ctx, conn, &sql, None, token_clone.clone())?;
            let batch = table.original_record_batch();

            let column_names = batch.column_values::<StringArray>("column_name")?;
            let data_types = batch.column_values::<StringArray>("data_type")?;
            let is_nullables = batch.column_values::<BooleanArray>("is_nullable")?;
            let comments = batch.column_values::<StringArray>("remarks")?;

            let mut fields = Vec::with_capacity(batch.num_rows());
            for i in 0..batch.num_rows() {
                let name = column_names.value(i);
                let data_type = data_types.value(i);
                let is_nullable = is_nullables.value(i);
                let comment = match comments.value(i) {
                    "" => None,
                    c => Some(c.to_string()),
                };

                let field = make_arrow_field_v2(
                    adapter.engine().type_ops().as_ref(),
                    String::from(name),
                    data_type,
                    Some(is_nullable),
                    comment,
                )?;
                fields.push(field);
            }

            if fields.is_empty() {
                return Err(AdapterError::new(
                    AdapterErrorKind::UnexpectedResult,
                    format!("no columns found in pg_catalog for {schema}.{identifier}"),
                ));
            }

            Ok(Arc::new(Schema::new(fields)))
        };

        let reduce_f = |acc: &mut Acc,
                        relation: Arc<dyn BaseRelation>,
                        schema: AdapterResult<Arc<Schema>>|
         -> Result<(), Cancellable<AdapterError>> {
            acc.insert(relation.semantic_fqn(), schema);
            Ok(())
        };

        run_schema_cache_map_reduce(
            factory,
            relations.to_vec(),
            item_span_operation_id,
            map_f,
            reduce_f,
            None,
            token,
        )
    }

    fn list_relations_schemas_by_patterns_inner(
        &self,
        _patterns: &[RelationPattern],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, Vec<(String, AdapterResult<RelationSchemaPair>)>> {
        todo!("PostgresAdapter::list_relations_schemas_by_patterns")
    }

    /// Metadata-based freshness is not supported on Postgres: the catalog
    /// records no last-modified time for a table (Redshift's implementation
    /// reads its own `stl_insert` system table, which Postgres lacks), and
    /// dbt-postgres never declared the `TableLastModifiedMetadata`
    /// capability. Return `NotSupported` rather than panicking, so `dbt
    /// source freshness` reports a clean error and `dbt build` falls back
    /// for sources without a `loaded_at_field`.
    fn freshness_inner(
        &self,
        _relations: &[Arc<dyn BaseRelation>],
        _token: CancellationToken,
    ) -> AsyncAdapterResult<'_, BTreeMap<String, MetadataFreshness>> {
        Box::pin(async {
            Err(Cancellable::Error(AdapterError::new(
                AdapterErrorKind::NotSupported,
                "the postgres adapter does not support metadata-based freshness checks; \
                 set a `loaded_at_field` (or `loaded_at_query`) on the source",
            )))
        })
    }

    fn create_schemas_if_not_exists(
        &self,
        state: &State<'_, '_>,
        catalog_schemas: Vec<(String, String, String)>,
    ) -> AdapterResult<Vec<(String, String, String, AdapterResult<()>)>> {
        create_schemas_if_not_exists(&self.adapter, self, state, catalog_schemas)
    }

    /// Lists each schema's relations on its own connection, as Redshift does,
    /// to hydrate the relation cache.
    fn list_relations_in_parallel_inner(
        &self,
        db_schemas: &[CatalogAndSchema],
        token: CancellationToken,
        report_progress: bool,
    ) -> AsyncAdapterResult<'_, BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>> {
        type Acc = BTreeMap<CatalogAndSchema, AdapterResult<RelationVec>>;
        let factory = Box::new(AdapterConnectionFactory::new(self.adapter.engine().clone()));

        let adapter = self.adapter.clone();
        let token_clone = token.clone();

        let map_f = move |conn: &mut ConnectionGuard<'_>,
                          db_schema: &CatalogAndSchema|
              -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
            let query_ctx = QueryCtx::default().with_desc("list_relations_in_parallel");
            with_relation_list_item_span(
                report_progress.then_some(RELATION_CACHE_OP_ID),
                &db_schema.to_string(),
                || adapter.list_relations(None, &query_ctx, conn, db_schema, token_clone.clone()),
            )
        };

        let reduce_f = move |acc: &mut Acc,
                             db_schema: CatalogAndSchema,
                             relations: AdapterResult<Vec<Arc<dyn BaseRelation>>>|
              -> Result<(), Cancellable<AdapterError>> {
            acc.insert(db_schema, relations);
            Ok(())
        };

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        map_reduce.run(Arc::new(db_schemas.to_vec()), token)
    }

    /// The `pg_depend` edges between relations in `db_schemas`, so that when a
    /// rebuild drops a relation with `cascade` the cache also evicts the views
    /// and materialized views Postgres dropped with it. Without them, a
    /// hydrated cache keeps a materialized view its parent's rebuild removed.
    /// One query per database, as Redshift does.
    fn fetch_relation_dependency_links_inner<'a>(
        &'a self,
        db_schemas: &'a [CatalogAndSchema],
        token: CancellationToken,
    ) -> AsyncAdapterResult<'a, Vec<ParentChildPair>> {
        let mut jobs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for s in db_schemas {
            jobs.entry(s.resolved_catalog.clone())
                .or_default()
                .insert(s.resolved_schema.to_lowercase());
        }
        let jobs: Vec<(String, BTreeSet<String>)> = jobs.into_iter().collect();

        let factory = Box::new(AdapterConnectionFactory::new(self.adapter.engine().clone()));
        let adapter = self.adapter.clone();
        let token_clone = token.clone();

        let map_f = move |conn: &mut ConnectionGuard<'_>,
                          job: &(String, BTreeSet<String>)|
              -> AdapterResult<Vec<ParentChildPair>> {
            let (catalog, schemas) = job;
            let ctx = QueryCtx::default().with_desc("postgres__get_relations");
            let engine = adapter.engine();
            let batch = engine.execute(
                None,
                conn.as_mut(),
                &ctx,
                REDSHIFT_GET_RELATIONS_SQL,
                token_clone.clone(),
            )?;
            parse_relation_dependencies(
                &batch,
                catalog,
                |schema| schemas.contains(&schema.to_lowercase()),
                engine.quoting(),
            )
        };

        let reduce_f = move |acc: &mut Vec<ParentChildPair>,
                             _job: (String, BTreeSet<String>),
                             res: AdapterResult<Vec<ParentChildPair>>|
              -> Result<(), Cancellable<AdapterError>> {
            match res {
                Ok(links) => acc.extend(links),
                Err(e) => {
                    // Best-effort, as on Redshift: a cascade just won't evict
                    // dependents until the next hydration.
                    tracing::warn!("fetch_relation_dependency_links: pg_depend query failed: {e}");
                }
            }
            Ok(())
        };

        let map_reduce = MapReduce::new(factory, Box::new(map_f), Box::new(reduce_f), None);
        Box::pin(async move { map_reduce.run(Arc::new(jobs), token).await })
    }
}

/// Per-schema variant for the cache-miss path: the `pg_depend` edges whose
/// referenced relation is in `db_schema`.
pub fn list_relation_dependencies(
    engine: &dyn AdapterEngine,
    ctx: &QueryCtx,
    conn: &mut ConnectionGuard<'_>,
    db_schema: &CatalogAndSchema,
    token: CancellationToken,
) -> AdapterResult<Vec<ParentChildPair>> {
    let batch = engine.execute(None, conn.as_mut(), ctx, REDSHIFT_GET_RELATIONS_SQL, token)?;
    let want = db_schema.resolved_schema.to_lowercase();
    parse_relation_dependencies(
        &batch,
        &db_schema.resolved_catalog,
        |schema| schema.to_lowercase() == want,
        engine.quoting(),
    )
}

/// (referenced, dependent) pairs from [`REDSHIFT_GET_RELATIONS_SQL`], kept
/// where the referenced relation's schema matches.
fn parse_relation_dependencies(
    batch: &RecordBatch,
    catalog: &str,
    schema_matches: impl Fn(&str) -> bool,
    quoting: ResolvedQuoting,
) -> AdapterResult<Vec<ParentChildPair>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let dep_schemas = batch.column_values::<StringArray>("dependent_schema")?;
    let dep_names = batch.column_values::<StringArray>("dependent_name")?;
    let ref_schemas = batch.column_values::<StringArray>("referenced_schema")?;
    let ref_names = batch.column_values::<StringArray>("referenced_name")?;

    let mut links = Vec::new();
    for i in 0..batch.num_rows() {
        let ref_schema = ref_schemas.value(i);
        if !schema_matches(ref_schema) {
            continue;
        }
        // The relation type does not matter for eviction, which goes by name.
        links.push((
            build_relation(
                catalog,
                ref_schema,
                ref_names.value(i),
                RelationType::View,
                quoting,
            )?,
            build_relation(
                catalog,
                dep_schemas.value(i),
                dep_names.value(i),
                RelationType::View,
                quoting,
            )?,
        ));
    }
    Ok(links)
}

fn build_relation(
    catalog: &str,
    schema: &str,
    name: &str,
    relation_type: RelationType,
    quoting: ResolvedQuoting,
) -> AdapterResult<Arc<dyn BaseRelation>> {
    let relation = Relation::new(
        AdapterType::Postgres,
        catalog.to_string(),
        schema.to_string(),
        name.to_string(),
    )
    .with_relation_type(relation_type)
    .with_quoting(quoting)
    .validate()?;
    Ok(Arc::new(relation) as Arc<dyn BaseRelation>)
}

/// The tables, views and materialized views in one schema.
///
/// Ports dbt-postgres's `postgres__list_relations_without_caching`
/// (https://github.com/dbt-labs/dbt-adapters/blob/802d40f0f0eb663c17e5be08d1e3fb2e55b9ee34/dbt-postgres/src/dbt/include/postgres/macros/adapters.sql),
/// except that the schema is matched exactly (case-insensitively) rather than
/// with `ilike`, which reads a `_` in the schema name as a wildcard and would
/// list another schema's relations under this one.
pub fn list_relations(
    engine: &dyn AdapterEngine,
    ctx: &QueryCtx,
    conn: &mut ConnectionGuard<'_>,
    db_schema: &CatalogAndSchema,
    token: CancellationToken,
) -> AdapterResult<Vec<Arc<dyn BaseRelation>>> {
    let schema = db_schema.resolved_schema.replace('\'', "''");
    let sql = format!(
        "select tablename as name, schemaname as schema, 'table' as type
from pg_catalog.pg_tables where lower(schemaname) = lower('{schema}')
union all
select viewname, schemaname, 'view'
from pg_catalog.pg_views where lower(schemaname) = lower('{schema}')
union all
select matviewname, schemaname, 'materialized_view'
from pg_catalog.pg_matviews where lower(schemaname) = lower('{schema}')"
    );
    let batch = engine.execute(None, conn.as_mut(), ctx, &sql, token)?;
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let names = batch.column_values::<StringArray>("name")?;
    let schemas = batch.column_values::<StringArray>("schema")?;
    let types = batch.column_values::<StringArray>("type")?;
    (0..batch.num_rows())
        .map(|i| {
            build_relation(
                &db_schema.resolved_catalog,
                schemas.value(i),
                names.value(i),
                RelationType::from(types.value(i)),
                engine.quoting(),
            )
        })
        .collect()
}
