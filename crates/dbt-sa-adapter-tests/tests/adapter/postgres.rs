//! Postgres adapter method and feature tests (see the `adapter` module).
//!
//! Recording runs each test in a schema named `<test_name>___<timestamp>___`,
//! 22 characters longer than the test name, and Postgres truncates an
//! identifier to 63 bytes. A longer name records against a schema whose name
//! no longer matches the one in the SQL, so keep test names here to 41
//! characters.

use crate::common::FS_RUNNER;

use dbt_adapter_core::AdapterType;
use dbt_common::{FsResult, current_function_name};
use dbt_test_utils::adapter_record_replay::run_fs_record_replay_task_seq;

async fn run_fs_record_replay_postgres(test_name: &str, project_path: &str) -> FsResult<()> {
    run_fs_record_replay_task_seq(
        env!("CARGO_MANIFEST_DIR"),
        &FS_RUNNER,
        test_name,
        project_path,
        AdapterType::Postgres.as_ref(),
        None,
        &["compile"],
        None,
        &[],
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_add_query() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/add_query").await
}

#[dbt_runtime::test]
async fn adapters_postgres_check_schema_exists() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/check_schema_exists",
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_create_schema() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/create_schema")
        .await
}

#[dbt_runtime::test]
async fn adapters_postgres_drop_schema() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/drop_schema").await
}

#[dbt_runtime::test]
async fn adapters_postgres_get_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/get_relation").await
}

/// The schema is listed once (`pg_tables`, `pg_views` and `pg_matviews`, plus
/// the `pg_depend` links), and both `get_relation` calls are answered from the
/// relation cache.
#[dbt_runtime::test]
async fn adapters_postgres_get_relation_from_cache() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/get_relation_from_cache",
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_relation_no_hydration() -> FsResult<()> {
    run_fs_record_replay_task_seq(
        env!("CARGO_MANIFEST_DIR"),
        &FS_RUNNER,
        current_function_name!(),
        "tests/data/adapter/get_relation_no_hydration",
        AdapterType::Postgres.as_ref(),
        None,
        &["run-operation check_get_relation"],
        None,
        &[],
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_drop_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/drop_relation")
        .await
}

#[dbt_runtime::test]
async fn adapters_postgres_rename_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/rename_relation",
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_truncate_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/truncate_relation",
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_load_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/load_relation")
        .await
}

#[dbt_runtime::test]
async fn adapters_postgres_list_schemas() -> FsResult<()> {
    run_fs_record_replay_postgres(current_function_name!(), "tests/data/adapter/list_schemas").await
}

#[dbt_runtime::test]
async fn adapters_postgres_get_columns_in_relation() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/get_columns_in_relation",
    )
    .await
}

#[dbt_runtime::test]
async fn adapters_postgres_get_missing_columns() -> FsResult<()> {
    run_fs_record_replay_postgres(
        current_function_name!(),
        "tests/data/adapter/get_missing_columns",
    )
    .await
}

/// Postgres records no table last-modified time, so a source with a
/// freshness config and no `loaded_at_field` is reported as not supported
/// (as dbt-postgres 1.x does) instead of panicking.
#[dbt_runtime::test]
async fn adapters_postgres_metadata_freshness() -> FsResult<()> {
    run_fs_record_replay_task_seq(
        env!("CARGO_MANIFEST_DIR"),
        &FS_RUNNER,
        current_function_name!(),
        "tests/data/adapter/metadata_source_freshness",
        AdapterType::Postgres.as_ref(),
        None,
        // run_0 creates the relations the test reads.
        &["run-operation seed_postgres", "source freshness"],
        None,
        &[],
    )
    .await
}
