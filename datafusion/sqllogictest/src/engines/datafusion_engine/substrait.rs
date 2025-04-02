use std::path::{PathBuf};
use std::process::Command;
use std::env;
use bytes::Bytes;
use crate::DFSqlLogicTestError;
use super::Result;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use log::info;
use duckdb::{Config, Connection};

/// Returns the package root, assumed to be the current working directory.
fn get_package_root() -> Result<PathBuf> {
    env::current_dir().map_err(
        |e| DFSqlLogicTestError::Other(format!("Failed to get current directory: {}", e))
    )
}

/// Initializes the Isthmus CLI:
/// - Checks if the `substrait-java` submodule is initialized; if not, runs
///   `git submodule update --init --recursive`.
/// - Checks if the CLI executable exists at `substrait-java/isthmus-cli/build/graal/isthmus`.
///   If not, runs `<package_root>/substrait-java/gradlew nativeImage` to build it.
pub fn init_isthmus() -> Result<()> {
    let package_root = get_package_root()?;
    let substrait_java = package_root.join("substrait-java");

    // Check if the submodule folder exists.
    if !substrait_java.exists() {
        // Initialize the git submodule.
        let status = Command::new("git")
            .arg("submodule")
            .arg("update")
            .arg("--init")
            .arg("--recursive")
            .current_dir(&package_root)
            .status().map_err(
                |e| DFSqlLogicTestError::Other(format!("Failed to run git submodule: {}", e))
            )?;
        if !status.success() {
            return Err(DFSqlLogicTestError::Other("Failed to initialize git submodule".to_string()));
        }
    }

    // Construct the path to the Isthmus CLI executable.
    let isthmus_exe = substrait_java
        .join("isthmus-cli")
        .join("build")
        .join("graal")
        .join("isthmus");

    // If the executable exists, we're done.
    if isthmus_exe.exists() {
        return Ok(());
    } else {
        // Build the Isthmus CLI using Gradle.
        let gradlew = substrait_java.join("gradlew");
        let status = Command::new(gradlew)
            .arg("nativeImage")
            .current_dir(&substrait_java)
            .status().map_err(
                |e| DFSqlLogicTestError::Other(format!("Failed to run gradlew nativeImage: {}", e))
            )?;
        if !status.success() {
            return Err(DFSqlLogicTestError::Other("Failed to build Isthmus CLI".to_string()));
        }
    }

    Ok(())
}

fn duckdb_config() -> duckdb::Result<Config> {
    Config::default()
    .access_mode(duckdb::AccessMode::ReadWrite)?
    .enable_external_access(true)?
    .enable_autoload_extension(true)?
    .allow_unsigned_extensions()
}

/// Converts the provided SQL string to a Substrait plan by running the Isthmus CLI.
/// It ensures that the CLI is built and then executes it with the provided SQL,
/// returning the binary protobuf output as a Vec<u8>.
pub(crate) fn to_substrait(preparation_statements: &[String], sql: &str) -> Result<Plan> {

    let config = duckdb_config().map_err(
        |e| DFSqlLogicTestError::Other(format!("Failed to create DuckDB config: {}", e))
    )?;

    let conn = Connection::open_in_memory_with_flags(config).map_err(
        |e| {
            info!("DuckDB ERROR: {}", e);
            DFSqlLogicTestError::Other(format!("Failed to open DuckDB connection: {}", e))
        }
    )?;
    info!("Opened DuckDB connection");
    conn.execute("INSTALL substrait FROM community", []).map_err(
        |e| {
            info!("Extension ERROR: {}", e);
            DFSqlLogicTestError::Other(format!("Failed to install substrait extension: {}", e))
        }
    )?;
    info!("Installed substrait extension");
    if !preparation_statements.is_empty() {
        let preparation_batch = format!("{};", preparation_statements.join(";"));
        conn.execute_batch(preparation_batch.as_str()).map_err(
            |e| DFSqlLogicTestError::Other(format!("Failed to execute preparation statements: {}", e))
        )?;
    }
    info!("Executed preparation statements \"{}\"", preparation_statements.join(";"));
    conn.execute("LOAD substrait", []).map_err(
        |e| {
            info!("Extension ERROR: {}", e);
            DFSqlLogicTestError::Other(format!("Failed to load substrait extension: {}", e))
        }
    )?;
    info!("Loaded substrait extension");
    let substrait_json_call = format!("CALL get_substrait_json('{}')", sql);
    let substrait_json: String = conn.query_row(substrait_json_call.as_str(), [], |r| r.get(0)).map_err(
        |e| {
            info!("Substrait ERROR: {}", e);
            DFSqlLogicTestError::Other(format!("Failed to get Substrait JSON: {}", e))
        }
    )?;
    info!("Substrait JSON: {}", substrait_json);

    let substrait_call = format!("CALL get_substrait('{}')", sql);
    let substrait_proto: Vec<u8> = conn.query_row(substrait_call.as_str(), [], |r| r.get(0)).map_err(
        |e| {
            info!("Substrait ERROR: {}", e);
            DFSqlLogicTestError::Other(format!("Failed to get Substrait Proto: {}", e))
        }
    )?;

    Plan::decode(Bytes::from(substrait_proto)).map_err(
        |e| DFSqlLogicTestError::Other(format!("Failed to decode Substrait plan: {}", e)),
    )

    /*
    let package_root = get_package_root()?;
    let isthmus_exe = package_root
        .join("substrait-java")
        .join("isthmus-cli")
        .join("build")
        .join("graal")
        .join("isthmus");

    // Build and execute the command.
    let mut command = Command::new(isthmus_exe);
    command.arg("--sqlconformancemode=LENIENT").arg("--outputformat=BINARY");
    for statement in preparation_statements {
        command.arg("-c").arg(statement);
    }
    command.arg(sql);

    info!("Executing:\n{:?}", command);

    // Run the Isthmus CLI command
    let output = command
        .output()
        .map_err(|e| DFSqlLogicTestError::Other(format!("Failed to run Isthmus CLI: {}", e)))?;

    if output.status.success() {
        // Convert the binary output to a Substrait plan.
        Plan::decode(Bytes::from(output.stdout)).map_err(
            |e| DFSqlLogicTestError::Other(format!("Failed to decode Substrait plan: {}", e)),
        )
    } else {
        Err(DFSqlLogicTestError::Other(
            format!("Isthmus CLI failed: {}", String::from_utf8_lossy(&output.stderr))
        ))
    }*/

}