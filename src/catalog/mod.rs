/*!
Implements the postgres catalog
*/

use std::collections::HashMap;

use iceberg_rs::{
    catalog::{
        namespace::Namespace, table_builder::TableBuilder, table_identifier::TableIdentifier,
        Catalog,
    },
    error::{IcebergError, Result},
    model::schema::SchemaV2,
    table::Table,
};

use tokio_postgres::{
    tls::{MakeTlsConnect, NoTlsStream},
    Client, Connection, NoTls, Socket,
};

static catalog_table_name: &str = "iceberg_tables";
static catalog_name_column: &str = "catalog_name";
static table_namespace_column: &str = "table_namespace";
static table_name_column: &str = "table_name";
static metadata_location_column: &str = "metadata_location";
static previous_metadata_location_column: &str = "previous_metadata_location";

/// Postgres catalog
pub struct PostgresCatalog {
    client: Client,
}

impl PostgresCatalog {
    /// Synchronously create a PostgresCatalog object that needs to be initialized later
    pub async fn connect(url: &str) -> Result<(Self, Connection<Socket, NoTlsStream>)> {
        let mut database_url = url.to_string();
        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|err| IcebergError::Message(format!("{}", err)))?;
        Ok((PostgresCatalog { client: client }, connection))
    }
}

#[async_trait::async_trait]
impl Catalog for PostgresCatalog {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: Namespace) -> Result<Vec<TableIdentifier>> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Create a table from an identifier and a schema
    async fn create_table(&self, identifier: TableIdentifier, schema: SchemaV2) -> Result<Table> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Check if a table exists
    async fn table_exists(&self, identifier: TableIdentifier) -> bool {
        false
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: TableIdentifier) -> Result<()> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Load a table.
    async fn load_table(&self, identifier: TableIdentifier) -> Result<Table> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, identifier: TableIdentifier) -> Result<()> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        &self,
        identifier: TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Instantiate a builder to either create a table or start a create/replace transaction.
    async fn build_table(
        &self,
        identifier: TableIdentifier,
        schema: SchemaV2,
    ) -> Result<TableBuilder> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(
        &mut self,
        name: String,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
}
