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

use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};

static CATALOG_TABLE_NAME: &str = "iceberg_tables";
static CATALOG_NAME_COLUMN: &str = "catalog_name";
static TABLE_NAMESPACE_COLUMN: &str = "table_namespace";
static TABLE_NAME_COLUMN: &str = "table_name";
static METADATA_LOCATION_COLUMN: &str = "metadata_location";
static PREVIOUS_METADATA_LOCATION_COLUMN: &str = "previous_metadata_location";

/// Postgres catalog
pub struct PostgresCatalog {
    name: Option<String>,
    client: Client,
}

impl PostgresCatalog {
    /// Synchronously create a PostgresCatalog object that needs to be initialized later
    pub async fn connect(url: &str) -> Result<(Self, Connection<Socket, NoTlsStream>)> {
        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|err| IcebergError::Message(format!("{}", err)))?;
        Ok((
            PostgresCatalog {
                client: client,
                name: None,
            },
            connection,
        ))
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
    async fn initialize(&mut self, name: &str, properties: HashMap<String, String>) -> Result<()> {
        self.name = Some(name.to_string());
        self.client
            .execute(
                &("CREATE TABLE IF NOT EXISTS ".to_string()
                    + CATALOG_TABLE_NAME
                    + " ("
                    + CATALOG_NAME_COLUMN
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_NAMESPACE_COLUMN
                    + " VARCHAR(255) NOT NULL,"
                    + TABLE_NAME_COLUMN
                    + " VARCHAR(255) NOT NULL,"
                    + METADATA_LOCATION_COLUMN
                    + " VARCHAR(5500),"
                    + PREVIOUS_METADATA_LOCATION_COLUMN
                    + " VARCHAR(5500),"
                    + "PRIMARY KEY ("
                    + CATALOG_NAME_COLUMN
                    + ", "
                    + TABLE_NAMESPACE_COLUMN
                    + ", "
                    + TABLE_NAME_COLUMN
                    + ")"
                    + ");"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        Ok(())
    }
}
