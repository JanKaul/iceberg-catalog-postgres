/*!
Implements the postgres catalog
*/

use std::{collections::HashMap, sync::Arc};

use iceberg_rs::{
    catalog::{
        namespace::Namespace, table_builder::TableBuilder, table_identifier::TableIdentifier,
        Catalog,
    },
    error::{IcebergError, Result},
    model::{schema::SchemaV2, table::TableMetadataV2},
    object_store::path::Path,
    table::Table,
};

use iceberg_rs::object_store::ObjectStore;
use tokio_postgres::{tls::NoTlsStream, Client, Connection, NoTls, Socket};

static CATALOG_TABLE_NAME: &str = "iceberg_tables";
static CATALOG_NAME_COLUMN: &str = "catalog_name";
static TABLE_NAMESPACE_COLUMN: &str = "table_namespace";
static TABLE_NAME_COLUMN: &str = "table_name";
static METADATA_LOCATION_COLUMN: &str = "metadata_location";
static PREVIOUS_METADATA_LOCATION_COLUMN: &str = "previous_metadata_location";

/// Postgres catalog
pub struct PostgresCatalog {
    name: String,
    client: Client,
    object_store: Arc<dyn ObjectStore>,
}

impl PostgresCatalog {
    /// Synchronously create a PostgresCatalog object that needs to be initialized later
    pub async fn connect(
        name: &str,
        url: &str,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<(Self, Connection<Socket, NoTlsStream>)> {
        let (client, connection) = tokio_postgres::connect(&url, NoTls)
            .await
            .map_err(|err| IcebergError::Message(format!("{}", err)))?;
        Ok((
            PostgresCatalog {
                client: client,
                name: name.to_string(),
                object_store: object_store,
            },
            connection,
        ))
    }
}

#[async_trait::async_trait]
impl Catalog for PostgresCatalog {
    /// Lists all tables in the given namespace.
    async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableIdentifier>> {
        let rows = self
            .client
            .query(
                &("SELECT ".to_string()
                    + CATALOG_NAME_COLUMN
                    + ", "
                    + TABLE_NAMESPACE_COLUMN
                    + ", "
                    + TABLE_NAME_COLUMN
                    + ", "
                    + METADATA_LOCATION_COLUMN
                    + ", "
                    + PREVIOUS_METADATA_LOCATION_COLUMN
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE ("
                    + CATALOG_NAME_COLUMN
                    + " = '"
                    + &self.name
                    + "' AND "
                    + TABLE_NAMESPACE_COLUMN
                    + "= '"
                    + &format!("{}", namespace)
                    + "');"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        rows.into_iter()
            .map(|x| {
                let namespace: &str = x
                    .try_get(&TABLE_NAMESPACE_COLUMN)
                    .map_err(|err| IcebergError::Message(err.to_string()))?;
                let name: &str = x
                    .try_get(TABLE_NAME_COLUMN)
                    .map_err(|err| IcebergError::Message(err.to_string()))?;
                Ok(TableIdentifier::parse(&format!("{}.{}", namespace, name))?)
            })
            .collect::<std::result::Result<Vec<_>, IcebergError>>()
    }
    /// Create a table from an identifier and a schema
    async fn create_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        schema: &SchemaV2,
    ) -> Result<Table> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Check if a table exists
    async fn table_exists(&self, identifier: &TableIdentifier) -> bool {
        false
    }
    /// Drop a table and delete all data and metadata files.
    async fn drop_table(&self, identifier: &TableIdentifier) -> Result<()> {
        let namespace = identifier.namespace();
        let table_name = identifier.name();
        let n_rows = self
            .client
            .execute(
                &("DELETE FROM ".to_string()
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME_COLUMN
                    + " = '"
                    + &self.name
                    + "' AND "
                    + TABLE_NAMESPACE_COLUMN
                    + " = '"
                    + &format!("{}", namespace)
                    + "' AND "
                    + TABLE_NAME_COLUMN
                    + " = '"
                    + table_name
                    + "';"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        if n_rows == 1 {
            // TODO: Delete associated files
            Ok(())
        } else if n_rows == 0 {
            Err(IcebergError::Message("Table already exists".to_string()))
        } else {
            Err(IcebergError::Message(
                "More than one table was added to the catalog.".to_string(),
            ))
        }
    }
    /// Load a table.
    async fn load_table(self: Arc<Self>, identifier: &TableIdentifier) -> Result<Table> {
        let namespace = identifier.namespace();
        let table_name = identifier.name();
        let rows = self
            .client
            .query(
                &("SELECT ".to_string()
                    + METADATA_LOCATION_COLUMN
                    + " FROM "
                    + CATALOG_TABLE_NAME
                    + " WHERE "
                    + CATALOG_NAME_COLUMN
                    + " = '"
                    + &self.name
                    + "' AND "
                    + TABLE_NAMESPACE_COLUMN
                    + " = '"
                    + &format!("{}", namespace)
                    + "' AND "
                    + TABLE_NAME_COLUMN
                    + " = '"
                    + table_name
                    + "';"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        if rows.len() == 1 {
            let path: Path = rows[0]
                .try_get::<_, &str>(METADATA_LOCATION_COLUMN)
                .map_err(|err| IcebergError::Message(err.to_string()))?
                .into();
            let bytes = &self
                .object_store
                .get(&path)
                .await
                .map_err(|err| IcebergError::Message(err.to_string()))?
                .bytes()
                .await
                .map_err(|err| IcebergError::Message(err.to_string()))?;
            let metadata: TableMetadataV2 = serde_json::from_str(
                std::str::from_utf8(bytes).map_err(|err| IcebergError::Message(err.to_string()))?,
            )
            .map_err(|err| IcebergError::Message(err.to_string()))?;
            let catalog: Arc<dyn Catalog> = self;
            Ok(Table::new(Arc::clone(&catalog), metadata))
        } else if rows.len() == 0 {
            Err(IcebergError::Message("Not implemented.".to_string()))
        } else {
            Err(IcebergError::Message("Not implemented.".to_string()))
        }
    }
    /// Invalidate cached table metadata from current catalog.
    async fn invalidate_table(&self, identifier: &TableIdentifier) -> Result<()> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Register a table with the catalog if it doesn't exist.
    async fn register_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        metadata_file_location: &str,
    ) -> Result<Table> {
        let namespace = identifier.namespace();
        let table_name = identifier.name();
        let n_rows = self
            .client
            .execute(
                &("INSERT INTO ".to_string()
                    + CATALOG_TABLE_NAME
                    + " ("
                    + CATALOG_NAME_COLUMN
                    + ", "
                    + TABLE_NAMESPACE_COLUMN
                    + ", "
                    + TABLE_NAME_COLUMN
                    + ", "
                    + METADATA_LOCATION_COLUMN
                    + ", "
                    + PREVIOUS_METADATA_LOCATION_COLUMN
                    + ") VALUES ('"
                    + &self.name
                    + "', '"
                    + &format!("{}", namespace)
                    + "', '"
                    + table_name
                    + "', '"
                    + metadata_file_location
                    + "', NULL ) ON CONFLICT ("
                    + CATALOG_NAME_COLUMN
                    + ", "
                    + TABLE_NAMESPACE_COLUMN
                    + ", "
                    + TABLE_NAME_COLUMN
                    + ") DO NOTHING;"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        if n_rows == 1 {
            self.load_table(identifier).await
        } else if n_rows == 0 {
            Err(IcebergError::Message("Table already exists".to_string()))
        } else {
            Err(IcebergError::Message(
                "More than one table was added to the catalog.".to_string(),
            ))
        }
    }
    /// Update a table by atomically changing the pointer to the metadata file
    async fn update_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        metadata_file_location: &str,
        previous_metadata_file_location: &str,
    ) -> Result<Table> {
        let namespace = identifier.namespace();
        let table_name = identifier.name();
        let n_rows = self
            .client
            .execute(
                &("UPDATE ".to_string()
                    + CATALOG_TABLE_NAME
                    + " SET "
                    + METADATA_LOCATION_COLUMN
                    + " = '"
                    + metadata_file_location
                    + "', "
                    + PREVIOUS_METADATA_LOCATION_COLUMN
                    + " = '"
                    + previous_metadata_file_location
                    + "' WHERE "
                    + CATALOG_NAME_COLUMN
                    + " = '"
                    + &self.name
                    + "' AND "
                    + TABLE_NAMESPACE_COLUMN
                    + " = '"
                    + &format!("{}", namespace)
                    + "' AND "
                    + TABLE_NAME_COLUMN
                    + " = '"
                    + table_name
                    + "' AND "
                    + METADATA_LOCATION_COLUMN
                    + " = '"
                    + previous_metadata_file_location
                    + "';"),
                &[],
            )
            .await
            .map_err(|err| IcebergError::Message(err.to_string()))?;
        if n_rows == 1 {
            self.load_table(identifier).await
        } else if n_rows == 0 {
            Err(IcebergError::Message("Table already exists".to_string()))
        } else {
            Err(IcebergError::Message(
                "More than one table was added to the catalog.".to_string(),
            ))
        }
    }
    /// Instantiate a builder to either create a table or start a create/replace transaction.
    async fn build_table(
        self: Arc<Self>,
        identifier: &TableIdentifier,
        schema: &SchemaV2,
    ) -> Result<TableBuilder> {
        Err(IcebergError::Message("Not implemented.".to_string()))
    }
    /// Initialize a catalog given a custom name and a map of catalog properties.
    /// A custom Catalog implementation must have a no-arg constructor. A compute engine like Spark
    /// or Flink will first initialize the catalog without any arguments, and then call this method to
    /// complete catalog initialization with properties passed into the engine.
    async fn initialize(self: Arc<Self>, properties: &HashMap<String, String>) -> Result<()> {
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
    fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }
}
