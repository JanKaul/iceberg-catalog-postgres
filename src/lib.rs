#![deny(missing_docs)]
/*!
# Iceberg catalog postgres

This liberary implements an iceberg catalog on top of postgres.
*/
pub mod catalog;

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use iceberg_rs::catalog::table_identifier::TableIdentifier;
    use iceberg_rs::catalog::{namespace::Namespace, Catalog};
    use iceberg_rs::model::schema::{AllType, PrimitiveType, SchemaV2, Struct, StructField};
    use iceberg_rs::object_store::memory::InMemory;
    use iceberg_rs::object_store::path::Path;
    use iceberg_rs::object_store::ObjectStore;

    use super::*;

    #[tokio::test]
    async fn test_initialization() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let catalog = Arc::new(catalog);
        catalog.initialize(&HashMap::new()).await.unwrap();
        assert_eq!(4, 4);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let catalog = Arc::new(catalog);
        Arc::clone(&catalog)
            .initialize(&HashMap::new())
            .await
            .unwrap();
        catalog
            .list_tables(&Namespace::try_new(&vec!["test".to_string()]).unwrap())
            .await
            .unwrap();
        assert_eq!(4, 4);
    }
    #[tokio::test]
    async fn test_create_update_drop_table() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            Arc::clone(&object_store),
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        let catalog = Arc::new(catalog);
        Arc::clone(&catalog)
            .initialize(&HashMap::new())
            .await
            .unwrap();
        let identifier = TableIdentifier::parse("test.table1").unwrap();
        let schema = SchemaV2 {
            schema_id: 1,
            identifier_field_ids: Some(vec![1, 2]),
            name_mapping: None,
            struct_fields: Struct {
                fields: vec![
                    StructField {
                        id: 1,
                        name: "one".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                    StructField {
                        id: 2,
                        name: "two".to_string(),
                        required: false,
                        field_type: AllType::Primitive(PrimitiveType::String),
                        doc: None,
                    },
                ],
            },
        };
        let table = Arc::clone(&catalog)
            .create_table(identifier.clone(), schema)
            .await
            .unwrap();
        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .unwrap();
        assert_eq!(exists, true);
        let metadata_location = table.metadata_location().to_string();
        let next_metadata_location = "data.db/test/table1/2-2.metadata.json".to_string();
        let from: Path = metadata_location.clone().into();
        let to: Path = next_metadata_location.clone().into();
        object_store.copy(&from, &to).await.unwrap();
        let _ = Arc::clone(&catalog)
            .update_table(&identifier, &next_metadata_location, &metadata_location)
            .await
            .unwrap();
        let _ = catalog.drop_table(&identifier).await.unwrap();
        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .unwrap();
        assert_eq!(exists, false);
    }
}
