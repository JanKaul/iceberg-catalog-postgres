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
    use iceberg_rs::catalog::Catalog;
    use iceberg_rs::model::schema::{AllType, PrimitiveType, SchemaV2, Struct, StructField};
    use iceberg_rs::object_store::memory::InMemory;
    use iceberg_rs::object_store::ObjectStore;

    use super::*;

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
        let mut table = Arc::clone(&catalog)
            .create_table(identifier.clone(), schema)
            .await
            .unwrap();
        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .unwrap();
        assert_eq!(exists, true);

        let metadata_location = table.metadata_location().to_string();

        let transaction = table.new_transaction();
        transaction.commit().await.unwrap();

        let new_metadata_location = table.metadata_location().to_string();

        assert_ne!(metadata_location, new_metadata_location);

        let _ = catalog.drop_table(&identifier).await.unwrap();
        let exists = Arc::clone(&catalog)
            .table_exists(&identifier)
            .await
            .unwrap();
        assert_eq!(exists, false);
    }
}
