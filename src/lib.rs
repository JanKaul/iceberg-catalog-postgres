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
    use iceberg_rs::object_store::memory::InMemory;

    use super::*;

    #[tokio::test]
    async fn test_initialization() {
        let object_store = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            object_store,
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
        let object_store = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            object_store,
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
    async fn test_register_table() {
        let object_store = Arc::new(InMemory::new());
        let (catalog, connection) = catalog::PostgresCatalog::connect(
            "test_catalog",
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
            object_store,
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
        let _ = Arc::clone(&catalog)
            .register_table(&identifier, "/data.db/test/table1/<1>-1.metadata.json")
            .await
            .unwrap();
        let _ = Arc::clone(&catalog)
            .update_table(
                &identifier,
                "/data.db/test/table1/<2>-2.metadata.json",
                "/data.db/test/table1/<1>-1.metadata.json",
            )
            .await
            .unwrap();
        let _ = catalog.drop_table(&identifier).await.unwrap();
        assert_eq!(4, 4);
    }
}
