#![deny(missing_docs)]
/*!
# Iceberg catalog postgres

This liberary implements an iceberg catalog on top of postgres.
*/
pub mod catalog;

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use iceberg_rs::catalog::{namespace::Namespace, Catalog};
    use object_store::local::LocalFileSystem;

    use super::*;

    #[tokio::test]
    async fn test_initialization() {
        let object_store =
            Arc::new(LocalFileSystem::new_with_prefix("~/workspace/iceberg").unwrap());
        let (mut pg, connection) = catalog::PostgresCatalog::connect(
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
        pg.initialize(&"test_catalog", HashMap::new())
            .await
            .unwrap();
        assert_eq!(4, 4);
    }

    #[tokio::test]
    async fn test_list_tables() {
        let object_store =
            Arc::new(LocalFileSystem::new_with_prefix("~/workspace/iceberg").unwrap());
        let (mut pg, connection) = catalog::PostgresCatalog::connect(
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
        pg.initialize(&"test_catalog", HashMap::new())
            .await
            .unwrap();
        pg.list_tables(Namespace::try_new(&vec!["test".to_string()]).unwrap())
            .await
            .unwrap();
        assert_eq!(4, 4);
    }
}
