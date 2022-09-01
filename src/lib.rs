#![deny(missing_docs)]
/*!
# Iceberg catalog postgres

This liberary implements an iceberg catalog on top of postgres.
*/
pub mod catalog;

#[cfg(test)]
mod tests {

    use super::*;

    #[tokio::test]
    async fn it_works() {
        let (pg, connection) = catalog::PostgresCatalog::connect(
            "postgres://postgres:postgres@localhost:5432/iceberg_catalog",
        )
        .await
        .unwrap();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        assert_eq!(4, 4);
    }
}
