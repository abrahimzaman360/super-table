use catalog::catalog_service_server::{CatalogService, CatalogServiceServer};
use catalog::{CreateTableRequest, CreateTableResponse, GetTableRequest, GetTableResponse};
use tonic::{Request, Response, Status, transport::Server};

pub mod catalog {
    tonic::include_proto!("catalog");
}

#[derive(Debug, Default)]
pub struct MyCatalog {}

#[tonic::async_trait]
impl CatalogService for MyCatalog {
    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();
        println!("Got a request: {:?}", req);

        // For now, return a dummy response
        let reply = GetTableResponse {
            table_identifier: req.table_identifier,
            metadata_location: "s3://bucket/metadata/v1.json".into(),
        };

        Ok(Response::new(reply))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let req = request.into_inner();
        println!("Creating table: {:?}", req);

        let reply = CreateTableResponse {
            table_identifier: req.table_identifier,
        };
        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let catalog = MyCatalog::default();

    println!("CatalogService listening on {}", addr);

    Server::builder()
        .add_service(CatalogServiceServer::new(catalog))
        .serve(addr)
        .await?;

    Ok(())
}
