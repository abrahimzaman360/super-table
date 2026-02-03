use clap::{Parser, Subcommand};

pub mod catalog {
    tonic::include_proto!("catalog");
}

use catalog::catalog_service_client::CatalogServiceClient;
use catalog::{CreateTableRequest, GetTableRequest};

#[derive(Parser)]
#[command(name = "supercli")]
#[command(about = "CLI for SuperTable - the next-gen table format", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Create a new table
    Create {
        /// The table identifier (e.g., "my_namespace.my_table")
        #[arg(short, long)]
        name: String,
        /// The base location for the table data (e.g., "s3://bucket/tables/my_table")
        #[arg(short, long)]
        location: String,
    },
    /// Get information about a table
    Get {
        /// The table identifier
        #[arg(short, long)]
        name: String,
    },
    /// Generate an audit report for a table
    Audit {
        /// The table identifier
        #[arg(short, long)]
        name: String,
        /// The format of the report (json, markdown, pdf)
        #[arg(short, long, default_value = "markdown")]
        format: String,
        /// The output file path
        #[arg(short, long)]
        output: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Connect to the catalog server
    let mut client = CatalogServiceClient::connect("http://[::1]:50051").await?;

    match cli.command {
        Commands::Create { name, location } => {
            println!("Creating table '{}' at '{}'...", name, location);
            let request = tonic::Request::new(CreateTableRequest {
                table_identifier: name.clone(),
                location,
            });
            let response = client.create_table(request).await?;
            println!("Table created: {:?}", response.into_inner());
        }
        Commands::Get { name } => {
            println!("Getting table '{}'...", name);
            let request = tonic::Request::new(GetTableRequest {
                table_identifier: name.clone(),
            });
            let response = client.get_table(request).await?;
            let table = response.into_inner();
            println!("Table: {}", table.table_identifier);
            println!("Metadata Location: {}", table.metadata_location);
        }
        Commands::Audit {
            name,
            format,
            output,
        } => {
            println!("Auditing table '{}'...", name);
            let request = tonic::Request::new(GetTableRequest {
                table_identifier: name.clone(),
            });
            let response = client.get_table(request).await?;
            let table = response.into_inner();

            // Load metadata from the location
            let metadata_json = std::fs::read_to_string(&table.metadata_location)?;
            let metadata: supercore::TableMetadata = serde_json::from_str(&metadata_json)?;

            let reporter = superaudit::AuditReporter::new(metadata, name);
            match format.to_lowercase().as_str() {
                "json" => reporter.export_json(&output)?,
                "markdown" | "md" => reporter.export_markdown(&output)?,
                "pdf" => reporter.export_pdf(&output)?,
                _ => println!("Unsupported format: {}", format),
            }
            println!("Audit report exported to {}", output);
        }
    }

    Ok(())
}
