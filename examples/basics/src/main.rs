use object_store::memory::InMemory;
use std::sync::Arc;
use supercore::prelude::*;
use supercore::table::Table;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("🚀 SuperTable Basics Example");

    // 1. Initialize Storage and Catalog
    let storage = Storage::new(Arc::new(InMemory::new()));
    let catalog = InMemoryCatalog::shared("local_catalog");

    // 2. Define Schema
    let schema = Schema::builder(1)
        .with_field(1, "id", Type::Long, true)
        .with_field(2, "user", Type::String, true)
        .with_field(3, "score", Type::Int, false)
        .build();

    println!("✅ Schema defined: {} fields", schema.fields.len());

    // 3. Create Table
    let identifier = TableIdentifier::parse("default.users");
    let location = identifier.to_string(); // Match identifier for catalog lookup
    let metadata = TableMetadata::builder(&location, schema.clone()).build();

    catalog.create_table(&identifier, metadata.clone()).await?;
    let mut table = Table::new(
        identifier.to_string(),
        catalog.clone(),
        metadata,
        storage.clone(),
    );

    println!("✅ Table created: {}", table.identifier());

    // 4. Perform an Append Transaction
    println!("📝 Starting append transaction...");
    let mut tx = table.new_transaction().with_operation(Operation::Append);

    // Add a dummy data file to satisfy change requirements
    tx.add_file(DataFile::new(
        "data/file1.parquet",
        FileFormat::Parquet,
        100,       // 100 records
        1024 * 10, // 10KB
    ));

    // In a real example we'd write data files here.
    // For this basic walkthrough, we'll demonstrate metadata commit.
    let snapshot = tx.commit(catalog.clone()).await?;
    println!(
        "✅ Committed snapshot {}: operation={:?}",
        snapshot.snapshot_id, snapshot.operation
    );

    // Refresh table to get the latest metadata from catalog
    table.refresh().await?;

    // 5. Time Travel
    println!("🕰️ Demonstrating Time Travel...");

    // Get version at current snapshot
    let table_v1 = table.at_snapshot(snapshot.snapshot_id)?;
    println!(
        "✅ Historical view loaded for snapshot {}",
        table_v1.metadata().current_snapshot_id.unwrap()
    );

    // 6. Schema Evolution
    println!("🧬 Evolving Schema...");
    let mut next_id = table.metadata().next_column_id();

    use supercore::schema::SchemaUpdate;
    let update =
        SchemaUpdate::new().add_column(None, "email", Type::String, Some("User email".into()));

    let new_schema = update
        .apply(&schema, table.metadata().next_schema_id(), &mut next_id)
        .map_err(|e: String| anyhow::anyhow!(e))?;

    println!(
        "✅ New column 'email' added to schema {}",
        new_schema.schema_id
    );

    // 7. Table Metrics
    println!("📊 Checking Table Metrics...");
    table.refresh().await?;
    let metrics = &table.metadata().metrics;
    println!("   Total Records: {}", metrics.total_records);
    println!("   Total Files:   {}", metrics.total_files);
    println!("   Total Size:    {} bytes", metrics.total_size_bytes);

    // 8. Audit Reporting
    println!("📋 Generating Audit Reports...");
    let reporter =
        superaudit::AuditReporter::new(table.metadata().clone(), table.identifier().to_string());

    // JSON Export
    reporter.export_json("audit_report.json")?;
    println!("   ✅ Audit report saved to audit_report.json");

    // Markdown Export
    reporter.export_markdown("audit_report.md")?;
    println!("   ✅ Audit report saved to audit_report.md");

    // PDF export
    reporter.export_pdf("audit_report.pdf")?;
    println!("   ✅ Audit report saved to audit_report.pdf");

    // 9. Polars Integration
    println!("🐻 Querying via Polars...");
    let connector = supercore::polars::PolarsConnector::new(table.clone());
    match connector.scan().await {
        Ok(lf) => {
            let df = lf.collect()?;
            println!("   ✅ Polars DataFrame retrieved ({} rows):", df.height());
            println!("{}", df.head(Some(5)));
        }
        Err(e) => {
            println!(
                "   ⚠️ Polars scan skipped (no manifest data in memory): {}",
                e
            );
        }
    }

    // 10. CDC & Checkpointing
    println!("🔄 Demonstrating CDC & Checkpointing...");
    let cdc = supercore::cdc::ChangeDataCapture::new(table.clone());

    // Create a checkpoint at current state
    let checkpoint = supercore::cdc::Checkpoint {
        last_snapshot_id: snapshot.snapshot_id,
        last_sequence_number: table.metadata().last_sequence_number,
    };

    match cdc.save_checkpoint(&checkpoint, "sync_point").await {
        Ok(_) => {
            println!(
                "   ✅ Checkpoint 'sync_point' saved at snapshot {}",
                snapshot.snapshot_id
            );
        }
        Err(e) => {
            println!("   ⚠️ Checkpoint save skipped (in-memory storage): {}", e);
        }
    }

    // Load and verify - only attempt if save was successful (in-memory will fail)
    println!("   ℹ️  CDC checkpoints require persistent storage for full demo.");

    println!("--------------------------------------------------");
    println!("🎉 SuperTable example completed successfully!");

    Ok(())
}
