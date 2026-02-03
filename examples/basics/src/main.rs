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

    println!("--------------------------------------------------");
    println!("🎉 SuperTable example completed successfully!");

    Ok(())
}
