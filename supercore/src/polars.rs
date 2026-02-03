use crate::table::Table;
use anyhow::Result;
use polars::prelude::*;

/// Bridges SuperTable with Polars.
pub struct PolarsConnector {
    table: Table,
}

impl PolarsConnector {
    pub fn new(table: Table) -> Self {
        Self { table }
    }

    /// Converts the current table state into a Polars LazyFrame.
    pub async fn scan(&self) -> Result<LazyFrame> {
        let scanner = self.table.new_scan();
        let tasks = scanner.plan().await?;

        // We'll collect all data into a temporary Parquet buffer
        // to avoid Arrow version conflicts between supercore and polars.
        let mut buffer: Vec<u8> = Vec::new();
        let schema = self.table.metadata.current_schema();
        let arrow_schema = schema.to_arrow_schema_ref();

        {
            let mut writer = parquet::arrow::ArrowWriter::try_new(&mut buffer, arrow_schema, None)?;

            let reader = crate::reader::TableReader::new(self.table.storage.clone());
            for task in tasks {
                let batches = reader.read_task(task).await?;
                for batch in &batches {
                    writer.write(batch)?;
                }
            }
            writer.close()?;
        }

        // Now read the buffer with Polars
        let cursor = std::io::Cursor::new(buffer);
        let df = ParquetReader::new(cursor).finish()?;
        Ok(df.lazy())
    }
}
