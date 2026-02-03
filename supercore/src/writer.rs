use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;

use crate::storage::Storage;

/// Writes Arrow RecordBatches to Parquet files and commits them to storage.
pub struct TableWriter {
    storage: Storage,
    table_location: String,
    schema: SchemaRef,
}

impl TableWriter {
    pub fn new(storage: Storage, table_location: String, schema: SchemaRef) -> Self {
        Self {
            storage,
            table_location,
            schema,
        }
    }

    /// Writes a batch of records to a new Parquet file.
    /// Returns the DataFile metadata for the written file.
    pub async fn write_batch(
        &self,
        batch: &RecordBatch,
        file_id: &str,
    ) -> anyhow::Result<crate::manifest::DataFile> {
        // Validate schema before writing
        let validator = crate::validation::SchemaValidator::new(self.schema.clone());
        validator.validate(batch)?;

        let props = WriterProperties::builder().build();

        // Write to an in-memory buffer first
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, self.schema.clone(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
        }

        let data = buffer.into_inner();
        let file_size = data.len() as i64;
        let record_count = batch.num_rows() as i64;
        let path = format!("{}/data/{}.parquet", self.table_location, file_id);

        self.storage.write(&path, Bytes::from(data)).await?;

        let stats = crate::statistics::calculate_stats(batch)?;

        let mut data_file = crate::manifest::DataFile::new(
            path,
            crate::manifest::FileFormat::Parquet,
            record_count,
            file_size,
        );
        data_file.statistics = stats;

        Ok(data_file)
    }
}
