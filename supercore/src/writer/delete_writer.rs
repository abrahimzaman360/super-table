use crate::manifest::{DataFile, FileContent, FileFormat};
use crate::storage::Storage;
use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use std::sync::Arc;

/// Writes position deletes (file_path, pos) to Parquet files.
pub struct PositionDeleteWriter {
    storage: Storage,
    table_location: String,
}

impl PositionDeleteWriter {
    pub fn new(storage: Storage, table_location: String) -> Self {
        Self {
            storage,
            table_location,
        }
    }

    /// Writes a batch of position deletes for a specific data file.
    pub async fn write_deletes(
        &self,
        target_file_path: &str,
        positions: Vec<i64>,
        file_id: &str,
    ) -> anyhow::Result<DataFile> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new("pos", DataType::Int64, false),
        ]));

        let len = positions.len();
        let file_path_array = StringArray::from(vec![target_file_path; len]);
        let pos_array = Int64Array::from(positions);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(file_path_array), Arc::new(pos_array)],
        )?;

        // Write Parquet
        let props = WriterProperties::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        let data = buffer.into_inner();
        let file_size = data.len() as i64;
        let record_count = len as i64;
        let path = format!("{}/data/{}-delete.parquet", self.table_location, file_id);

        self.storage.write(&path, Bytes::from(data)).await?;

        // Format is currently Data, should be PositionDeletes
        let data_file = DataFile::new(path, FileFormat::Parquet, record_count, file_size)
            .with_content(FileContent::PositionDeletes);

        Ok(data_file)
    }
}

/// Writes equality deletes to Parquet files.
pub struct EqualityDeleteWriter {
    storage: Storage,
    table_location: String,
    schema: SchemaRef,
    equality_ids: Vec<i32>,
}

impl EqualityDeleteWriter {
    pub fn new(
        storage: Storage,
        table_location: String,
        schema: SchemaRef,
        equality_ids: Vec<i32>,
    ) -> Self {
        Self {
            storage,
            table_location,
            schema,
            equality_ids,
        }
    }

    /// Writes a batch of equality deletes.
    pub async fn write_batch(
        &self,
        batch: &RecordBatch,
        file_id: &str,
    ) -> anyhow::Result<DataFile> {
        let props = WriterProperties::builder().build();
        let mut buffer = Cursor::new(Vec::new());
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, self.schema.clone(), Some(props))?;
            writer.write(batch)?;
            writer.close()?;
        }

        let data = buffer.into_inner();
        let file_size = data.len() as i64;
        let record_count = batch.num_rows() as i64;
        let path = format!("{}/data/{}-eq-delete.parquet", self.table_location, file_id);

        self.storage.write(&path, Bytes::from(data)).await?;

        // Calculate stats for pruning
        let stats = crate::statistics::calculate_stats(batch)?;

        let mut data_file = DataFile::new(path, FileFormat::Parquet, record_count, file_size)
            .with_content(FileContent::EqualityDeletes)
            .with_equality_ids(self.equality_ids.clone());

        data_file.statistics = stats;

        Ok(data_file)
    }
}
