use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use std::sync::Arc;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<dyn ObjectStore>,
}

impl std::fmt::Debug for Storage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Storage").finish()
    }
}

impl Storage {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }

    /// Creates a Storage from a location string (e.g., "s3://bucket/path" or "/local/path")
    /// Defaults to LocalFileSystem if no scheme is provided or file:// scheme is used.
    pub fn from_location(location: &str) -> anyhow::Result<Self> {
        let url = url::Url::parse(location)
            .map_err(|e| anyhow::anyhow!("Invalid location URL: {}", e))?;
        let (store, _path) = object_store::parse_url(&url)?;
        Ok(Self::new(store.into()))
    }

    pub async fn read(&self, path: &str) -> anyhow::Result<Bytes> {
        let path = Path::from(path);
        let result = self.inner.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
    }

    pub async fn write(&self, path: &str, content: Bytes) -> anyhow::Result<()> {
        let path = Path::from(path);
        self.inner.put(&path, content.into()).await?;
        Ok(())
    }

    pub async fn delete(&self, path: &str) -> anyhow::Result<()> {
        let path = Path::from(path);
        self.inner.delete(&path).await?;
        Ok(())
    }

    pub async fn list_files(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let prefix = Path::from(prefix);
        let mut list = self.inner.list(Some(&prefix));
        let mut files = Vec::new();

        use futures::StreamExt;
        while let Some(meta) = list.next().await {
            let meta = meta?;
            files.push(meta.location.to_string());
        }

        Ok(files)
    }
}
