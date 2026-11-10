pub(crate) mod acr;

use std::path::Path;

use anyhow::{Context, Result};
use overlaybd::dense_export;

pub(crate) async fn write_dense_overlaybd_layer_to_file(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    dense_export::write_dense_layer_to_file_with_digest(source, destination).await
}

pub(crate) fn write_dense_overlaybd_layer_to_file_blocking(
    source: &Path,
    destination: &Path,
) -> Result<dense_export::DenseLayerDescriptor> {
    // POSIX snapshot publish calls this inside `run_repository_blocking`.
    // Do not call it from an async task; use the async variant above instead.
    let source = source.to_path_buf();
    let destination = destination.to_path_buf();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("create runtime for dense overlaybd export")?;
    runtime.block_on(write_dense_overlaybd_layer_to_file(&source, &destination))
}
