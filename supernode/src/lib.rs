#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

/// Opens a SuperTable at the given location.
#[napi]
pub async fn open_table(identifier: String, location: String) -> napi::Result<String> {
    // Placeholder for JS bindings to the core library
    Ok(format!("Opened table {} at {}", identifier, location))
}

/// Deletes rows matching a filter.
#[napi]
pub async fn delete_table_rows(location: String, filter: String) -> napi::Result<String> {
    // Placeholder: In a real implementation, we would load the table from location
    // and call DeleteBuilder with the filter.
    // For now, we simulate success.
    Ok(format!("Deleted rows in {} matching {}", location, filter))
}

/// Merges data into the table.
#[napi]
pub async fn merge_table_rows(location: String, _on_column: String) -> napi::Result<String> {
    // Placeholder: In real implementation, we would accept a JS object as source
    Ok(format!("Merged data into {}", location))
}

/// Returns the current version of the SuperTable Node.js bindings.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
