#![deny(clippy::all)]

#[macro_use]
extern crate napi_derive;

/// Opens a SuperTable at the given location.
#[napi]
pub async fn open_table(identifier: String, location: String) -> napi::Result<String> {
    // Placeholder for JS bindings to the core library
    Ok(format!("Opened table {} at {}", identifier, location))
}

/// Returns the current version of the SuperTable Node.js bindings.
#[napi]
pub fn version() -> String {
    env!("CARGO_PKG_VERSION").to_string()
}
