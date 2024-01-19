//! Auxiliary FST serializing and deserializing functions
use memmap::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::{Seek, Write};
use anyhow::{Context, Result};

/// Serializes a value using bincode
pub fn serialize_value<T>(value: &T) -> Vec<u8>
where
    T: Serialize,
{
    bincode::serialize(value).unwrap()
}

/// Write a buffer (serialized value) to the file, returning its starting and ending positions
pub fn write_buffer_to_binary_file(
    file: &mut File,
    serialized_value: Vec<u8>,
) -> Result<(u64, u64)> {
    let start_position = file.stream_position()
        .with_context(|| "Failed to read binary file when writing to postings")?;

    file.write_all(&serialized_value)
        .with_context(|| "Failed to write serialized value to postings")?;

    let end_position = file.stream_position()
        .with_context(|| "Failed to write serialized value to postings")?;

    Ok((start_position, end_position))
}

/// Write a bincode-serializable value to the file, returning its starting and ending positions
pub fn write_value_to_binary_file<T>(file: &mut File, value: &T) -> Result<(u64, u64)>
where
    T: Serialize,
{
    let start_position = file.stream_position()
        .with_context(|| "Failed to read binary file when writing to postings")?;

    let serialized_value = bincode::serialize(value)
        .with_context(|| "Failed to serialize extraneous value when writing to postings")?;
    file.write_all(&serialized_value)
        .with_context(|| "Failed to write serialized value to postings")?;

    let end_position = file.stream_position()
        .with_context(|| "Failed to read binary file when writing to postings")?;

    Ok((start_position, end_position))
}

/// Read a value of type T from a Mmapped file, given its starting and ending position
pub fn read_value_from_mmap<T>(file: &Mmap, start: u64, end: u64) -> Result<T>
where
    T: DeserializeOwned,
{
    let slice = &file[start as usize..end as usize];
    let deserialized_value: T = bincode::deserialize(slice)
        .with_context(|| "Failed to deserialize extraneous value when writing to postings")?;

    Ok(deserialized_value)
}
