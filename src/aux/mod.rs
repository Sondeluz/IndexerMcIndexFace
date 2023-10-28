use std::fs::File;
use std::{io, mem};
use std::io::{Read, Seek, SeekFrom, Write};
use fst::Map;
use memmap::Mmap;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn serialize_value<T>(value: &T) -> Vec<u8>
    where
        T: Serialize,
{
    bincode::serialize(value).unwrap()
}

pub fn write_value_from_serialized(file: &mut File, serialized_value: Vec<u8>) -> io::Result<(u64, u64)> {
    let start_position = file.seek(SeekFrom::Current(0))?;

    file.write_all(&serialized_value)?;

    let end_position = file.seek(SeekFrom::Current(0))?;
    Ok((start_position, end_position))
}

pub fn write_value<T>(file: &mut File, value: &T) -> io::Result<(u64, u64)>
    where
        T: Serialize,
{
    let start_position = file.seek(SeekFrom::Current(0))?;

    let serialized_value = bincode::serialize(value).unwrap();
    file.write_all(&serialized_value)?;

    let end_position = file.seek(SeekFrom::Current(0))?;
    Ok((start_position, end_position))
}

pub fn read_value<T>(file: &mut File, position: u64) -> io::Result<T>
    where
        T: DeserializeOwned,
{
    file.seek(SeekFrom::Start(position))?;

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer)?;

    let deserialized_value: T = bincode::deserialize(&buffer).unwrap();
    Ok(deserialized_value)
}

pub fn read_value_from_mmap<T>(file: &Mmap, start: u64, end: u64) -> io::Result<T>
    where
        T: DeserializeOwned,
{
    let slice = &file[start as usize..end as usize];
    let deserialized_value: T = bincode::deserialize(slice).unwrap();

    Ok(deserialized_value)
}

pub fn query_fst_u64(fst_map: &Map<Mmap>, key: &str) -> Option<u64> {
    fst_map.get(key)
}

pub fn query_fst_f64(fst_map: &Map<Mmap>, key: &str) -> Option<f64> {
    unsafe {
        mem::transmute(fst_map.get(key))
    }
}

