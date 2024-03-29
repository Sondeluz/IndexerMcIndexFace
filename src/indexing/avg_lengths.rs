// Writer for the average document lengths for each field. Although this uses a FST-backed index, we could simply use
// a json file or similar, since the number of fields shouldn't be extremely high.

use anyhow::{Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::{io, mem};
use fst::MapBuilder;

pub struct Avglengths {
    avg_lengths: BTreeMap<String, f64>
}

impl Avglengths {
    pub fn new() -> Self {
        Self { avg_lengths: BTreeMap::new() }
    }

    pub fn add_avg_length(&mut self, index_key: String, avg_length: f64) {
        self.avg_lengths.insert(index_key, avg_length);
    }

    pub fn write_avg_lengths(&self) -> Result<()> {
        let avg_lengths_file = File::create("avg_lengths_index.fst")?;
        let wtr = io::BufWriter::new(avg_lengths_file);

        let mut build = MapBuilder::new(wtr)?;
        for (index_key, length) in &self.avg_lengths {
            // Dirty trick: Since the FST only allows integers, we can
            // simply write the f64 as an u64
            let length_as_u64 = unsafe {
                mem::transmute(length)
            };

            build.insert(index_key, length_as_u64)?;
        }
        build.finish()?;

        Ok(())
    }
}