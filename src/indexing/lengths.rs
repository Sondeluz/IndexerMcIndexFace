// Writer for the document lengths for a given field

use anyhow::Result;
use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use fst::MapBuilder;

pub struct Lengths {
    index_key: String,
    lengths: BTreeMap<String, u64>
}

impl Lengths {
    pub fn new(index_key: String) -> Self {
        Self { index_key, lengths: BTreeMap::new() }
    }

    pub fn add_length(&mut self, docid: String, length: u64) {
        self.lengths.insert(docid, length);
    }

    pub fn add_lengths(&mut self, lengths_to_add: &Lengths) {
        for (doc_id, length) in &lengths_to_add.lengths {
            self.add_length(doc_id.clone(), *length)
        }
    }

    pub fn write_lengths(&self) -> Result<f64> {
        let wtr = io::BufWriter::new(File::create(format!("lengths_index_{}.fst", self.index_key))?);

        let mut avg_length: u64 = 0;

        let mut build = MapBuilder::new(wtr)?;
        for (docid, length) in &self.lengths {
            build.insert(docid, *length)?;
            avg_length += length;
        }
        build.finish()?;

        Ok((avg_length as f64) / (self.lengths.len() as f64))
    }
}