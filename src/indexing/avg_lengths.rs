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

    pub fn write_avg_lengths(&self) {
        let wtr = io::BufWriter::new(File::create("avg_lengths_index.fst").unwrap());


        let mut build = MapBuilder::new(wtr).unwrap();
        for (index_key, length) in &self.avg_lengths {
            let length_as_u64 = unsafe {
                mem::transmute(length)
            };

            build.insert(index_key, length_as_u64).unwrap();
        }
        build.finish().unwrap();
    }
}