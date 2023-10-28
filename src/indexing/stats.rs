use std::fs::File;
use std::io;
use std::io::Write;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct IndexStats {
    pub n_docs: usize
}

pub struct Stats {
    stats: IndexStats
}

impl Stats {
    pub fn new(n_docs: usize) -> Self {
        Self { stats: IndexStats { n_docs } }
    }

    pub fn write_stats(&self) {
        let stats_json = serde_json::to_string(&self.stats).unwrap();

        let mut wtr = io::BufWriter::new(File::create("index_stats.json").unwrap());
        wtr.write_all(stats_json.as_bytes()).expect("Failed to write index stats JSON file");
    }
}