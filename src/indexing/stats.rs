use std::fs::File;
use std::io;
use std::io::Write;
use anyhow::Result;
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct IndexStats {
    pub n_docs: usize
    // More stuff could go here
}

pub struct Stats {
    stats: IndexStats
}

impl Stats {
    pub fn new(n_docs: usize) -> Self {
        Self { stats: IndexStats { n_docs } }
    }

    pub fn write_stats(&self) -> Result<()> {
        let stats_json = serde_json::to_string(&self.stats)?;

        let mut wtr = io::BufWriter::new(File::create("index_stats.json")?);
        wtr.write_all(stats_json.as_bytes())?;
        
        Ok(())
    }
}