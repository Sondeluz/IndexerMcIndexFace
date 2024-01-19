// Writer for the index postings for a given field
// Postings are divided into a FST-backed index file and a raw postings file. The index maps tokens to the raw file's 
// starting position of its serialized postings map, which contains the length of the serialized value immediately 
// followed by it

use crate::aux;
use crate::aux::{write_buffer_to_binary_file, write_value_to_binary_file};
use anyhow::Result;
use fst::MapBuilder;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;

type Docid = String;
type Tf = u64;

struct PostingsBTree {
    postings: HashMap<
        // Token
        String,
        // Map of postings
        HashMap<Docid, Tf>,
    >,
}

impl PostingsBTree {
    pub fn new() -> Self {
        Self {
            postings: HashMap::new(),
        }
    }

    pub fn add_token_to_docid(&mut self, docid: &String, token: &str) {
        let postings_for_token = self
            .postings
            .entry(token.to_string())
            .or_default();
        let tf = postings_for_token.entry(docid.to_string()).or_insert(0);
        *tf += 1;
    }

    /// For merging Postings instances
    pub fn add_tree(&mut self, postings_to_merge: &Postings) {
        for (token, postings_map) in &postings_to_merge.postings_tree.postings {
            let target_entry = self
                .postings
                .entry(token.to_string())
                .or_default();

            for (doc_id, tf) in postings_map {
                *target_entry.entry(doc_id.to_string()).or_insert(0) += tf;
            }
        }
    }
}

pub struct Postings {
    index_key: String,
    postings_tree: PostingsBTree,
}

impl Postings {
    pub fn new(index_key: String) -> Self {
        Self {
            index_key,
            postings_tree: PostingsBTree::new(),
        }
    }

    pub fn add_token_to_docid(&mut self, docid: &String, token: &str) {
        self.postings_tree.add_token_to_docid(docid, token);
    }

    /// Merge both Postings instances into this one
    pub fn add_postings(&mut self, postings_to_merge: &Postings) {
        self.postings_tree.add_tree(postings_to_merge);
    }

    pub fn write_postings(&mut self) -> Result<()> {
        let posting_positions = self.create_postings_file();
        self.create_postings_fst_file(&posting_positions?)
    }

    fn create_postings_file(&mut self) -> Result<BTreeMap<String, u64>> {
        let mut file = File::create(format!("postings_data_{}.bin", &self.index_key))?;
        let mut positions: BTreeMap<String, u64> = BTreeMap::new();

        let mut ordered_postings: Vec<_> = self.postings_tree.postings.drain().collect();
        ordered_postings.sort_by(|a, b| a.0.cmp(&b.0));

        for (posting, value) in &ordered_postings {
            let serialized_value = aux::serialize_value(value);

            // Write its length first!
            let (start_position, _end_position) =
                write_value_to_binary_file(&mut file, &serialized_value.len())?;
            let (_start_position, _end_position) =
                write_buffer_to_binary_file(&mut file, serialized_value)?;

            // And store that length's start position
            positions.insert(posting.clone(), start_position);
        }

        Ok(positions)
    }

    fn create_postings_fst_file(&self, positions: &BTreeMap<String, u64>) -> Result<()> {
        let wtr = io::BufWriter::new(
            File::create(format!("postings_index_{}.fst", self.index_key))?,
        );

        let mut build = MapBuilder::new(wtr)?;
        for (posting, pos) in positions {
            build.insert(posting, *pos)?;
        }
        build.finish()?;
        
        Ok(())
    }
}
