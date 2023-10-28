use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::ptr::write;
use fst::MapBuilder;
use crate::aux;
use crate::aux::{write_value, write_value_from_serialized};

type Docid = String;
type Tf = u64;

struct PostingsBTree {
    postings : BTreeMap<
        // Token
        String,
        // Map of postings
        BTreeMap<Docid, Tf>>
}

impl PostingsBTree {
    pub fn new() -> Self {
        Self { postings: BTreeMap::new() }
    }

    pub fn add_token_to_docid(&mut self, docid: &String, token: &str) {
        if self.postings.contains_key(token) {
            let mut postings_for_token = self.postings.get_mut(token).unwrap();

            if postings_for_token.contains_key(docid) {
                let tf = postings_for_token.get(docid).unwrap();
                postings_for_token.insert(docid.clone(), tf.saturating_add(1));
            } else {
                postings_for_token.insert(docid.clone(), 1);
            }
        } else {
            let mut postings_for_token = BTreeMap::new();
            postings_for_token.insert(docid.clone(), 1);

            self.postings.insert(token.to_string(), postings_for_token);
        }
    }
}

pub struct Postings {
    index_key: String,
    postings_tree: PostingsBTree
}

impl Postings {
    pub fn new(index_key: String) -> Self {
        Self { index_key, postings_tree: PostingsBTree::new() }
    }

    pub fn add_token_to_docid(&mut self, docid: &String, token: &str) {
        self.postings_tree.add_token_to_docid(docid, token);
    }

    pub fn write_postings(&self) {
        let posting_positions = self.create_postings_file();
        self.create_postings_fst_file(&posting_positions);
    }

    fn create_postings_file(&self) -> BTreeMap<String, u64> {
        let mut file = File::create(format!("postings_data_{}.bin", &self.index_key)).unwrap();
        let mut positions: BTreeMap<String, u64> = BTreeMap::new();

        for (posting, value) in &self.postings_tree.postings {
            let serialized_value = aux::serialize_value(value);

            // Write its length first!
            let (start_position, _end_position) = write_value(&mut file, &serialized_value.len()).unwrap();
            let (_start_position, _end_position) = write_value_from_serialized(&mut file, serialized_value).unwrap();
            
            // And store that length's start position
            positions.insert(posting.clone(), start_position);
        }

        positions
    }

    fn create_postings_fst_file(&self, positions: &BTreeMap<String, u64>) {
        let wtr = io::BufWriter::new(File::create(format!("postings_index_{}.fst", self.index_key)).unwrap());

        let mut build = MapBuilder::new(wtr).unwrap();
        for (posting, pos) in positions {
            build.insert(posting, *pos).unwrap();
        }
        build.finish().unwrap();
    }
}




