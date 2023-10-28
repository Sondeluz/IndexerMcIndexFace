use std::collections::HashMap;
use std::fs::File;
use fst::{IntoStreamer, Map};
use memmap::Mmap;
use crate::indexing::avg_lengths::Avglengths;
use crate::indexing::lengths::Lengths;
use crate::indexing::postings::Postings;
use crate::indexing::stats::Stats;
use crate::tokenizer;

pub struct Indexer {
    postings_writers: HashMap<String, Postings>,
    lengths_writers: HashMap<String, Lengths>,
    avg_lengths_writer: Avglengths
}

impl Indexer {
    pub fn new(field_keys: HashMap<String, String>) -> Self {
        let mut postings_writers = HashMap::new();
        let mut length_writers = HashMap::new();
        let avg_lengths_writer = Avglengths::new();

        for index_key in field_keys.values() {
            postings_writers.insert(index_key.clone(), Postings::new(index_key.clone()));
            length_writers.insert(index_key.clone(), Lengths::new(index_key.clone()));
        }

        Self { postings_writers, lengths_writers: length_writers, avg_lengths_writer }
    }

    pub fn index(&mut self, iter : impl Iterator<Item = Option<(String, HashMap<String, String>)>>) {
        let index_keys: Vec<String> = self.postings_writers.keys().cloned().collect();
        let mut n_docs: usize = 0;

        for (mut docid, fields_text) in iter.flatten() {
            n_docs += 1;

            let mut doc_id_bytes : [u8 ; 128] = [0; 128];

            for (byte_i, docid_byte) in docid.as_bytes().iter().enumerate() {
                doc_id_bytes[byte_i] = *docid_byte;
            }

            let docid = match std::str::from_utf8(&doc_id_bytes) {
                Ok(v) => v.to_string(),
                Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
            };

            for index_key in &index_keys {
                if let Some(field_text) = fields_text.get(index_key) {
                    let tokens = tokenizer::tokenize(field_text)
                        .iter()
                        .map(|t| t.clean())
                        .collect::<Vec<String>>();

                    for token in tokens {
                        // Get the PostingsWriter for this field
                        self.postings_writers.get_mut(index_key)
                            .unwrap()
                            // And count the token for the docid
                            .add_token_to_docid(&docid, &token);
                    }

                    self.lengths_writers.get_mut(index_key)
                        .unwrap()
                        .add_length(docid.clone(), field_text.len() as u64);
                }
            }
        }

        for index_key in &index_keys {
            let postings_writer = self.postings_writers.get(index_key).unwrap();
            postings_writer.write_postings();

            let lengths_writer = self.lengths_writers.get_mut(index_key).unwrap();
            let avg_length = lengths_writer.write_lengths();

            self.avg_lengths_writer.add_avg_length(index_key.clone(), avg_length);
            self.avg_lengths_writer.write_avg_lengths();
        }

        let stats_writer = Stats::new(n_docs);
        stats_writer.write_stats();
    }
}