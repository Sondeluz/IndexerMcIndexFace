use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io;
use fst::{IntoStreamer, Map};
use memmap::Mmap;
use crate::{aux, indexing};

#[derive(Debug)]
pub struct Retriever {
    index_keys: Vec<String>,

    lengths_maps: HashMap<String, Map<Mmap>>,
    avg_lengths_map: Map<Mmap>,
    postings_maps: HashMap<String, Map<Mmap>>,

    postings_data_files: HashMap<String, Mmap>,

    index_stats: indexing::stats::IndexStats
}


impl Retriever {
    pub fn new(index_keys: Vec<String>) -> Self {
        let mut lengths_maps = HashMap::new();
        let mut postings_maps = HashMap::new();
        let mut postings_data_files = HashMap::new();

        let avg_lengths_map = unsafe {
            Map::new(Mmap::map(&File::open("avg_lengths_index.fst").unwrap()).unwrap()).unwrap()
        };

        for index_key in &index_keys {
            let mmap = unsafe {
                Mmap::map(&File::open(format!("lengths_index_{}.fst", index_key)).unwrap()).unwrap()
            };
            lengths_maps.insert(index_key.clone(), Map::new(mmap).unwrap());

            let mmap = unsafe {
                Mmap::map(&File::open(format!("postings_index_{}.fst", index_key)).unwrap()).unwrap()
            };
            postings_maps.insert(index_key.clone(), Map::new(mmap).unwrap());

            let mmap = unsafe {
                Mmap::map(&File::open(format!("postings_data_{}.bin", index_key)).unwrap()).unwrap()
            };
            postings_data_files.insert(index_key.clone(), mmap);
        }

        let index_stats = serde_json::from_reader(io::BufReader::new(File::open("index_stats.json").unwrap()))
            .expect("Failed to read index stats JSON file");

        Self { index_keys, lengths_maps, avg_lengths_map, postings_maps, postings_data_files, index_stats }
    }

    pub fn search(&self, query_token: &str, k1: f64, b: f64, field_weights : HashMap<String, f64>) -> HashMap<String, f64> {
        let matching_docids = self.get_matching_docids(query_token);
        let lengths = self.get_lengths(&matching_docids);
        let mut weighted_avg_lengths = HashMap::new();
        for (index_key ,avg_length) in self.get_avg_lengths(&matching_docids) {
            weighted_avg_lengths.insert(index_key.clone(), field_weights.get(&index_key).unwrap() * (avg_length as f64));
        }

        let doc_frequency = self.get_doc_frequency(&matching_docids);

        let idf = (self.index_stats.n_docs as f64) / (doc_frequency as f64);

        let mut doc_scores: HashMap<String, f64> = HashMap::new();

        // Calculate scores per field, instead of per doc_id
        for (index_key, postings) in matching_docids {
            let field_weight = field_weights.get(&index_key).expect(&format!("Field weight not found for {}", &index_key));

            for (doc_id, tf) in postings {
                let new_tf = field_weight * (tf as f64);
                let weighted_doc_len = self.get_bm25f_doc_len(&doc_id, &lengths, &field_weights);

                let bm25f_field = (new_tf * (k1 + 1.0)) / (k1 * ((1.0 - b) + b * (weighted_doc_len / weighted_avg_lengths.get(&index_key).unwrap()) + new_tf));

                if doc_scores.contains_key(&doc_id) {
                    doc_scores.insert(doc_id.clone(), doc_scores.get(&doc_id).unwrap() + bm25f_field);
                } else {
                    doc_scores.insert(doc_id.clone(), bm25f_field);
                }
            }
        }

        doc_scores
            .into_iter()
            .map(|(doc_id, score)| (doc_id.trim_end_matches('\0').to_string(), idf * score))
            .collect()
    }

    fn get_bm25f_doc_len(&self, doc_id: &str, lengths : &HashMap<String, HashMap<String, u64>>, field_weights : &HashMap<String, f64>) -> f64 {
        let mut doc_len = 0.0;

        for (index_key, doc_lengths) in lengths {
            if doc_lengths.contains_key(doc_id) {
                let unweighted_doc_len = *doc_lengths.get(doc_id).unwrap() as f64;
                let field_weight = *field_weights.get(index_key).unwrap();

                doc_len += field_weight * unweighted_doc_len;
            }
        }

        doc_len
    }

    pub(crate) fn get_matching_docids(&self, query_token: &str) -> HashMap<String, BTreeMap<String, u64>> {
        let mut matching_docids = HashMap::new();

        for index_key in &self.index_keys {
            let postings_fst = self.postings_maps.get(index_key).unwrap();

            if let Some(start_pos) = postings_fst.get(query_token) {
                let postings_file = self.postings_data_files.get(index_key).unwrap();

                let postings_size: u64 = aux::read_value_from_mmap(&postings_file, start_pos, start_pos+8).unwrap();
                let postings: BTreeMap<String, u64> = aux::read_value_from_mmap(&postings_file, start_pos+8, start_pos+8+postings_size).unwrap();

                matching_docids.insert(index_key.clone(), postings);
            }
        }

        matching_docids
    }

    fn get_lengths(&self, matching_docids: &HashMap<String, BTreeMap<String, u64>>) -> HashMap<String, HashMap<String, u64>> {
        let mut lengths: HashMap<String, HashMap<String, u64>> = HashMap::new();

        for (index_key, postings) in matching_docids {
            for docid in postings.keys() {
                if let Some(length) = self.get_docid_length(docid, index_key) {
                    if lengths.contains_key(index_key) {
                        lengths.get_mut(index_key).unwrap()
                            .insert((*docid.clone()).to_string(), length);
                    } else {
                        let mut internal_map = HashMap::new();
                        internal_map.insert(docid.clone(), length);
                        lengths.insert(index_key.clone(), internal_map);
                    }
                } else {
                    // TODO
                    panic!("Unknown docid length")
                }
            }
        }

        lengths
    }

    fn get_avg_lengths(&self, matching_docids: &HashMap<String, BTreeMap<String, u64>>) -> HashMap<String, u64> {
        let mut avg_lengths = HashMap::new();

        for index_key in matching_docids.keys() {
            avg_lengths.insert(index_key.clone(), self.avg_lengths_map.get(index_key).unwrap());
        }

        avg_lengths
    }

    fn get_docid_length(&self, docid: &str, index_key: &str) -> Option<u64> {
        let lengths_fst = self.lengths_maps.get(index_key).unwrap();

        lengths_fst.get(docid)
    }

    fn get_doc_frequency(&self, matching_docids: &HashMap<String, BTreeMap<String, u64>>) -> u64 {
        let mut docs = HashSet::new();

        for postings_map in matching_docids.values() {
            for doc_id in postings_map.keys() {
                docs.insert(doc_id.clone());
            }
        }

        docs.len() as u64
    }
}