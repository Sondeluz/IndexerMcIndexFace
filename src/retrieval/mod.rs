// BM25F-based retriever implementation (Warning: I didn't verify its correctness)

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::{io, thread};
use anyhow::{anyhow, Result};
use crossbeam_channel::bounded;
use fst::Map;
use indicatif::{ProgressBar};
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
    pub fn new(index_keys: Vec<String>) -> Result<Self> {
        let mut lengths_maps = HashMap::new();
        let mut postings_maps = HashMap::new();
        let mut postings_data_files = HashMap::new();

        let avg_lengths_map = unsafe {
            Map::new(Mmap::map(&File::open("avg_lengths_index.fst")?)?)?
        };

        for index_key in &index_keys {
            let mmap = unsafe {
                Mmap::map(&File::open(format!("lengths_index_{}.fst", index_key))?)?
            };
            lengths_maps.insert(index_key.clone(), Map::new(mmap)?);

            let mmap = unsafe {
                Mmap::map(&File::open(format!("postings_index_{}.fst", index_key))?)?
            };
            postings_maps.insert(index_key.clone(), Map::new(mmap)?);

            let mmap = unsafe {
                Mmap::map(&File::open(format!("postings_data_{}.bin", index_key))?)?
            };
            postings_data_files.insert(index_key.clone(), mmap);
        }

        let index_stats = serde_json::from_reader(io::BufReader::new(File::open("index_stats.json")?))?;

        Ok(Self { index_keys, lengths_maps, avg_lengths_map, postings_maps, postings_data_files, index_stats })
    }

    /// Run a BM25F query on a multi-token query. Internally parallelized
    pub fn retrieval_multiple_tokens(&self,
                                     query_tokens: &Vec<String>,
                                     field_k1_params: &HashMap<String, f64>,
                                     field_b_params: &HashMap<String, f64>,
                                     field_weights : &HashMap<String, f64>) -> Result<Vec<(String, f64)>> {
        let (jobs_send_channel, jobs_recv_channel) =
            bounded::<Option<String>>(query_tokens.len());
        let (results_send_channel, results_recv_channel) =
            bounded::<Option<HashMap<String, f64>>>(query_tokens.len());

        for token in query_tokens {
            jobs_send_channel.send(Some(token.clone())).unwrap();
        }

        // Run single-token search functions on a thread pool, and then merge all results
        //
        // A better idea would be to have a mix of retrieval and merge threads, but if we had such strict time
        // requirements we may as well do it in a distributed environment
        thread::scope(|scope| {
            let mut handles = Vec::new();

            for _ in 0..num_cpus::get() {
                let results_send_channel_clone = results_send_channel.clone();
                let jobs_recv_channel_clone = jobs_recv_channel.clone();

                handles.push(scope.spawn(move || {
                    loop {
                        if let Some(token) = jobs_recv_channel_clone.recv().unwrap() {
                            let results = self.retrieval_single_token(
                                &token,
                                field_k1_params,
                                field_b_params,
                                field_weights,
                            ).unwrap();

                            results_send_channel_clone.send(Some(results)).unwrap();
                        } else {
                            results_send_channel_clone
                                .send(None)
                                .unwrap();
                            break;
                        }
                    }
                }));
            }

            let progress_bar = ProgressBar::new(query_tokens.len() as u64);
            progress_bar.println("Retrieving results for each token...");
            let mut results: Vec<HashMap<String, f64>> = Vec::new();
            for _ in query_tokens {
                results.push(results_recv_channel.recv()
                    .unwrap()
                    .unwrap());
                progress_bar.inc(1);
            }

            progress_bar.finish();

            for _ in 0..num_cpus::get() {
                jobs_send_channel.send(None).unwrap();
                results_recv_channel.recv().unwrap();
            }

            for _ in 0..num_cpus::get() {
                handles.pop().unwrap().join().unwrap();
            }


            // TODO merge ordered
            println!("Merging results...");
            let mut merged_results: HashMap<String, f64> = HashMap::new();
            for map in results {
                for (key, value) in map {
                    *merged_results.entry(key.clone()).or_insert(0.0) += value;
                }
            }

            let mut ordered_results: Vec<(String, f64)> = merged_results.into_iter().collect();
            ordered_results.sort_by(|a, b| b.1.total_cmp(&a.1));

            Ok(ordered_results)
        })
    }

    /// Run a BM25F query on a single-token query
    pub fn retrieval_single_token(&self,
                                  query_token: &str,
                                  field_k1_params: &HashMap<String, f64>,
                                  field_b_params: &HashMap<String, f64>,
                                  field_weights : &HashMap<String, f64>) -> Result<HashMap<String, f64>> {
        let matching_docids_postings = self.get_matching_docids_postings(query_token)?;
        let lengths = self.get_lengths(&matching_docids_postings)?;
        let mut weighted_avg_lengths = HashMap::new();
        for (index_key ,avg_length) in self.get_avg_lengths(&matching_docids_postings) {
            weighted_avg_lengths.insert(index_key.clone(), field_weights.get(&index_key).unwrap() * (avg_length as f64));
        }

        let doc_frequency = self.get_doc_frequency(&matching_docids_postings);

        let idf = (self.index_stats.n_docs as f64) / (doc_frequency as f64);

        let mut doc_scores: HashMap<String, f64> = HashMap::new();

        // Calculate scores per field, instead of per doc_id
        for (index_key, postings) in matching_docids_postings {
            let field_weight = field_weights.get(&index_key)
                .unwrap_or_else(|| panic!("Field weight not found for {}, cannot perform retrieval!", &index_key));

            for (doc_id, tf) in postings {
                let new_tf = field_weight * (tf as f64);
                let weighted_doc_len = self.get_bm25f_doc_len(&doc_id, &lengths, field_weights);
                let k1 = field_k1_params.get(&index_key).unwrap();
                let b = field_b_params.get(&index_key).unwrap();

                let bm25f_field = (new_tf * (k1 + 1.0)) / (k1 * ((1.0 - b) + b * (weighted_doc_len / weighted_avg_lengths.get(&index_key).unwrap()) + new_tf));

                if doc_scores.contains_key(&doc_id) {
                    doc_scores.insert(doc_id.clone(), doc_scores.get(&doc_id).unwrap() + bm25f_field);
                } else {
                    doc_scores.insert(doc_id.clone(), bm25f_field);
                }
            }
        }

        Ok(doc_scores
            .into_iter()
            .map(|(doc_id, score)| (doc_id.trim_end_matches('\0').to_string(), idf * score))
            .collect())
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

    /// Returns a Map of index_key -> Map of doc_id -> Tf for query_token
    pub(crate) fn get_matching_docids_postings(&self, query_token: &str) -> Result<HashMap<String, BTreeMap<String, u64>>> {
        let mut matching_docids = HashMap::new();

        for index_key in &self.index_keys {
            let postings_fst = self.postings_maps.get(index_key).unwrap();

            if let Some(start_pos) = postings_fst.get(query_token) {
                let postings_file = self.postings_data_files.get(index_key).unwrap();

                let postings_size: u64 = aux::read_value_from_mmap(postings_file, start_pos, start_pos+8)?;
                let postings: BTreeMap<String, u64> = aux::read_value_from_mmap(postings_file, start_pos+8, start_pos+8+postings_size)?;

                matching_docids.insert(index_key.clone(),
                                       // Map of doc_id -> Tf
                                       postings);
            }
        }

        Ok(matching_docids)
    }

    fn get_lengths(&self, matching_docids: &HashMap<String, BTreeMap<String, u64>>) -> Result<HashMap<String, HashMap<String, u64>>> {
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
                    return Err(anyhow!(format!("Couldn't find the length for docid {}", docid)));
                }
            }
        }

        Ok(lengths)
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