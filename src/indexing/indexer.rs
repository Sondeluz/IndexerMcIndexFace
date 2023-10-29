use crate::indexing::avg_lengths::Avglengths;
use crate::indexing::lengths::Lengths;
use crate::indexing::postings::Postings;
use crate::indexing::stats::Stats;
use crate::tokenizer;
use dashmap::DashMap;
use std::sync::{Arc};
use std::thread;
use crossbeam_channel::bounded;

pub struct Indexer {
    field_keys: DashMap<String, String>
}

type IndexJob = (String, DashMap<String, String>);

impl Indexer {
    pub fn new(field_keys: DashMap<String, String>) -> Self {
        Self { field_keys }
    }

    fn index_worker(postings_writers: Arc<DashMap<String, Postings>>,
                    lengths_writers: Arc<DashMap<String, Lengths>>,
                    index_keys: Vec<String>,
                    docid: String,
                    fields_text: DashMap<String, String>) {
        let mut doc_id_bytes: [u8; 128] = [0; 128];

        for (byte_i, docid_byte) in docid.as_bytes().iter().enumerate() {
            doc_id_bytes[byte_i] = *docid_byte;
        }

        let docid = match std::str::from_utf8(&doc_id_bytes) {
            Ok(v) => v.to_string(),
            Err(e) => panic!("Invalid UTF-8 sequence: {}", e),
        };

        for index_key in &(*index_keys) {
            if let Some(field_text) = fields_text.get(index_key) {
                let tokens = tokenizer::tokenize(&field_text)
                    .iter()
                    .map(|t| t.clean())
                    .collect::<Vec<String>>();

                for token in tokens {
                    // Get the PostingsWriter for this field
                    postings_writers
                        .get_mut(index_key)
                        .unwrap()
                        // And count the token for the docid
                        .add_token_to_docid(&docid, &token);
                }

                lengths_writers
                    .get_mut(index_key)
                    .unwrap()
                    .add_length(docid.clone(), field_text.len() as u64);
            }
        }
    }

    pub fn index(&mut self, iter: impl Iterator<Item = Option<(String, DashMap<String, String>)>>) {
        let postings_writers = Arc::new(DashMap::new());
        let length_writers = Arc::new(DashMap::new());
        let mut avg_lengths_writer = Avglengths::new();

        let mut index_keys = Vec::new();

        // TODO fix horrible clone
        for (_, index_key) in self.field_keys.clone().into_iter() {
            postings_writers.insert(index_key.clone(), Postings::new(index_key.clone()));
            length_writers.insert(index_key.clone(), Lengths::new(index_key.clone()));

            index_keys.push(index_key);
        }

        let mut n_docs: usize = 0;

        let (jobs_channel_send, jobs_channel_send_recv) = bounded::<Option<IndexJob>>(num_cpus::get());
        let (jobs_channel_finish_send, jobs_channel_finish_recv) = bounded::<Option<u8>>(num_cpus::get());

        let mut handles = Vec::new();

        for _ in 0..num_cpus::get() {
            let (_, jobs_channel_recv_clone) = (jobs_channel_send.clone(), jobs_channel_send_recv.clone());
            let (jobs_channel_finish_send_clone, _) = (jobs_channel_finish_send.clone(), jobs_channel_finish_recv.clone());

            let ik = index_keys.clone();
            let c = postings_writers.clone();
            let l = length_writers.clone();

            handles.push(thread::spawn(move || {
                loop {
                    if let Some(job) = jobs_channel_recv_clone.recv().unwrap() {
                        Indexer::index_worker(c.clone(), l.clone(), ik.clone(), job.0, job.1);
                    } else {
                        jobs_channel_finish_send_clone.send(None).expect("TODO: panic message");
                        break;
                    }
                }
            }));
        }

        for (docid, fields_text) in iter.flatten() {
            n_docs += 1;

            println!("Indexed document {}", n_docs);

            jobs_channel_send.send(Some((docid, fields_text))).unwrap();
        }

        for _ in 0..num_cpus::get() {
            jobs_channel_send.send(None).unwrap();
            jobs_channel_finish_recv.recv().unwrap();
        }

        for _ in 0..num_cpus::get() {
            handles
                .pop().unwrap()
                .join().unwrap();
        }

        for index_key in index_keys.iter() {
            postings_writers.get_mut(index_key).unwrap().write_postings();
            let avg_length = length_writers.get_mut(index_key).unwrap().write_lengths();

            avg_lengths_writer.add_avg_length(index_key.clone(), avg_length);
            avg_lengths_writer.write_avg_lengths();
        }

        let stats_writer = Stats::new(n_docs);
        stats_writer.write_stats();
    }
}
