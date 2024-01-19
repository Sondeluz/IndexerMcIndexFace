// Main indexing process with a naive multi-threaded implementation (create per-thread postings and lengths, then merge
// them). This can cause OOMs if the collection is massively large

use crate::indexing::avg_lengths::Avglengths;
use crate::indexing::lengths::Lengths;
use crate::indexing::postings::Postings;
use crate::indexing::stats::Stats;
use crate::tokenizer;
use anyhow::{Result};
use crossbeam_channel::bounded;
use std::collections::HashMap;
use std::thread;
use std::time::Duration;
use indicatif::{ProgressBar, ProgressStyle};

pub struct Indexer {
    field_keys: HashMap<String, String>,
}

type IndexJob = (String, HashMap<String, String>);
type IndexResults = (HashMap<String, Postings>, HashMap<String, Lengths>);

impl Indexer {
    pub fn new(field_keys: HashMap<String, String>) -> Self {
        Self { field_keys }
    }

    fn index_worker_function(
        postings_writers: &mut HashMap<String, Postings>,
        lengths_writers: &mut HashMap<String, Lengths>,
        index_keys: Vec<String>,
        docid: String,
        fields_text: HashMap<String, String>,
    ) {
        // Only take the first 32 characters from the docid
        let mut doc_id_bytes: [u8; 32] = [0; 32];

        for (byte_i, docid_byte) in docid.as_bytes().iter().enumerate() {
            doc_id_bytes[byte_i] = *docid_byte;
        }

        let docid = std::str::from_utf8(&doc_id_bytes).unwrap().to_string();

        for index_key in &(*index_keys) {
            if let Some(field_text) = fields_text.get(index_key) {
                let tokens = tokenizer::tokenize(field_text)
                    .iter()
                    .map(|t| t.clean())
                    .collect::<Vec<String>>();

                for token in &tokens {
                    // Get the PostingsWriter for this field
                    postings_writers
                        .get_mut(index_key)
                        .unwrap()
                        // And count the token for the docid
                        .add_token_to_docid(&docid, token);
                }

                lengths_writers
                    .get_mut(index_key)
                    .unwrap()
                    .add_length(docid.clone(), tokens.len() as u64);
            }
        }
    }

    pub fn index(&mut self, docs_iter: impl Iterator<Item = Option<(String, HashMap<String, String>)>>) -> Result<()> {
        let mut index_keys = Vec::new();
        for (_, index_key) in self.field_keys.clone().into_iter() {
            index_keys.push(index_key);
        }

        let (jobs_channel_send, jobs_channel_send_recv) =
            bounded::<Option<IndexJob>>(num_cpus::get());
        let (jobs_channel_finish_send, jobs_channel_finish_recv) =
            bounded::<IndexResults>(num_cpus::get());

        let mut handles = Vec::new();

        // Every worker will receive documents and keep local in-memory postings and lengths, which will be
        // returned upon receiving the finish signal
        for _ in 0..num_cpus::get() {
            let (_, jobs_channel_recv_clone) =
                (jobs_channel_send.clone(), jobs_channel_send_recv.clone());
            let (jobs_channel_finish_send_clone, _) = (
                jobs_channel_finish_send.clone(),
                jobs_channel_finish_recv.clone(),
            );

            let ik = index_keys.clone();
            //let c = postings_writers.clone();
            //let l = length_writers.clone();

            handles.push(thread::spawn(move || {
                let mut postings_writers = HashMap::new();
                let mut lengths_writers = HashMap::new();

                for index_key in &ik {
                    postings_writers.insert(index_key.clone(), Postings::new(index_key.clone()));
                    lengths_writers.insert(index_key.clone(), Lengths::new(index_key.clone()));
                }

                loop {
                    if let Some(job) = jobs_channel_recv_clone.recv().unwrap() {
                        Indexer::index_worker_function(
                            &mut postings_writers,
                            &mut lengths_writers,
                            ik.clone(),
                            job.0,
                            job.1,
                        );
                    } else {
                        jobs_channel_finish_send_clone
                            .send((postings_writers, lengths_writers))
                            .unwrap();
                        break;
                    }
                }
            }));
        }

        println!("Starting indexing...");
        let fancy_spinner = ProgressBar::new_spinner();
        fancy_spinner.enable_steady_tick(Duration::from_millis(120));
        fancy_spinner.set_style(
            ProgressStyle::with_template("{spinner:.red} {msg}")
                .unwrap()
                .tick_strings(&[
                    "▹▹▹▹▹",
                    "▸▹▹▹▹",
                    "▹▸▹▹▹",
                    "▹▹▸▹▹",
                    "▹▹▹▸▹",
                    "▹▹▹▹▸",
                    "▪▪▪▪▪",
                ]),
        );

        let mut n_docs: usize = 0;
        for (docid, fields_text) in docs_iter.flatten() {
            // Since the queue is bounded, it's a decent progress approximation
            n_docs += 1;
            fancy_spinner.set_message(format!("Indexed document {n_docs}"));

            jobs_channel_send.send(Some((docid, fields_text)))?;
        }
        fancy_spinner.finish_with_message("All documents processed! Writing index files...");

        // Join all worker postings and lengths and write them
        let mut avg_lengths_writer = Avglengths::new();
        let mut postings_writers = HashMap::new();
        let mut lengths_writers = HashMap::new();

        for index_key in &index_keys {
            postings_writers.insert(index_key.clone(), Postings::new(index_key.clone()));
            lengths_writers.insert(index_key.clone(), Lengths::new(index_key.clone()));
        }

        for _ in 0..num_cpus::get() {
            jobs_channel_send.send(None)?;
            let (worker_postings_writers, worker_lengths_writers) =
                jobs_channel_finish_recv.recv()?;

            for (index_key, postings) in postings_writers.iter_mut() {
                let worker_postings = worker_postings_writers.get(index_key).unwrap();
                postings.add_postings(worker_postings);

                let worker_lengths = worker_lengths_writers.get(index_key).unwrap();

                lengths_writers
                    .get_mut(index_key)
                    .unwrap()
                    .add_lengths(&worker_lengths);
            }
        }

        for _ in 0..num_cpus::get() {
            handles.pop().unwrap().join().unwrap();
        }

        for index_key in index_keys.iter() {
            postings_writers
                .get_mut(index_key)
                .unwrap()
                .write_postings()?;
            let avg_length = lengths_writers.get_mut(index_key).unwrap().write_lengths()?;

            avg_lengths_writer.add_avg_length(index_key.clone(), avg_length);
            avg_lengths_writer.write_avg_lengths()?;
        }

        let stats_writer = Stats::new(n_docs);
        stats_writer.write_stats()?;

        Ok(())
    }
}
