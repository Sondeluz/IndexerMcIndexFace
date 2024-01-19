mod aux;
mod document_reader;
mod indexing;
mod retrieval;
mod tokenizer;

use crate::document_reader::*;
use crate::indexing::Indexer;
use anyhow::{Context, Result};
use lipsum::lipsum_words_with_rng;
use rand::{random, Rng};
use serde::Serialize;
use std::collections::HashMap;
use random_word::Lang;

#[derive(Serialize)]
struct DummyFile {
    docid: String,
    field1: String,
    field2: String,
}

fn get_random_doc_id() -> String {
    let possible_chars: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789";
    let mut rng = rand::thread_rng();

    (0..32)
        .map(|_| {
            let idx = rng.gen_range(0..possible_chars.len());
            possible_chars[idx] as char
        })
        .collect()
}

fn generate_files_to_index(n_files: usize) {
    let mut rng = rand::thread_rng();

    let mut tokens_field_1 : Vec<&str> = Vec::new();
    for _ in 0..rng.gen_range(0..=25000) {
        tokens_field_1.push(random_word::gen(Lang::En));
    }

    let mut tokens_field_2 : Vec<&str> = Vec::new();
    for _ in 0..rng.gen_range(0..=25000) {
        tokens_field_2.push(random_word::gen(Lang::En));
    }

    for i in 0..n_files {
        let doc_id = get_random_doc_id();
        let data = DummyFile {
            docid: doc_id.clone(),
            field1: tokens_field_1.join(" "),
            field2: tokens_field_2.join(" "),
        };

        let json = serde_json::to_string_pretty(&data).expect("Failed to serialize data");

        std::fs::write(format!("documents/{}.json", doc_id), json).expect("Failed to write file");
    }
}

fn main() -> Result<()> {
    println!("Generating random document collection of size 5000...");
    generate_files_to_index(5000);

    let mut field_keys = HashMap::new(); // Document field name -> Index field name
    field_keys.insert("field1".to_string(), "field1_index_name".to_string());
    field_keys.insert("field2".to_string(), "field2_index_name".to_string());
    let doc_reader = DocumentReader::new(field_keys.clone(), "./documents".to_string());
    let docs_iter = doc_reader.process_documents().unwrap();

    let mut indexer = Indexer::new(field_keys.clone());
    indexer.index(docs_iter)
        .with_context(|| "Error during indexing:")?;

    let mut field_weights = HashMap::new();
    let mut field_k1s = HashMap::new();
    let mut field_bs = HashMap::new();
    field_weights.insert("field1_index_name".to_string(), 1.0);
    field_k1s.insert("field1_index_name".to_string(), 1.2);
    field_bs.insert("field1_index_name".to_string(), 0.75);

    field_weights.insert("field2_index_name".to_string(), 0.5);
    field_k1s.insert("field2_index_name".to_string(), 1.2);
    field_bs.insert("field2_index_name".to_string(), 0.75);

    let retriever = retrieval::Retriever::new(field_keys.into_values().collect())
        .with_context(|| "Error during retriever initialization:")?;

    let mut query_tokens : Vec<String> = Vec::new();
    for _ in 0..rand::thread_rng().gen_range(0..=25000) {
        query_tokens.push(random_word::gen(Lang::En).to_string());
    }

    let results = retriever.retrieval_multiple_tokens(
            &query_tokens,
            &field_k1s,
            &field_bs,
            &field_weights
        ).with_context(|| "Error during retrieval:")?;

    println!(
        "Top five results: {:?}",
        results[..5].to_vec()
    );

    Ok(())
}
