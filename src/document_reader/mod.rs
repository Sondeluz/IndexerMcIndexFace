use anyhow::{anyhow, Context, Result};
use serde_json::Value;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::fs::DirEntry;
use std::io::Read;
use std::path::PathBuf;

/// Document reader/parser for JSON files, which will index all .json files in a given directory.
/// It allows setting aliases for the field keys to be parsed from the files
pub struct DocumentReader {
    field_keys: HashMap<String, String>,
    docs_directory: String,
}

const MAX_DOCID_LENGTH: usize = 32;
const DOCID_FIELD_NAME: &str = "docid";

impl DocumentReader {
    pub fn new(field_keys: HashMap<String, String>, docs_directory: String) -> Self {
        DocumentReader {
            field_keys,
            docs_directory,
        }
    }

    /// Returns a document processing iterator over the folder, which allows the indexer to lazily extract new documents
    /// The iterator may return None values if the document was not valid or an error occurred when processing it
    pub fn process_documents(
        &self,
    ) -> Result<impl Iterator<Item = Option<(String, HashMap<String, String>)>> + '_> {
        let dir_entries = fs::read_dir(&self.docs_directory)
            .with_context(|| format!("Failed to read directory {:?}", self.docs_directory))?;

        let iterator = dir_entries.map(move |entry| match entry {
            Ok(entry) => {
                if Self::is_indexable_file(&entry) {
                    if let Ok(processed_indexable_file) = self.process_json_file(entry.path()) {
                        Some(processed_indexable_file)
                    } else {
                        eprintln!(
                            "Warning: failed to process indexable file: {}",
                            entry.path().display()
                        );
                        None
                    }
                } else {
                    None
                }
            }
            Err(error) => {
                eprintln!("Warning: failed to retrieve directory entry: {}", error);
                None
            }
        });

        Ok(iterator)
    }

    fn is_indexable_file(entry: &DirEntry) -> bool {
        entry.path().is_file() && entry.path().extension() == Some(OsStr::new("json"))
    }

    fn process_json_file(&self, path: PathBuf) -> Result<(String, HashMap<String, String>)> {
        let json = Self::file_to_json(&path)?;

        match json.get(DOCID_FIELD_NAME) {
            Some(docid_json_value) => {
                let docid = docid_json_value.as_str().unwrap().to_string();

                if docid.len() <= MAX_DOCID_LENGTH {
                    let mut fields_text = HashMap::new();
                    for (field_key, index_key) in self.field_keys.iter() {
                        if let Some(text) = json.get(field_key) {
                            fields_text
                                .insert(index_key.to_string(), text.as_str().unwrap().to_string());
                        }
                    }

                    Ok((docid, fields_text))
                } else {
                    Err(anyhow!(format!(
                        "docid length exceeded for document {}",
                        path.display()
                    )))
                }
            }
            None => Err(anyhow!(format!(
                "Could not find docid in {}",
                path.display()
            ))),
        }
    }

    /// Converts a file to a valid JSON string
    fn file_to_json(path: &PathBuf) -> Result<Value, anyhow::Error> {
        let mut file =
            fs::File::open(path).with_context(|| format!("Failed to open file {:?}", path))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .with_context(|| format!("Failed to read file {:?}", path))?;

        let json: Value = serde_json::from_str(&contents)
            .with_context(|| format!("Failed to parse JSON in file {:?}", path))?;
        Ok(json)
    }
}
