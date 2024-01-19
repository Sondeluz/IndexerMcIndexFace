// File writers
mod postings;
mod lengths;
mod avg_lengths;
pub(crate) mod stats; // The only writer intended to be exposed

// Main indexer implementation
mod indexer;

pub use self::indexer::Indexer as Indexer;