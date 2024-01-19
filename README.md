# A (toy) low-level document indexing and retrieval system 
IndexerMcIndexFace is a tiny traditional document indexing and retrieval system that I wrote 
as an excuse to play with FSTs (using the `BurntSushi/fst` crate) and Rust's parallelization 
capabilities (using also the `crossbeam` crate for message passing)

## Features:
- Fully written in Rust
- Uses FSTs for fast access to postings
- Allows fielded documents, and uses the `BM25F` retrieval model (note: I didn't verify its correctness)
- The indexing stage is paralellized with a threadpool by creating and merging independent indexes
  - (Note that this is a naive implementation, and although it's extremely fast it can be really memory hungry)
- The retrieval stage is parallelized with a threadpool, where in this case it runs a different search for every token

## Warnings:
- This is a toy project (e.g: index files are not compressed, the parallelization techniques are naive and resource-hungry...) 
  and the API is very basic.

## Usage:
- Simply run `cargo run --release`. `main.rs` will create a dummy collection of 1000 files using the
  `MitchellRhysHall/random_word` crate, and then will index and perform a randomised moderately sized query.

## Possible improvements:
- The use of FSTs opens up many possibilities, as regex-like searches can be easily performed.
- Better parallelization techniques: Right now, each thread will create its own in-memory index, which will
  be later joined and written to binary files. This means that the memory usage can be very high for bigger
  collections of documents.
- Better tokenizers.
- N-gram or similar, more elaborate, indexes.
- Alternative retrieval models, phrase queries, etc.
