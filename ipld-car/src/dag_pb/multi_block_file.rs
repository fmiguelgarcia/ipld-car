use derive_more::{Constructor, From};

#[derive(Debug, Constructor, Clone, From)]
pub struct MultiBlockFile {
	pub block_sizes: Vec<u64>,
}
