use crate::{
	unixfs,
	CidCodec::{DagPb, Raw},
	Config, FlatIterErr, FlatIterator, LeafPolicy, WithCid,
};

use bytes::Bytes;
use libipld::{
	multihash::{Code, MultihashDigest as _},
	pb::{PbLink, PbNode},
	Cid,
};
use prost::Message;
use std::{
	collections::HashSet as Set,
	fs::File,
	io::BufReader,
	path::{Path, PathBuf},
};
use thiserror_no_std::Error;

#[derive(Debug, Error)]
pub enum UnixFsBuilderErr {
	FlatIter(#[from] FlatIterErr),
	NoChunks,
	ChunkTooBig,
}

#[derive(Default)]
pub struct UnixFsBuilder {
	config: Config,
	paths: Set<PathBuf>,
}

impl UnixFsBuilder {
	pub fn config(mut self, config: Config) -> Self {
		self.config = config;
		self
	}

	pub fn add_file<P: AsRef<Path>>(mut self, path: P) -> Self {
		self.paths.insert(path.as_ref().to_path_buf());
		self
	}

	pub fn build_root_cid(self) -> Result<Cid, UnixFsBuilderErr> {
		debug_assert!(self.paths.len() < 2, "No more than one file allowed");
		assert!(self.config.leaf_policy == LeafPolicy::Raw, "Only Raw leaf policy supported");

		let chunk_size = self.config.chunk_policy.into();
		let chunker_with_cids = self
			.paths
			.into_iter()
			.map(|path| {
				let file = File::open(&path)?;
				let chunker_with_cid = WithCid::new(FlatIterator::new(BufReader::new(file), chunk_size));
				Ok::<_, FlatIterErr>(chunker_with_cid)
			})
			.collect::<Result<Vec<_>, _>>()?;

		// NOTE: Name is empty string, to become compatible with https://dag.ipfs.tech
		let empty_name = Some(String::new());
		let mut file_cids = chunker_with_cids
			.into_iter()
			.map(|chunker_with_cid| {
				let mut links = chunker_with_cid
					.map(|result_inner| {
						let (cid, chunk) = result_inner?;
						let size: u64 = chunk.len().try_into().map_err(|_| UnixFsBuilderErr::ChunkTooBig)?;
						let link = PbLink { cid, name: empty_name.clone(), size: Some(size) };
						Ok::<_, UnixFsBuilderErr>(link)
					})
					.collect::<Result<Vec<_>, _>>()?;

				let cid = match links.len() {
					0 => Cid::new_v1(Raw as u64, Code::Sha2_256.digest(&[])),
					1 => links.pop().expect("At least one link .qed").cid,
					_ => {
						let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
						let filesize = blocksizes.iter().sum::<u64>();
						let data: Bytes = unixfs::Data::file(filesize, blocksizes).encode_to_vec().into();
						let root = PbNode { data: Some(data), links }.into_bytes();
						Cid::new_v1(DagPb as u64, Code::Sha2_256.digest(&root))
					},
				};
				Ok::<_, UnixFsBuilderErr>(cid)
			})
			.collect::<Result<Vec<_>, _>>()?;
		debug_assert!(file_cids.len() == 1, "Only one file allowed");
		file_cids.pop().ok_or(UnixFsBuilderErr::NoChunks)
	}
}

impl<P> FromIterator<P> for UnixFsBuilder
where
	P: AsRef<Path>,
{
	fn from_iter<T: IntoIterator<Item = P>>(iter: T) -> Self {
		let paths = iter.into_iter().map(|p| p.as_ref().to_path_buf()).collect();
		Self { paths, ..Default::default() }
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		ConfigBuilder, LeafPolicy,
		WellKnownChunkSize::{self, F16KiB, F1KiB, F256KiB, F512B},
	};

	use libipld::multibase::Base;
	use std::{env, path::Path};
	use test_case::test_case;

	fn raw_conf(chunk_size: WellKnownChunkSize) -> Config {
		ConfigBuilder::default()
			.leaf_policy(LeafPolicy::Raw)
			.chunk_policy(chunk_size.into())
			.build()
			.unwrap()
	}

	fn test_file<P: AsRef<Path>>(relative_path: P) -> PathBuf {
		let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
		Path::new(&manifest_dir).join("resources").join("tests").join(relative_path)
	}

	// #[test_case(Config::default(), &["bitcoin.pdf"] => "bafybeihdkj2dgcrkbstkvwdp7fb75rnvlfokvpbgy6kcw43sfrohf2yoka";
	// "BTC whitepaper 16KiB UnixFs  Balanced 11 children")]
	#[test_case(raw_conf(F256KiB), &["empty.txt"] => "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"; "Empty 256KiB Raw Flat DAG")]
	#[test_case(raw_conf(F16KiB), &["empty.txt"] => "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"; "Empty 16KiB Raw Flat DAG")]
	#[test_case(raw_conf(F256KiB), &["bitcoin.pdf"] => "bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm"; "BTC whitepaper 256KiB Raw Flat DAG")]
	#[test_case(raw_conf(F16KiB), &["bitcoin.pdf"] => "bafybeibq5c6excift7kndpbnaar7s5eanqgexlaal7vw5r3j4uxns6xnbu"; "BTC whitepaper 16KiB Raw Flat DAG")]
	#[test_case(raw_conf(F1KiB), &["bitcoin.pdf"] => "bafybeiexwr3z65fewherchi63n4xbthpn6aft6t57d4pbmojhjknwepo34"; "BTC whitepaper 1KiB Raw Flat DAG")]
	#[test_case(raw_conf(F512B), &["bitcoin.pdf"] => "bafybeihbxyhevuhjvb4ctfqiglu7im7fqf7ghbfmheysp63npyuiggzfiu"; "BTC whitepaper 512KiB Raw Flat DAG")]
	#[test_log::test]
	fn test_add_files<P: AsRef<Path>>(conf: Config, files: &[P]) -> String {
		let builder = UnixFsBuilder::from_iter(files.iter().map(test_file)).config(conf);

		builder
			.build_root_cid()
			.expect("Failed to build root CID")
			.to_string_of_base(Base::Base32Lower)
			.unwrap()
	}
}
