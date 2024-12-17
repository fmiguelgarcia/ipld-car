use crate::{
	unixfs::{pb, proto, UnixFs},
	CidCodec::{DagPb, Raw},
	Config, FlatIterErr, FlatIterator, LeafPolicy, PbLink, PbNode, WithCid,
};

use libipld::{
	multihash::{Code, MultihashDigest as _},
	Cid,
};
use prost::Message;
use std::{
	collections::HashMap as Map,
	io::{self, Read},
	path::{Path, PathBuf},
};
use thiserror_no_std::Error;

#[derive(Debug, Error)]
pub enum FileSystemWriterError {
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	FlatIter(#[from] FlatIterErr),
	#[error("Chunk size cannot be converted into u64")]
	ChunkTooBig,
	#[error("No chunks were created")]
	NoChunks,
	#[error("Package too big to allocated into a UnixFs")]
	PackageTooBig,
}

pub struct FileSystemWriter<R> {
	config: Config,
	paths: Map<PathBuf, R>,
}

impl<R> FileSystemWriter<R> {
	pub fn config(mut self, config: Config) -> Self {
		self.config = config;
		self
	}

	fn base_add_data<T: AsRef<Path>>(&mut self, reader: R, to: T) {
		self.paths.insert(to.as_ref().to_path_buf(), reader);
	}

	pub fn add_data<T>(mut self, reader: R, to: T) -> Self
	where
		T: AsRef<Path>,
	{
		self.base_add_data(reader, to);
		self
	}

	pub fn add_datas<I, T>(mut self, datas: I) -> Self
	where
		I: IntoIterator<Item = (R, T)>,
		T: AsRef<Path>,
	{
		datas.into_iter().for_each(|(reader, to)| self.base_add_data(reader, to));
		self
	}
}

impl<R> FileSystemWriter<R>
where
	R: Read,
{
	pub fn build(self) -> Result<UnixFs<R>, FileSystemWriterError> {
		debug_assert!(self.paths.len() < 2, "No more than one file allowed");
		assert!(self.config.leaf_policy == LeafPolicy::Raw, "Only Raw leaf policy supported");

		let chunk_size = self.config.chunk_policy.into();
		let chunker_with_cids = self
			.paths
			.into_values()
			.map(|reader| {
				let chunker_with_cid = WithCid::new(FlatIterator::new(reader, chunk_size));
				Ok::<_, FlatIterErr>(chunker_with_cid)
			})
			.collect::<Result<Vec<_>, _>>()?;

		let mut file_cids = chunker_with_cids
			.into_iter()
			.map(|mut chunker_with_cid| {
				let links = chunker_with_cid.try_fold(vec![], |mut links, result_inner| {
					let (cid, chunk) = result_inner?;
					let size: u64 = chunk.len().try_into().map_err(|_| FileSystemWriterError::ChunkTooBig)?;
					let link = pb::link::new(cid, size);
					links.push(link);
					Ok::<_, FileSystemWriterError>(links)
				})?;

				// Recover the original reader
				let data_reader = chunker_with_cid.into_inner().into_inner();

				let (cid, maybe_node, data_len) = from_links(links);
				Ok::<_, FileSystemWriterError>((cid, maybe_node, data_reader, data_len))
			})
			.collect::<Result<Vec<_>, _>>()?;
		debug_assert!(file_cids.len() == 1, "Only one file allowed");

		let (cid, maybe_node, data_reader, data_len) = file_cids.pop().ok_or(FileSystemWriterError::NoChunks)?;

		UnixFs::new(cid, maybe_node, data_reader, data_len).ok_or(FileSystemWriterError::PackageTooBig)
	}
}

impl<R> Default for FileSystemWriter<R> {
	fn default() -> Self {
		Self { config: Config::default(), paths: Map::new() }
	}
}

impl<R, P> FromIterator<(R, P)> for FileSystemWriter<R>
where
	P: AsRef<Path>,
{
	fn from_iter<I>(iter: I) -> Self
	where
		I: IntoIterator<Item = (R, P)>,
	{
		let mut this = Self::default();
		iter.into_iter().for_each(|(reader, to)| this.base_add_data(reader, to));
		this
	}
}

fn from_links(mut links: Vec<PbLink>) -> (Cid, Option<PbNode>, u64) {
	match links.len() {
		0 => (Cid::new_v1(Raw.into(), Code::Sha2_256.digest(&[])), None, 0),
		1 => {
			let link = links.pop().expect("At least one link .qed");
			(link.cid, None, link.size.unwrap_or_default())
		},
		_ => {
			// Add DAG PB node with UnixFS::File as data.
			let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
			let data = proto::Data::file_in_blocks(blocksizes);
			let file_size = data.filesize.expect("Filesize is set .qed");

			let node = pb::node::new(links, data.encode_to_vec());
			let node_enc = pb::node::encode(&node);
			let cid = Cid::new_v1(DagPb.into(), Code::Sha2_256.digest(&node_enc));

			(cid, Some(node), file_size)
		},
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		test_helpers::{raw_conf, test_file},
		WellKnownChunkSize::{F16KiB, F1KiB, F256KiB, F512B},
	};

	use libipld::multibase::Base;
	use std::path::Path;
	use test_case::test_case;

	// #[test_case(Config::default(), &["bitcoin.pdf"] => "bafybeihdkj2dgcrkbstkvwdp7fb75rnvlfokvpbgy6kcw43sfrohf2yoka";
	// "BTC whitepaper 16KiB UnixFs  Balanced 11 children")]
	#[test_case(raw_conf(F256KiB), &["empty.txt"] => "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"; "Empty 256KiB Raw Flat DAG")]
	#[test_case(raw_conf(F16KiB), &["empty.txt"] => "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"; "Empty 16KiB Raw Flat DAG")]
	#[test_case(raw_conf(F256KiB), &["bitcoin.pdf"] => "bafkreifrm5azdkeoyxg5om7eeqfidabraxoecllmm4enkovzj7ber5hvkm"; "BTC whitepaper 256KiB Raw Flat DAG")]
	#[test_case(raw_conf(F16KiB), &["bitcoin.pdf"] => "bafybeibq5c6excift7kndpbnaar7s5eanqgexlaal7vw5r3j4uxns6xnbu"; "BTC whitepaper 16KiB Raw Flat DAG")]
	#[test_case(raw_conf(F1KiB), &["bitcoin.pdf"] => "bafybeiexwr3z65fewherchi63n4xbthpn6aft6t57d4pbmojhjknwepo34"; "BTC whitepaper 1KiB Raw Flat DAG")]
	#[test_case(raw_conf(F512B), &["bitcoin.pdf"] => "bafybeihbxyhevuhjvb4ctfqiglu7im7fqf7ghbfmheysp63npyuiggzfiu"; "BTC whitepaper 512KiB Raw Flat DAG")]
	// #[test_log::test]
	fn test_add_files<P: AsRef<Path>>(conf: Config, files: &[P]) -> String {
		let datas = files.iter().map(|file| (test_file(file), file.as_ref()));
		let file = FileSystemWriter::default().config(conf).add_datas(datas).build().expect("Valid UnixFs .qed");

		let cid = file.cid;
		cid.to_string_of_base(Base::Base32Lower).unwrap()
	}
}
