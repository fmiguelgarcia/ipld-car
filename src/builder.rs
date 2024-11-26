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
use quick_protobuf::{message::MessageWrite, Writer};
use std::{
	collections::HashMap as Map,
	fs::File,
	io::{BufReader, Read},
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
	paths: Map<PathBuf, Box<dyn Read>>,
}

impl UnixFsBuilder {
	pub fn config(mut self, config: Config) -> Self {
		self.config = config;
		self
	}

	pub fn add_file<F, T>(mut self, from: F, to: T) -> Result<Self, std::io::Error>
	where
		F: AsRef<Path>,
		T: AsRef<Path>,
	{
		let file = BufReader::new(File::open(from.as_ref())?);
		self.paths.insert(to.as_ref().to_path_buf(), Box::new(file));
		Ok(self)
	}

	pub fn add_data<D, T>(mut self, data: D, to: T) -> Self
	where
		T: AsRef<Path>,
		D: Read + 'static,
	{
		self.paths.insert(to.as_ref().to_path_buf(), Box::new(data));
		self
	}

	pub fn build(self) -> Result<(Cid, Option<PbNode>), UnixFsBuilderErr> {
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

				let (cid, pb_node) = match links.len() {
					0 => (Cid::new_v1(Raw as u64, Code::Sha2_256.digest(&[])), None),
					1 => (links.pop().expect("At least one link .qed").cid, None),
					_ => {
						let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
						let filesize = blocksizes.iter().sum::<u64>();
						let data: Bytes = unixfs::Data::file(filesize, blocksizes).encode_to_vec().into();

						let pb_node = build_pb_node(links, Some(data));
						let pb_node_data = encode_pb_node(&pb_node);
						let cid = Cid::new_v1(DagPb as u64, Code::Sha2_256.digest(&pb_node_data));
						(cid, Some(pb_node))
					},
				};

				Ok::<_, UnixFsBuilderErr>((cid, pb_node))
			})
			.collect::<Result<Vec<_>, _>>()?;
		debug_assert!(file_cids.len() == 1, "Only one file allowed");
		file_cids.pop().ok_or(UnixFsBuilderErr::NoChunks)
	}

	pub fn only_root_cid(self) -> Result<Cid, UnixFsBuilderErr> {
		self.build().map(|(cid, _)| cid)
	}
}

/// Build a PBNode from the links and data.
fn build_pb_node(mut links: Vec<PbLink>, data: Option<Bytes>) -> PbNode {
	// Links must be strictly sorted by name before encoding, leaving stable
	// ordering where the names are the same (or absent).
	links.sort_by(|a, b| {
		let a = a.name.as_ref().map(|s| s.as_bytes()).unwrap_or(&[][..]);
		let b = b.name.as_ref().map(|s| s.as_bytes()).unwrap_or(&[][..]);
		a.cmp(b)
	});

	PbNode { data, links }
}

fn encode_pb_node(pb_node: &PbNode) -> Bytes {
	let mut buf = Vec::with_capacity(pb_node.get_size());
	let mut writer = Writer::new(&mut buf);

	pb_node.write_message(&mut writer).expect("Protobuf is valid .qed");
	buf.into()
}

/*
impl<I, P> TryFrom<I> for UnixFsBuilder
where
	I: Iterator<Item = P>,
	P: AsRef<Path>,
{
	type Error = std::io::Error;

	fn try_from(paths: I) -> Result<Self, Self::Error> {
		let mut builder = Self::default();
		for path in paths.map(|path| path.as_ref()) {
			// .filter(Path::is_file) {
			let file_name = path.file_name().expect("File name is not empty .qed");
			builder = builder.add_file(path, file_name)?;
		}
		Ok(builder)
	}
}*/

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
		let mut builder = UnixFsBuilder::default().config(conf);
		for file in files.iter().map(test_file).filter(|file| file.is_file()) {
			let name = PathBuf::from(file.file_name().expect("File name is not empty .qed"));
			builder = builder.add_file(file, name).expect("Valid file .qed");
		}

		builder
			.only_root_cid()
			.expect("Failed to build root CID")
			.to_string_of_base(Base::Base32Lower)
			.unwrap()
	}
}
