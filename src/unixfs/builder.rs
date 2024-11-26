use crate::{
	unixfs::{proto, SeekableRead, UnixFs},
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
	io::{BufReader, Error as IoError, Read, Seek},
	path::{Path, PathBuf},
};
use thiserror_no_std::Error;

#[derive(Debug, Error)]
pub enum UnixFsBuilderErr {
	FlatIter(#[from] FlatIterErr),
	Io(#[from] IoError),
	NoChunks,
	ChunkTooBig,
	PackageTooBig,
}

#[derive(Default)]
pub struct UnixFsBuilder {
	config: Config,
	paths: Map<PathBuf, Box<dyn SeekableRead>>,
}

impl UnixFsBuilder {
	pub fn config(mut self, config: Config) -> Self {
		self.config = config;
		self
	}

	fn base_add_file<F, T>(&mut self, from: F, to: T) -> Result<(), IoError>
	where
		F: AsRef<Path>,
		T: AsRef<Path>,
	{
		let file = BufReader::new(File::open(from.as_ref())?);
		self.paths.insert(to.as_ref().to_path_buf(), Box::new(file));
		Ok(())
	}

	pub fn add_file<F, T>(mut self, from: F, to: T) -> Result<Self, std::io::Error>
	where
		F: AsRef<Path>,
		T: AsRef<Path>,
	{
		self.base_add_file(from, to)?;
		Ok(self)
	}

	pub fn add_files<I, F, T>(mut self, files: I) -> Result<Self, std::io::Error>
	where
		I: IntoIterator<Item = (F, T)>,
		F: AsRef<Path>,
		T: AsRef<Path>,
	{
		for (from, to) in files {
			self.base_add_file(from, to)?;
		}
		Ok(self)
	}

	pub fn add_data<D, T>(mut self, data: D, to: T) -> Self
	where
		T: AsRef<Path>,
		D: Read + Seek + 'static,
	{
		self.paths.insert(to.as_ref().to_path_buf(), Box::new(data));
		self
	}

	pub fn build(self) -> Result<UnixFs, UnixFsBuilderErr> {
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
			.map(|mut chunker_with_cid| {
				let mut links = chunker_with_cid.try_fold(vec![], |mut links, result_inner| {
					let (cid, chunk) = result_inner?;
					let size: u64 = chunk.len().try_into().map_err(|_| UnixFsBuilderErr::ChunkTooBig)?;
					let link = PbLink { cid, name: empty_name.clone(), size: Some(size) };
					links.push(link);
					Ok::<_, UnixFsBuilderErr>(links)
				})?;

				// Recover the original reader, and rewind it to the beginning.
				let mut reader = chunker_with_cid.into_inner().into_inner();
				reader.rewind()?;

				let (cid, pb_node, package_len) = match links.len() {
					0 => (Cid::new_v1(Raw as u64, Code::Sha2_256.digest(&[])), None, 0),
					1 => {
						let link = links.pop().expect("At least one link .qed");
						(link.cid, None, link.size.unwrap_or_default())
					},
					_ => {
						let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
						let filesize = blocksizes.iter().sum::<u64>();
						let data: Bytes = proto::Data::file(filesize, blocksizes).encode_to_vec().into();

						let pb_node = build_pb_node(links, Some(data));
						let pb_node_data = encode_pb_node(&pb_node);
						let package_len =
							filesize.checked_add(pb_node_data.len() as u64).ok_or(UnixFsBuilderErr::PackageTooBig)?;
						let cid = Cid::new_v1(DagPb as u64, Code::Sha2_256.digest(&pb_node_data));

						(cid, Some(pb_node), package_len)
					},
				};

				Ok::<_, UnixFsBuilderErr>((cid, pb_node, package_len, reader))
			})
			.collect::<Result<Vec<_>, _>>()?;
		debug_assert!(file_cids.len() == 1, "Only one file allowed");

		let (cid, maybe_pb_node, package_len, reader) = file_cids.pop().ok_or(UnixFsBuilderErr::NoChunks)?;
		Ok(UnixFs::new(cid, maybe_pb_node, package_len, reader))
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
		let from_to = files.iter().map(|file| (test_file(file), file.as_ref()));
		let file = UnixFsBuilder::default()
			.config(conf)
			.add_files(from_to)
			.expect("files are valid .qed")
			.build()
			.expect("Valid UnixFs .qed");

		let cid = file.cid;
		cid.to_string_of_base(Base::Base32Lower).unwrap()
	}
}
