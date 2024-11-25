use crate::{unixfs, Config, LeafPolicy};

use bytes::{Bytes, BytesMut};
use libipld::{
	cid::{
		multibase::Base,
		multihash::{Code, MultihashDigest},
	},
	pb::{PbLink, PbNode},
	Cid,
};
use prost::Message;
use std::{
	collections::BTreeMap,
	io::Read,
	path::{Path, PathBuf},
};
use tracing::{debug, trace};

const DAG_PB: u64 = 0x70;
const RAW: u64 = 0x55;

#[derive(Default)]
pub struct UnixFsBuilder {
	config: Config,
	files: BTreeMap<PathBuf, Box<dyn Read>>,
}

impl UnixFsBuilder {
	pub fn config(mut self, config: Config) -> Self {
		self.config = config;
		self
	}

	pub fn add_file<P: AsRef<Path>>(mut self, reader: impl Read + 'static, path: P) -> Self {
		self.files.insert(path.as_ref().to_path_buf(), Box::new(reader));
		self
	}

	pub fn into_cid(self) -> Cid {
		match self.config.leaf_policy {
			// LeafPolicy::UnixFs => self.to_cid_unixfs(),
			LeafPolicy::Raw => self.into_cid_raw(),
		}
	}

	/*
	fn to_cid_unixfs(self) -> Cid{
		let data = Some(Bytes::from_static(b"Bitcoin whitepaper"));
		let root = PbNode { data, links: vec![] };
		let root_encoded = root.into_bytes();

		let cid_hash = Code::Sha2_256.digest(&root_encoded);
		Cid::new_v1(DAG_PB, cid_hash)
	}
	*/

	fn into_cid_raw(self) -> Cid {
		let chunk_size: usize = self.config.chunk_policy.into();
		let mut chunk = BytesMut::zeroed(chunk_size);

		let links = self
			.files
			.into_iter()
			.flat_map(|(path, mut reader)| {
				trace!(?path, "Processing file");
				let mut links = Vec::new();
				loop {
					let read_bytes = reader.read(chunk.as_mut()).unwrap();
					if read_bytes == 0 {
						break;
					}
					let cid = Cid::new_v1(RAW, Code::Sha2_256.digest(&chunk[..read_bytes]));
					trace!(
						?path,
						idx = links.len(),
						read_bytes,
						cid = cid.to_string_of_base(Base::Base32Lower).unwrap().as_str()
					);
					// NOTE: Name is empty string, to become compatible with https://dag.ipfs.tech
					let name = Some(String::new());
					links.push(PbLink { cid, name, size: Some(read_bytes as u64) });
				}
				links
			})
			.collect::<Vec<_>>();

		debug!(links_len = links.len(), "Root CID links count");
		match links.len() {
			0 => Cid::new_v1(RAW, Code::Sha2_256.digest(&[])),
			1 => links[0].cid,
			_ => {
				let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
				let filesize = blocksizes.iter().sum::<u64>();
				debug!(filesize, blocksizes = ?blocksizes, "Root CID filesize and blocksizes");
				let unixfs_file = unixfs::Data {
					r#type: unixfs::data::DataType::File as i32,
					filesize: Some(filesize),
					data: None,
					blocksizes,
					hash_type: None,
					fanout: None,
					mode: None,
					mtime: None,
				};
				let data: Bytes = unixfs_file.encode_to_vec().into();
				let root = PbNode { data: Some(data), links }.into_bytes();

				Cid::new_v1(DAG_PB, Code::Sha2_256.digest(&root))
			},
		}
	}
}

impl FromIterator<(PathBuf, Box<dyn Read>)> for UnixFsBuilder {
	fn from_iter<T: IntoIterator<Item = (PathBuf, Box<dyn Read>)>>(iter: T) -> Self {
		let files = iter.into_iter().collect();
		Self { files, ..Default::default() }
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		ConfigBuilder, LeafPolicy,
		WellKnownChunkSize::{self, F16KiB, F1KiB, F256KiB, F512B},
	};

	use std::{env, fs::File, io::BufReader, path::Path};
	use test_case::test_case;

	fn raw_conf(chunk_size: WellKnownChunkSize) -> Config {
		ConfigBuilder::default()
			.leaf_policy(LeafPolicy::Raw)
			.chunk_policy(chunk_size.into())
			.build()
			.unwrap()
	}

	fn test_file<P: AsRef<Path>>(relative_path: P) -> BufReader<File> {
		let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
		let path = Path::new(&manifest_dir).join("resources").join("tests").join(relative_path);
		let file = File::open(path).unwrap();
		BufReader::new(file)
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
		let file_with_reader_iter = files.iter().map(|p| {
			let reader: Box<dyn Read> = Box::new(test_file(p));
			(p.as_ref().to_path_buf(), reader)
		});
		let builder = UnixFsBuilder::from_iter(file_with_reader_iter).config(conf);

		builder.into_cid().to_string_of_base(Base::Base32Lower).unwrap()
	}
}
