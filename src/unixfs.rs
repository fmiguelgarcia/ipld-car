use derive_more::Constructor;
use libipld::{pb::PbNode, Cid};
use std::io::{Read, Seek};

pub mod builder;
pub mod proto;
pub use builder::UnixFsBuilder;

pub trait SeekableRead: Read + Seek {}

impl<T: Read + Seek> SeekableRead for T {}

#[derive(Constructor)]
pub struct UnixFs {
	pub cid: Cid,
	pub pb_node: Option<PbNode>,
	pub package_len: u64,
	pub reader: Box<dyn SeekableRead>,
}
