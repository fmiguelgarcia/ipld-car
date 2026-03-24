use bytes::{Buf as _, Bytes};
use libipld::pb::PbNode;
use std::io::Read;

pub struct ReaderWithLen {
	pub reader: Box<dyn Read>,
	pub len: u64,
}

impl ReaderWithLen {
	pub fn new<R: Read + 'static>(reader: R, len: u64) -> Self {
		Self { reader: Box::new(reader), len }
	}
}

impl From<PbNode> for ReaderWithLen {
	fn from(pb_node: PbNode) -> Self {
		let enc_pb_node = Bytes::from(pb_node.into_bytes());
		let enc_pb_node_len = enc_pb_node.len() as u64;

		ReaderWithLen::new(enc_pb_node.reader(), enc_pb_node_len)
	}
}
