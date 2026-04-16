use crate::{
	bounded_reader::traits::{Bounded as _, CloneAndRewind as _},
	car::{tools::write_block, BlockType, CarHeader, ContentAddressableArchive},
	error::{Error, Result, NODE_IDX_QED},
	traits::AsCIDGraph as _,
};

use bytes::{Buf as _, Bytes};
use std::io::{BufWriter, Read, Seek, Write};
use tempfile::NamedTempFile;

impl<T: Read + Seek + 'static> ContentAddressableArchive<T> {
	pub fn write<W: Write>(&mut self, writer: &mut W) -> Result<u64> {
		let mut acc_written = 0u64;

		// Write header
		let root_cids = self.root_cids().copied().collect::<Vec<_>>();
		let header = CarHeader::new_v1(root_cids);
		let header_written = header.write(writer)? as u64;

		// Write blocks in node insertion order, which preserves the original file block order
		// on round-trips. BFS would visit children in reverse-insertion order due to petgraph's
		// adjacency list being prepend-only.
		for id in self.dag.node_indices() {
			let block = self.dag.node_weight(id).expect(NODE_IDX_QED);
			let cid = block.cid;
			let written_bytes = match &block.r#type {
				BlockType::Raw => {
					let len = block.data.bound_len();
					write_block(cid, len, &mut block.data.clone_and_rewind(), writer)?
				},
				BlockType::DagPb(dag_pb) => {
					if block.data.bound_len() > 0 {
						// Pass-through: write the original bytes from the loaded file.
						let len = block.data.bound_len();
						write_block(cid, len, &mut block.data.clone_and_rewind(), writer)?
					} else {
						// New block (no original bytes): encode from structure.
						let pb_node = Bytes::from(self.as_pb_node(id, dag_pb)?.into_bytes());
						let pb_node_len = pb_node.len() as u64;
						write_block(cid, pb_node_len, &mut pb_node.reader(), writer)?
					}
				},
			};
			acc_written = acc_written.checked_add(written_bytes).ok_or(Error::FileTooLarge)?;
		}

		header_written.checked_add(acc_written).ok_or(Error::FileTooLarge)
	}

	pub fn write_to_tmp(&mut self) -> Result<NamedTempFile> {
		let mut writer = BufWriter::new(NamedTempFile::new()?);
		let _ = self.write(&mut writer)?;
		Ok(writer.into_inner()?)
	}
}
