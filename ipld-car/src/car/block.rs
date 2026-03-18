use crate::{
	arena::ArenaItem,
	car::block_content::BlockContent,
	config::{CidCodec, Config},
	dag_pb::{DagPb, Link, ReaderWithLen},
	ensure,
	error::{DagPbResult, Error, Result},
	fail, Arena, BoundedReader, ContextLen,
};

use derivative::Derivative;
use libipld::Cid;
use std::io::{self, copy, Read, Seek, Write};

#[derive(Derivative)]
#[derivative(Clone)]
pub struct Block<T> {
	cid: Option<Cid>,
	#[derivative(Clone(bound = ""))]
	pub(crate) content: BlockContent<T>,
}

impl<T> Block<T> {
	pub fn new<C, CT>(cid: C, content: CT) -> Self
	where
		C: Into<Option<Cid>>,
		CT: Into<BlockContent<T>>,
	{
		Self { cid: cid.into(), content: content.into() }
	}

	pub fn cid(&self) -> Option<&Cid> {
		self.cid.as_ref()
	}

	pub fn push_directory_entry(&mut self, name: String, link: Link) -> DagPbResult<()> {
		if let BlockContent::DagPb(DagPb::Dir(directory)) = &mut self.content {
			ensure!(!directory.entries().contains_key(&name), io::Error::from(io::ErrorKind::AlreadyExists));
			directory.mut_entries().insert(name, link);
			self.cid.take();

			Ok(())
		} else {
			fail!(io::Error::from(io::ErrorKind::NotFound))
		}
	}
}

impl<T> ContextLen for Block<T> {
	fn data_len(&self) -> u64 {
		self.content.data_len()
	}

	fn dag_pb_len(&self) -> u64 {
		self.content.dag_pb_len()
	}
}

impl<T: Read + Seek> ArenaItem for Block<T> {
	type Id = Cid;

	#[inline]
	fn index(&self) -> Option<Self::Id> {
		self.cid
	}

	fn children(&self) -> Vec<Self> {
		match &self.content {
			BlockContent::DagPb(DagPb::MultiBlockFile(mbf)) => {
				let mut local_arena = Arena::default();
				let mut offset = 0u64;

				mbf.links()
					.iter()
					.map(|link| {
						let sub_reader = mbf
							.reader()
							.sub(offset..offset + link.cumulative_dag_size)
							.expect("Sub reader is valid in `Block::children`");
						let codec = CidCodec::try_from(link.cid.codec()).expect("Generated block uses valid CID codec");
						let block = match codec {
							CidCodec::Raw => Block::new(link.cid, BlockContent::Raw(sub_reader)),
							CidCodec::DagPb => {
								let child_id = DagPb::load(&mut local_arena, link.cid, sub_reader)
									.expect("Block previously loaded .qed");
								let block_ref = local_arena.get(child_id).expect("Node inserted previously .qed");
								debug_assert_eq!(block_ref.cid(), Some(&link.cid));

								(*block_ref).clone()
							},
							_ => unimplemented!("Unimplemented CID codec on `Block::children`"),
						};
						offset += link.cumulative_dag_size;

						block
					})
					.collect()
			},
			_ => vec![],
		}
	}
}

impl<T> std::fmt::Debug for Block<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let cid = self.cid.as_ref().map(Cid::to_string);
		f.debug_struct("Block").field("cid", &cid).field("content", &self.content).finish()
	}
}

// Write into
// ===========================================================================

impl<T: Read + Seek + 'static> Block<T> {
	pub fn write<W: Write>(&self, arena: &Arena<Block<T>>, w: &mut W, config: &Config) -> Result<u64> {
		let cid = self.cid.as_ref();
		match &self.content {
			BlockContent::Raw(reader) => write_raw(w, cid, reader, config),
			BlockContent::DagPb(dag_pb) => write_dag_pb(w, arena, cid, dag_pb, config),
		}
	}
}

fn write_dag_pb<W: Write, T: Read + Seek + 'static>(
	w: &mut W,
	arena: &Arena<Block<T>>,
	cid: Option<&Cid>,
	dag_pb: &DagPb<T>,
	_config: &Config,
) -> Result<u64> {
	match cid {
		Some(cid) => {
			let ReaderWithLen { mut reader, len } = dag_pb.content_writer(arena)?;
			write_block(cid, &mut reader, len, w)
		},
		None => unimplemented!("Block:write_dag_pb"),
	}
}

fn write_raw<W: Write, T: Read + Seek>(
	w: &mut W,
	cid: Option<&Cid>,
	reader: &BoundedReader<T>,
	_config: &Config,
) -> Result<u64> {
	match cid {
		Some(cid) => {
			let reader_len = reader.bound_len();
			let mut reader = reader.clone_and_rewind();
			write_block(cid, &mut reader, reader_len, w)
		},
		None => unimplemented!("Block::write_raw"),
	}
}

fn write_block<R: Read, W: Write>(cid: &Cid, reader: &mut R, reader_len: u64, w: &mut W) -> Result<u64> {
	let cid = cid.to_bytes();
	let section_len = reader_len.checked_add(cid.len() as u64).ok_or(Error::FileTooLarge)?;

	let leb_written = leb128::write::unsigned(w, section_len)? as u64;
	w.write_all(&cid)?;
	let copied = copy(reader, w)?;

	copied.checked_add(leb_written + cid.len() as u64).ok_or(Error::FileTooLarge)
}
