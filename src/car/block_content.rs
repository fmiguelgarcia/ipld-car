use crate::{dag_pb::DagPb, BoundedReader};

use derive_more::From;

/*
use std::io::{Read, Seek, Write};
pub trait WriteReadSeek: Read + Write + Seek + Send + Sync {}
impl<T: Read + Write + Seek + Send + Sync> WriteReadSeek for T {}
*/

#[derive(From)]
pub enum BlockContent<T> {
	Raw(BoundedReader<T>),
	DagPb(DagPb<T>),
	/*
	#[from(skip)]
	File(#[debug(skip)] BoundedReader<T>),
	#[from(skip)]
	ExternalFile(#[debug(skip)] Arc<Mutex<dyn WriteReadSeek>>),
	FileRef(Vec<EntryId>),
	Dir(BTreeMap<String, EntryId>),
	// Ref(u64),
	Link(u64),
	Symlink(String),
	*/
}

impl<T> std::fmt::Debug for BlockContent<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Raw(reader) => f.debug_tuple("Raw").field(reader).finish(),
			Self::DagPb(dag) => f.debug_tuple("DagPb").field(dag).finish(),
		}
	}
}

// Read
// ===========================================================================

/*
impl<T: Read + Seek> CarEntry<T> {

	pub fn read(&mut self, cid: Cid, reader: T) -> CarResult<Vec<CarEntry<T>> {
		let mut buf = Bytes::from(vec![0u8; 16_384]);
		let read_bytes = reader.read(&mut buf)?;
		let PbNode{ links, data}= PbNode::from_bytes(buf)?;
	}
}
*/

// Write into
// ===========================================================================

/*
impl<T: Read + Seek> CarEntry<T> {
	pue fn cid_or_build(&mut self, config: &Config) -> CarResult<Cid> {
		if let Some(cid) = self.cid {
			return Ok(cid)
		}

		let cid = self.dry_run_write(config)?;
		self.cid = Some(cid);
		Ok(cid)
	}

	pub fn dry_run_write(&self, config: &Config) -> CarResult<Cid> {
		let mut empty = empty();
		match &self.content {
			CarEntryContent::File(bounded_reader) =>
				write_file_content(&mut bounded_reader.clone(), &mut empty, config),
			CarEntryContent::ExternalFile(shared_reader) => {
				let mut reader = shared_reader.lock().map_err(|_| CarPoisoned::Storage)?;
				write_file_content(&mut *reader, &mut empty, config)
			},
			CarEntryContent::Link(..) => self.cid.ok_or(CarErr::Invalid(CarInvalid::Link)),
			CarEntryContent::Dir(..) | CarEntryContent::FileRef(..) | CarEntryContent::Symlink(..) => {
				unimplemented!()
			},
		}
	}

	pub fn write<W: Write>(&self, arena: &EntryArena<T>, writer: &mut W, config: &Config) -> CarResult<Cid> {
		let (cid, tmp_writer) = match &self.content {
			CarEntryContent::File(reader) => {
				let mut tmp_writer = BufWriter::new(tempfile()?);
				let cid = write_file_content(&mut reader.clone(), &mut tmp_writer, config)?;
				(cid, tmp_writer)
			},
			CarEntryContent::ExternalFile(shared_reader) => {
				let mut tmp_writer = BufWriter::new(tempfile()?);
				let mut reader = shared_reader.lock().map_err(|_| CarPoisoned::Storage)?;
				let cid = write_file_content(&mut *reader, &mut tmp_writer, config)?;
				(cid, tmp_writer)
			},
			CarEntryContent::Dir(name_to_entry_id) => {
				let mut tmp_writer = BufWriter::new(tempfile()?);
				let cid = write_dir_content(arena, name_to_entry_id, self.cid.as_ref(), &mut tmp_writer, config)?;
				(cid, tmp_writer)
			},
			CarEntryContent::Link(..) => return self.cid.ok_or_else(|| CarInvalid::Link.into()),
			CarEntryContent::FileRef(..) => return self.cid.ok_or_else(|| CarInvalid::FileRef.into()),
			CarEntryContent::Symlink(..) => unimplemented!("Wirte content Symlink"),
		};

		let mut tmp_file = tmp_writer.into_inner().map_err(|buf_into_err| buf_into_err.into_error())?;
		let tmp_file_size = tmp_file.stream_position()?;
		let encoded_cid = cid.to_bytes();
		let block_len = tmp_file_size.checked_add(encoded_cid.len() as u64).ok_or(CarErr::FileTooLarge)?;
		tmp_file.seek(SeekFrom::Start(0))?;
		let mut tmp_reader = BufReader::new(tmp_file);

		// Write into `writer` following CAR block format.
		leb128::write::unsigned(writer, block_len)?;
		writer.write_all(&encoded_cid)?;
		let _ = copy(&mut tmp_reader, writer)?;

		Ok(cid)
	}
}

fn write_dir_content<T, W>(
	arena: &EntryArena<T>,
	name_to_entry_id: &BTreeMap<String, usize>,
	cid: Option<&Cid>,
	writer: &mut W,
	config: &Config,
) -> CarResult<Cid>
where
	W: Write,
{
	// Build PbLinks
	let links = name_to_entry_id
		.iter()
		.map(|(name, entry_id)| {
			let entry = arena.get(*entry_id).ok_or(CarNotFound::EntryId(*entry_id))?;
			let cid = entry.cid.ok_or(CarNotFound::CidOnDirEntry)?;
			Ok(PbLink { cid, name: name.clone().into(), size: None })
		})
		.collect::<Result<Vec<_>, CarErr>>()?;

	// Build pbNode
	let data = Some(Bytes::from(proto::Data::new_directory().encode_to_vec()));
	let pb_node = Bytes::from(PbNode { links, data }.into_bytes());
	copy(&mut pb_node.clone().reader(), writer)?;

	match cid {
		Some(cid) => Ok(*cid),
		None => {
			ensure!(config.layout == DAGLayout::Flat, CarNotSupported::DAGLayout(config.layout));
			ensure!(config.leaf_policy == LeafPolicy::Raw, CarNotSupported::LeafPolicy(config.leaf_policy));
			let mut hasher = config.hasher().ok_or(CarNotSupported::Hasher(config.hash_code))?;
			let (cid, _) = build_cid(&mut pb_node.clone().reader(), &mut *hasher, config)?
				.expect("PbNode with directory is not empty .qed");

			Ok(cid)
		},
	}
}

fn write_file_content<R, W>(reader: &mut R, writer: &mut W, config: &Config) -> CarResult<Cid>
where
	R: Read + Seek + ?Sized,
	W: Write,
{
	let block_size = config.chunk_policy.fixed_size().ok_or(CarNotSupported::ChunkPolicy(config.chunk_policy))? as u64;
	ensure!(config.layout == DAGLayout::Flat, CarNotSupported::DAGLayout(config.layout));
	ensure!(config.leaf_policy == LeafPolicy::Raw, CarNotSupported::LeafPolicy(config.leaf_policy));
	reader.rewind()?;

	// Build PbLinks
	let mut links = vec![];
	let mut hasher = config.hasher().ok_or(CarNotSupported::Hasher(config.hash_code))?;
	while let Some((cid, hashed_bytes)) = build_cid(&mut reader.take(block_size), &mut *hasher, config)? {
		links.push(PbLink { cid, name: None, size: Some(hashed_bytes) });
	}

	// Build PbNode
	let blocksizes = links.iter().filter_map(|link| link.size).collect::<Vec<_>>();
	debug_assert_eq!(blocksizes.len(), links.len());
	let filesize = blocksizes.iter().sum();
	let data = Some(Bytes::from(proto::Data::new_file(filesize, blocksizes).encode_to_vec()));
	let pb_node = Bytes::from(PbNode { links, data }.into_bytes());

	let (root_cid, _) =
		build_cid(&mut pb_node.clone().reader(), &mut *hasher, config)?.expect("PbNode encode is not empty .qed");

	// Copy PbNode & data
	copy(&mut pb_node.reader(), writer)?;
	reader.rewind()?;
	copy(reader, writer)?;

	Ok(root_cid)
}

fn build_cid<R, H>(reader: &mut R, hasher: &mut H, config: &Config) -> CarResult<Option<(Cid, u64)>>
where
	R: Read,
	H: Hasher + Write + ?Sized,
{
	hasher.reset();
	let hashed_bytes = copy(reader, hasher)?;
	if hashed_bytes == 0 {
		return Ok(None)
	}

	let digest = config.hash_code.wrap(hasher.finalize())?;
	let cid = Cid::new_v1(config.cid_codec as u64, digest);

	Ok(Some((cid, hashed_bytes)))
}
*/
