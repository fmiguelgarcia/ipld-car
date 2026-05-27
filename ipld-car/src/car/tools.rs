use crate::{
	car::BlockId,
	ensure,
	error::{Error, LoopDetectedErr, Result},
};

use libipld::Cid;
use smallvec::SmallVec;
use std::{
	io::{copy, Read, Write},
	path::{Path, PathBuf},
};

pub(crate) fn write_block<R: Read, W: Write>(cid: Cid, reader_len: u64, reader: &mut R, w: &mut W) -> Result<u64> {
	let cid = cid.to_bytes();
	let section_len = reader_len.checked_add(cid.len() as u64).ok_or(Error::FileTooLarge)?;

	let leb_written = leb128::write::unsigned(w, section_len)? as u64;
	w.write_all(&cid)?;
	let copied = copy(reader, w)?;

	copied.checked_add(leb_written + cid.len() as u64).ok_or(Error::FileTooLarge)
}

/// Uses `open_block_ids` to track visited block IDs, in order to detect loops during the
/// resolution of symbolic links.
pub(crate) fn ensure_no_loop<P: Into<PathBuf>>(
	visited: &mut SmallVec<[BlockId; 1]>,
	next_id: BlockId,
	path: P,
) -> Result<()> {
	ensure!(!visited.contains(&next_id), LoopDetectedErr::Symlink(path.into()));
	visited.push(next_id);
	Ok(())
}

pub(crate) fn parent_or_root(path: &Path) -> &Path {
	path.parent().unwrap_or(Path::new("."))
}
