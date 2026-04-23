use crate::{
	car::fs::{CarFs, RWTransmuter},
	error::{Error, Result},
	traits::AsFileSystem as _,
	ContentAddressableArchive,
};

use std::{
	io::{Read, Result as IoResult, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};

pub struct DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	car: Arc<Mutex<ContentAddressableArchive<T>>>,
	file_path: PathBuf,
	writer: Option<W>,
}
impl<T, W> DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	pub fn new(fs: &CarFs<T>, file_path: &Path, writer: W) -> Self {
		let car = Arc::clone(&fs.car);
		let writer = Some(writer);

		Self { car, file_path: file_path.to_path_buf(), writer }
	}
}

impl<T, W> DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	fn ref_add_to_car(&mut self) -> Result<()> {
		if let Some(mut writer) = Option::take(&mut self.writer) {
			writer.seek(SeekFrom::Start(0))?;
			let reader = writer.into_reader()?;
			self.car.lock()?.add_file(&self.file_path, reader.into())?;
		}

		Ok(())
	}
}

impl<T, W> Seek for DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
		self.writer.as_mut().map(|w| w.seek(pos)).unwrap_or(Ok(0))
	}
}

impl<T, W> Read for DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Read + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
		self.writer.as_mut().map(|w| w.read(buf)).unwrap_or(Ok(0))
	}
}

impl<T, W> Write for DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
		self.writer.as_mut().map(|w| w.write(buf)).unwrap_or(Ok(0))
	}

	fn flush(&mut self) -> IoResult<()> {
		self.writer.as_mut().map(|w| w.flush()).unwrap_or(Ok(()))
	}
}

/// # TODO:
/// - Look better architecture to append file and support VFS.
impl<T, W> Drop for DeferredAppendFile<T, W>
where
	T: Read + Seek + From<<W as RWTransmuter>::Reader>,
	W: Write + Seek + RWTransmuter,
	Error: From<<W as RWTransmuter>::IntoReaderErr>,
{
	fn drop(&mut self) {
		if let Err(e) = self.ref_add_to_car() {
			tracing::error!(?e, "Failed to commit block on drop");
		}
	}
}
