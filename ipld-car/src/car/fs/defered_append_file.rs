use crate::{
	arena::ArenaId,
	car::fs::{CarFile, CarFs},
	error::{Error, Result},
	ContentAddressableArchive,
};

use std::{
	io::{Read, Result as IoResult, Seek, SeekFrom, Write},
	path::{Path, PathBuf},
	sync::{Arc, Mutex},
};

pub struct DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	car: Arc<Mutex<ContentAddressableArchive<T>>>,
	parent: ArenaId,
	parent_path: PathBuf,
	name: String,
	writer: Option<W>,
}

impl<T, W> DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	pub fn new(fs: &CarFs<T>, parent: ArenaId, parent_path: &Path, name: String, writer: W) -> Self {
		let car = Arc::clone(&fs.car);
		let parent_path = parent_path.to_path_buf();
		let writer = Some(writer);

		Self { car, parent, name, parent_path, writer }
	}

	fn ref_add_to_car(&mut self) -> Result<()> {
		if let Some(mut writer) = self.writer.take() {
			writer.seek(SeekFrom::Start(0))?;
			let reader = writer.into_reader()?;
			self.car.lock()?.add_file(self.name.clone(), self.parent, &self.parent_path, reader.into())?;
		}

		Ok(())
	}

	#[allow(dead_code)]
	fn add_to_car(mut self) -> Result<()> {
		self.ref_add_to_car()
	}
}

impl<T, W> Seek for DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
		self.writer.as_mut().map(|w| w.seek(pos)).unwrap_or(Ok(0))
	}
}

impl<T, W> Read for DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + Read + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
		self.writer.as_mut().map(|w| w.read(buf)).unwrap_or(Ok(0))
	}
}

impl<T, W> Write for DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
		self.writer.as_mut().map(|w| w.write(buf)).unwrap_or(Ok(0))
	}

	fn flush(&mut self) -> IoResult<()> {
		self.writer.as_mut().map(|w| w.flush()).unwrap_or(Ok(()))
	}
}

impl<T, W> Drop for DeferedAppendFile<T, W>
where
	T: Read + Seek + From<<W as CarFile>::Reader>,
	W: Write + Seek + CarFile,
	Error: From<<W as CarFile>::IntoReaderErr>,
{
	fn drop(&mut self) {
		let _ = self.ref_add_to_car();
	}
}
