use crate::{
	car::{
		fs::{deferred_append_file::DeferredAppendFile, CarFs, RWTransmuter},
		ContentAddressableArchive,
	},
	error::{Error, Result},
};

use std::{
	fmt::Debug,
	io::{Read, Seek},
	path::Path,
	sync::{Arc, Mutex, MutexGuard},
	time::SystemTime,
};
use vfs::{
	error::{VfsError, VfsErrorKind, VfsResult},
	FileSystem, SeekAndRead, SeekAndWrite, VfsMetadata,
};

impl<T> FileSystem for CarFs<T>
where
	T: RWTransmuter + Read + Seek + Debug + Sync + Send + 'static,
	T: From<<<T as RWTransmuter>::Writer as RWTransmuter>::Reader>,
	<T as RWTransmuter>::Writer: Seek + Send + Sync + 'static,
	Error: From<<<T as RWTransmuter>::Writer as RWTransmuter>::IntoReaderErr>,
{
	fn read_dir(&self, path: &str) -> VfsResult<Box<dyn Iterator<Item = String> + Send>> {
		let path = Path::new(path);
		let entries = car_lock(&self.car)?.read_dir(path)?.map(|entry| entry.to_owned()).collect::<Vec<_>>();

		Ok(Box::new(entries.into_iter()))
	}

	fn create_file(&self, path: &str) -> VfsResult<Box<dyn SeekAndWrite + Send>> {
		let path = Path::new(path);
		let writer = <T as RWTransmuter>::Writer::temporal()?;

		let deferred_appender = DeferredAppendFile::<T, _>::new(self, path, writer);
		Ok(Box::new(deferred_appender))
	}

	fn create_dir(&self, path: &str) -> VfsResult<()> {
		let path = Path::new(path);
		let mut car = car_lock(&self.car)?;
		car.create_dir(path).map_err(Into::into)
	}

	fn open_file(&self, path: &str) -> VfsResult<Box<dyn SeekAndRead + Send>> {
		let path = Path::new(path);
		let car = car_lock(&self.car)?;
		let file = car.open_file(path)?;
		Ok(Box::new(file))
	}

	fn exists(&self, path: &str) -> VfsResult<bool> {
		let car = car_lock(&self.car)?;
		Ok(car.exists(Path::new(path)))
	}

	/// Returns the file metadata for the file at this path
	fn metadata(&self, path: &str) -> VfsResult<VfsMetadata> {
		let path = Path::new(path);
		let meta = car_lock(&self.car)?.metadata(path).map(Into::into)?;
		Ok(meta)
	}

	/// Opens the file at this path for appending
	fn append_file(&self, _path: &str) -> VfsResult<Box<dyn SeekAndWrite + Send>> {
		Err(VfsErrorKind::NotSupported.into())
	}

	/// Sets the files creation timestamp, if the implementation supports it
	fn set_creation_time(&self, _path: &str, _time: SystemTime) -> VfsResult<()> {
		Err(VfsError::from(VfsErrorKind::NotSupported))
	}
	/// Sets the files modification timestamp, if the implementation supports it
	fn set_modification_time(&self, _path: &str, _time: SystemTime) -> VfsResult<()> {
		Err(VfsError::from(VfsErrorKind::NotSupported))
	}
	/// Sets the files access timestamp, if the implementation supports it
	fn set_access_time(&self, _path: &str, _time: SystemTime) -> VfsResult<()> {
		Err(VfsError::from(VfsErrorKind::NotSupported))
	}

	/// Removes the file at this path
	fn remove_file(&self, _path: &str) -> VfsResult<()> {
		Err(VfsError::from(VfsErrorKind::NotSupported))
	}
	/// Removes the directory at this path
	fn remove_dir(&self, _path: &str) -> VfsResult<()> {
		Err(VfsError::from(VfsErrorKind::NotSupported))
	}
}

fn car_lock<T: Read + Seek>(
	car: &Arc<Mutex<ContentAddressableArchive<T>>>,
) -> Result<MutexGuard<'_, ContentAddressableArchive<T>>> {
	car.lock().map_err(Error::from)
}
