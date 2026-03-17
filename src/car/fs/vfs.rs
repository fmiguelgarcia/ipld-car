use crate::{
	car::{
		fs::{defered_append_file::DeferedAppendFile, CarFile, CarFs},
		BlockContent, ContentAddressableArchive,
	},
	dag_pb::DagPb,
	error::{Error, Result},
	fail,
};

use std::{
	ffi::OsStr,
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
	T: Read + Seek + Debug + Sync + Send + 'static + CarFile,
	<T as CarFile>::Writer: CarFile<Reader = T> + Seek + Send + Sync,
	Error: From<<<T as CarFile>::Writer as CarFile>::IntoReaderErr>,
{
	fn read_dir(&self, path: &str) -> VfsResult<Box<dyn Iterator<Item = String> + Send>> {
		let path = Path::new(path);
		let car = car_lock(&self.car)?;
		let found = car.path_to_block(path)?;

		match &found.content {
			BlockContent::DagPb(DagPb::Dir(dir)) => {
				let names = dir.entries().keys().cloned().collect::<Vec<_>>();
				Ok(Box::new(names.into_iter()))
			},
			_ => Err(VfsErrorKind::NotSupported.into()),
		}
	}

	fn create_file(&self, path: &str) -> VfsResult<Box<dyn SeekAndWrite + Send>> {
		let path = Path::new(path);
		let file_name = path.file_name().and_then(OsStr::to_str).ok_or(VfsErrorKind::InvalidPath)?;
		let parent_path = path.parent().unwrap_or_else(|| Path::new("."));

		let parent_id = self.lock()?.path_to_block_id(parent_path)?;
		let writer = <T as CarFile>::Writer::temporal()?;

		let defered_appender = DeferedAppendFile::new(self, parent_id, file_name.to_string(), writer);
		Ok(Box::new(defered_appender))
	}

	fn create_dir(&self, path: &str) -> VfsResult<()> {
		let path = Path::new(path);
		let dir_name = path.file_name().and_then(OsStr::to_str).ok_or(VfsErrorKind::InvalidPath)?;
		let parent_path = path.parent().unwrap_or_else(|| Path::new("."));

		let mut car = car_lock(&self.car)?;
		car.create_dir(parent_path, dir_name).map_err(Into::into)
	}

	fn open_file(&self, path: &str) -> VfsResult<Box<dyn SeekAndRead + Send>> {
		let path = Path::new(path);

		let car = car_lock(&self.car)?;
		let found = car.path_to_block(path)?;
		match &found.content {
			BlockContent::Raw(reader) => Ok(Box::new(reader.clone_and_rewind())),
			BlockContent::DagPb(dag_pb) => match dag_pb {
				DagPb::SingleBlockFile(sbl) => Ok(sbl.reader()),
				DagPb::MultiBlockFile(..) | DagPb::Symlink(..) | DagPb::Dir(..) => fail!(VfsErrorKind::FileNotFound),
			},
		}
	}

	fn exists(&self, path: &str) -> VfsResult<bool> {
		let path = Path::new(path);
		let car = car_lock(&self.car)?;
		let _found_id = car.path_to_block_id(path)?;
		Ok(true)
	}

	/// Returns the file metadata for the file at this path
	fn metadata(&self, path: &str) -> VfsResult<VfsMetadata> {
		let path = Path::new(path);
		let car = car_lock(&self.car)?;
		let found = car.path_to_block(path)?;

		car.metadata_by_ref(found)
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

/*
fn path_to_mut_block<'a, T>(car: &'a mut ContentAddressableArchive<T>, path: &Path) -> CarResult<&'a mut Block<T>> {
	let found_id = car.path_to_block_id(path)?;
	car.arena.get_mut(found_id).ok_or_else(|| CarErr::from(CarNotFound::ArenaId(found_id)))
}
*/

fn car_lock<T: Read + Seek>(
	car: &Arc<Mutex<ContentAddressableArchive<T>>>,
) -> Result<MutexGuard<'_, ContentAddressableArchive<T>>> {
	car.lock().map_err(Error::from)
}
