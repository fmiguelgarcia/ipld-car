use crate::{car::ContentAddressableArchive, error::Result};

use std::{
	convert::Infallible,
	fs::File,
	io::{BufReader, BufWriter, IntoInnerError, Read, Result as IoResult, Seek, Write},
	result::Result as StdResult,
	sync::{Arc, Mutex, MutexGuard},
};

mod defered_append_file;
#[cfg(test)]
mod tests;
mod vfs;

#[derive(Debug, Clone)]
pub struct CarFs<T: Read + Seek> {
	car: Arc<Mutex<ContentAddressableArchive<T>>>,
}

impl<T: Read + Seek> CarFs<T> {
	pub fn into_inner(self) -> Option<ContentAddressableArchive<T>> {
		let mutexed = Arc::into_inner(self.car)?;
		mutexed.into_inner().ok()
	}

	pub fn lock(&self) -> Result<MutexGuard<'_, ContentAddressableArchive<T>>> {
		self.car.lock().map_err(Into::into)
	}
}

impl<T: Read + Seek> From<ContentAddressableArchive<T>> for CarFs<T> {
	fn from(car: ContentAddressableArchive<T>) -> Self {
		Self { car: Arc::new(Mutex::new(car)) }
	}
}

pub trait CarFile {
	type Writer: Write;
	type Reader: Read;
	type IntoWriterErr;
	type IntoReaderErr;

	fn temporal() -> IoResult<Self>
	where
		Self: Sized;

	fn into_writer(self) -> StdResult<Self::Writer, Self::IntoWriterErr>
	where
		Self: Sized;

	fn into_reader(self) -> StdResult<Self::Reader, Self::IntoReaderErr>
	where
		Self: Sized;
}

impl CarFile for File {
	type Writer = File;
	type Reader = File;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> IoResult<Self> {
		tempfile::tempfile()
	}

	fn into_writer(self) -> StdResult<Self::Writer, Self::IntoWriterErr> {
		Ok(self)
	}

	fn into_reader(self) -> StdResult<Self::Reader, Self::IntoReaderErr> {
		Ok(self)
	}
}

impl<F: CarFile + Write + Read> CarFile for BufReader<F> {
	type Writer = BufWriter<F>;
	type Reader = BufReader<F>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> IoResult<Self> {
		F::temporal().map(BufReader::new)
	}

	fn into_writer(self) -> StdResult<Self::Writer, Self::IntoWriterErr> {
		Ok(BufWriter::new(self.into_inner()))
	}

	fn into_reader(self) -> StdResult<Self::Reader, Self::IntoReaderErr> {
		Ok(self)
	}
}

impl<F: CarFile + Write + Read> CarFile for BufWriter<F> {
	type Writer = BufWriter<F>;
	type Reader = BufReader<F>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = IntoInnerError<Self>;

	fn temporal() -> IoResult<Self> {
		F::temporal().map(BufWriter::new)
	}

	fn into_writer(self) -> StdResult<Self::Writer, Self::IntoWriterErr> {
		Ok(self)
	}

	fn into_reader(self) -> StdResult<Self::Reader, Self::IntoReaderErr> {
		self.into_inner().map(BufReader::new)
	}
}
