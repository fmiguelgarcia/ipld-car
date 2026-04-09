use crate::{car::ContentAddressableArchive, error::Result};

use std::{
	io::{Read, Seek},
	sync::{Arc, Mutex, MutexGuard},
};

mod rw_transmuter;
pub use rw_transmuter::RWTransmuter;
mod deferred_append_file;
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
