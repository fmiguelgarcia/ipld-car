use crate::car::ContentAddressableArchive;

use std::sync::{Arc, Mutex};

#[cfg(test)]
mod tests;
mod vfs;

#[derive(Debug)]
pub struct CarFs<T> {
	car: Arc<Mutex<ContentAddressableArchive<T>>>,
}

impl<T> CarFs<T> {
	pub fn into_inner(self) -> Option<ContentAddressableArchive<T>> {
		let mutexed = Arc::into_inner(self.car)?;
		mutexed.into_inner().ok()
	}
}

impl<T> From<ContentAddressableArchive<T>> for CarFs<T> {
	fn from(car: ContentAddressableArchive<T>) -> Self {
		Self { car: Arc::new(Mutex::new(car)) }
	}
}
