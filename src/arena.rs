use std::{collections::BTreeMap, slice::Iter};

use derivative::Derivative;

pub type ArenaId = usize;

/// Trait for items that can be indexed in an arena. Items provide an optional index key that maps
/// to their arena ID.
pub trait ArenaItem {
	type Id: Ord;

	fn index(&self) -> Option<Self::Id>;
	fn children(&self) -> Vec<Self>
	where
		Self: Sized;
}

/// Arena allocator with optional indexed lookup. Stores items in a vector and maintains an
/// index mapping arbitrary keys to arena IDs. Items are accessed by ArenaId (vector position)
/// or by their indexed key.
///
/// # TODO:
// - Delete an element. Use kind of `Option` because index should be increasing and it cannot be reindexed.
#[derive(Debug, Derivative)]
#[derivative(Default(bound = ""))]
pub struct Arena<T: ArenaItem> {
	items: Vec<T>,
	index: BTreeMap<T::Id, ArenaId>,
}

impl<T: ArenaItem> Arena<T> {
	/// Creates a new arena with pre-allocated capacity.
	pub fn with_capacity(capacity: usize) -> Self {
		Self { items: Vec::with_capacity(capacity), index: BTreeMap::new() }
	}

	/// Returns the ArenaId that will be assigned to the next item pushed.
	#[inline]
	pub fn next_id(&self) -> ArenaId {
		self.items.len()
	}

	/// Pushes a new item into the arena, returns its ArenaId.
	pub fn push(&mut self, item: T) -> ArenaId {
		// Insert `item` and index it.
		let id = self.items.len();
		if let Some(idx) = item.index() {
			self.index.insert(idx, id);
		}
		self.items.push(item);

		id
	}

	pub fn recursive_push(&mut self, item: T) -> ArenaId {
		let children = item.children();
		let id = self.push(item);
		let _child_ids = children.into_iter().map(|child| self.push(child)).collect::<Vec<_>>();
		id
	}

	/// Returns an iterator over all items in the arena.
	pub fn iter(&self) -> Iter<'_, T> {
		self.items.iter()
	}

	/// Returns a reference to the item with the given ArenaId.
	#[inline]
	pub fn get(&self, id: usize) -> Option<&T> {
		self.items.get(id)
	}

	/// Returns a reference to the item with the given index key.
	pub fn get_by_index(&self, idx: &T::Id) -> Option<&T> {
		let id = self.get_id_by_index(idx)?;
		self.get(id)
	}

	/// Returns the ArenaId associated with the given index key.
	#[inline]
	pub fn get_id_by_index(&self, idx: &T::Id) -> Option<ArenaId> {
		self.index.get(idx).cloned()
	}

	/// Returns a mutable reference to the item with the given ArenaId.
	#[inline]
	pub fn get_mut(&mut self, id: usize) -> Option<&mut T> {
		self.items.get_mut(id)
	}

	/// Returns a mutable reference to the item with the given index key.
	pub fn get_mut_by_index(&mut self, idx: &T::Id) -> Option<&mut T> {
		let id = self.get_id_by_index(idx)?;
		self.get_mut(id)
	}
}
