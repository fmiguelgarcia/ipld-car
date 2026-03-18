use crate::{Arena, ArenaId, ArenaItem};

use libipld::{pb::PbLink, Cid};

#[derive(derive_more::Debug, Clone, Copy)]
pub struct Link {
	#[debug("{}", cid.to_string())]
	pub cid: Cid,
	pub cumulative_dag_size: u64,

	pub blocksize: Option<u64>,
	/// In-memory hint: the `ArenaId` of the block this link points to. Not serialized.
	/// Set when a link is created in-memory via [`create_dir`] to avoid CID-index
	/// collisions between distinct blocks that share the same content (and thus CID).
	pub(crate) arena_id: Option<ArenaId>,
}

impl Link {
	pub fn new<B, I>(cid: Cid, cumulative_dag_size: u64, blocksize: B, arena_id: I) -> Self
	where
		B: Into<Option<u64>>,
		I: Into<Option<ArenaId>>,
	{
		Self { cid, cumulative_dag_size, blocksize: blocksize.into(), arena_id: arena_id.into() }
	}

	pub fn with_arena_id(mut self, id: ArenaId) -> Self {
		self.arena_id = Some(id);
		self
	}

	pub fn with_arena<T>(mut self, arena: &Arena<T>) -> Self
	where
		T: ArenaItem<Id = Cid>,
	{
		self.arena_id = arena.get_id_by_index(&self.cid);
		self
	}
}

impl From<&Link> for PbLink {
	fn from(l: &Link) -> Self {
		PbLink { cid: l.cid, name: None, size: Some(l.cumulative_dag_size) }
	}
}

impl From<PbLink> for Link {
	fn from(pb_link: PbLink) -> Self {
		let cumulative_dag_size = pb_link.size.unwrap_or_default();
		Self::new(pb_link.cid, cumulative_dag_size, None, None)
	}
}
