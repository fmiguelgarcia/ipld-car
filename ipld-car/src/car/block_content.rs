use crate::{dag_pb::DagPb, BoundedReader, ContextLen};

use derive_more::From;

#[derive(From)]
pub enum BlockContent<T> {
	Raw(BoundedReader<T>),
	DagPb(DagPb<T>),
}

impl<T> Clone for BlockContent<T> {
	fn clone(&self) -> Self {
		match self {
			Self::Raw(reader) => Self::Raw(reader.clone()),
			Self::DagPb(dag) => Self::DagPb(dag.clone()),
		}
	}
}

impl<T> ContextLen for BlockContent<T> {
	fn data_len(&self) -> u64 {
		match self {
			Self::Raw(r) => r.bound_len(),
			Self::DagPb(dag) => dag.data_len(),
		}
	}

	fn dag_pb_len(&self) -> u64 {
		match self {
			Self::Raw(r) => r.bound_len(),
			Self::DagPb(dag) => dag.dag_pb_len(),
		}
	}
}

impl<T> std::fmt::Debug for BlockContent<T> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Raw(reader) => f.debug_tuple("Raw").field(reader).finish(),
			Self::DagPb(dag) => f.debug_tuple("DagPb").field(dag).finish(),
		}
	}
}
