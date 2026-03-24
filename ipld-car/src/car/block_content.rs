use crate::{dag_pb::DagPb, error::Result, BoundedReader, CIDBuilder, Config, ContextLen};

use derivative::Derivative;
use derive_more::From;
use libipld::Cid;
use std::io::{Read, Seek};

#[derive(From, Derivative, derive_more::Debug)]
#[derivative(Clone(bound = ""))]
pub enum BlockContent<T> {
	Raw(BoundedReader<T>),
	DagPb(DagPb<T>),
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

	fn invalidate(&mut self) {
		match self {
			Self::Raw(..) => {},
			Self::DagPb(dag) => dag.invalidate(),
		}
	}

	fn was_invalidated(&self) -> bool {
		match self {
			Self::Raw(..) => false,
			Self::DagPb(dag) => dag.was_invalidated(),
		}
	}
}

// Ipld & CID related
// ===========================================================================

impl<T: Read + Seek> CIDBuilder for BlockContent<T> {
	fn cid(&self, config: &Config) -> Result<Cid> {
		match self {
			Self::DagPb(dag) => dag.cid(config),
			Self::Raw(..) => unimplemented!("Cid of Raw content"),
		}
	}
}
