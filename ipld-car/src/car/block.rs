use crate::{
	bounded_reader::{sync::BoundedReader, traits::Bounded},
	dag_pb::{DagPb, DagPbType},
	traits::ContextLen,
};

use derivative::Derivative;
use libipld::Cid;

#[derive(Derivative, derive_more::Debug)]
#[derivative(Clone(bound = ""))]
pub enum BlockType<T> {
	Raw,
	DagPb(DagPb<T>),
}

#[derive(Derivative, derive_more::Debug)]
#[derivative(Clone(bound = ""))]
pub struct Block<T> {
	pub cid: Cid,
	pub r#type: BlockType<T>,
	pub data: BoundedReader<T>,
}

impl<T> Block<T> {
	pub fn new_raw<D>(cid: Cid, data: D) -> Self
	where
		D: Into<BoundedReader<T>>,
	{
		Self { cid, data: data.into(), r#type: BlockType::Raw }
	}

	pub fn new_dag_pb<PB, D>(cid: Cid, dag_pb: PB, data: D) -> Self
	where
		D: Into<BoundedReader<T>>,
		PB: Into<DagPb<T>>,
	{
		Self { cid, data: data.into(), r#type: BlockType::DagPb(dag_pb.into()) }
	}

	pub fn dag_pb_type(&self) -> Option<&DagPbType> {
		match &self.r#type {
			BlockType::Raw => None,
			BlockType::DagPb(dag) => Some(&dag.r#type),
		}
	}
}

impl<T> ContextLen for Block<T> {
	fn data_len(&self) -> u64 {
		self.data.bound_len()
	}

	fn pb_data_len(&self) -> u64 {
		match &self.r#type {
			BlockType::Raw => self.data.bound_len(),
			BlockType::DagPb(dag_pb) => dag_pb.data.bound_len() + self.data.bound_len(),
		}
	}
}
