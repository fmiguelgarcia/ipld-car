use crate::{
	bounded_reader::traits::Bounded as _,
	car::{BlockType, ContentAddressableArchive},
	dag_pb::DagPbType,
	traits::ContextLen,
};

impl<T> ContextLen for ContentAddressableArchive<T> {
	fn data_len(&self) -> u64 {
		self.dag
			.node_weights()
			.map(|block| match &block.r#type {
				BlockType::Raw => block.data.bound_len(),
				BlockType::DagPb(dag_pb) => match &dag_pb.r#type {
					DagPbType::SingleBlockFile => dag_pb.data.bound_len(),
					_ => 0u64,
				},
			})
			.sum()
	}

	fn pb_data_len(&self) -> u64 {
		self.dag.node_weights().map(|block| block.data.bound_len()).sum()
	}
}
