pub trait ContextLen {
	fn data_len(&self) -> u64;
	fn pb_data_len(&self) -> u64;
}
