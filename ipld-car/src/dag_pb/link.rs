use derive_more::From;
use derive_new::new;

#[derive(From, Debug)]
pub enum Link {
	Named(#[from] NamedLink),
	Block(#[from] BlockLink),
}

impl Link {
	pub fn name(&self) -> Option<&str> {
		match self {
			Self::Named(named) => Some(named.name.as_str()),
			Self::Block(..) => None,
		}
	}

	pub fn cumulative_dag_size(&self) -> u64 {
		match self {
			Self::Named(..) => 0u64,
			Self::Block(bl) => bl.cumulative_dag_size,
		}
	}
}

#[derive(new, Debug)]
pub struct NamedLink {
	#[new(into)]
	pub name: String,
}

#[derive(new, Debug)]
pub struct BlockLink {
	pub cumulative_dag_size: u64,
}
