/// Defines the layout of the Directed Acyclic Graph (DAG).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub enum DAGLayout {
	#[default]
	Flat,
	Balanced(MaxChildren),
	Trickle(MaxChildren, LayerRepeats),
}

impl DAGLayout {
	pub fn max_children_per_layer(&self) -> u32 {
		match self {
			Self::Flat => u32::MAX,
			Self::Balanced(max) => *max as u8 as u32,
			Self::Trickle(max, _) => *max as u8 as u32,
		}
	}
}

/// Maximum children count for balanced/trickle DAG layouts (future use).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Default, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MaxChildren {
	C11 = 11,
	C44 = 44,
	#[default]
	C174 = 174,
}

/// Number of times a layer repeats in trickle DAG layout (future use).
#[cfg_attr(feature = "std", derive(Debug))]
#[derive(Clone, Copy, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum LayerRepeats {
	LR1 = 1,
	LR4 = 4,
	#[default]
	LR16 = 16,
}
