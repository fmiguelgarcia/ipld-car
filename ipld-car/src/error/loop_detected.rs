use crate::car::BlockId;

use std::path::PathBuf;

#[cfg_attr(feature = "std", derive(Debug))]
#[derive(thiserror::Error)]
pub enum LoopDetectedErr {
	#[error("There is a symlink loop on path {0:?}")]
	Symlink(PathBuf),
	#[error("Loop detected, block {0:?} needs to update a closed block {1:?}")]
	OnRebuildAncestors(BlockId, BlockId),
	#[error("Loop detected while writing CAR at block {0:?}")]
	OnWrittingCar(BlockId),
}
