use crate::{config::CidCodec, error::Result, Config, ReaderWithLen};

use libipld::{multihash::MultihashDigest, pb::PbNode, Cid};
use std::io::copy;

pub trait DagPbCidDefaultBuilder {}

pub trait CIDBuilder {
	fn cid(&self, config: &Config) -> Result<Cid>;
}

impl<T> CIDBuilder for T
where
	T: DagPbCidDefaultBuilder,
	PbNode: for<'a> From<&'a T>,
{
	fn cid(&self, config: &Config) -> Result<Cid> {
		let pb_ndoe = PbNode::from(self);
		let mut hasher = config.hasher()?;
		let ReaderWithLen { mut reader, len: _ } = ReaderWithLen::from(pb_ndoe);

		let _ = copy(&mut reader, &mut hasher)?;
		let digest = config.hash_code.wrap(hasher.finalize())?;
		let cid = Cid::new_v1(CidCodec::DagPb as u64, digest);
		Ok(cid)
	}
}
