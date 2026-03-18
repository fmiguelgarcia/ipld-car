use thiserror::Error;

#[derive(Error, Debug)]
pub enum CidErr {
	#[error("CID codec `{0}` is not supported")]
	CodecNotSupported(u64),
	#[error("CID cannot be parse")]
	Parse(#[from] libipld::cid::Error),
}
