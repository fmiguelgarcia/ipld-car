use ciborium::tag::Required;
use derive_more::{AsRef, Constructor, Deref, From, Into};
use libipld::Cid;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;
#[cfg(feature = "std")]
use std::fmt;

///_Serialize a CID using `tag(42)` + `0x00` + cid_bytes
#[derive(From, Into, Constructor, PartialEq, Eq, Clone, AsRef, Deref)]
pub struct CborCid(pub Cid);

impl Serialize for CborCid {
	fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
		// Prepend `0x00`
		let cid_bytes = self.0.to_bytes();
		let mut payload = Vec::with_capacity(1 + cid_bytes.len());
		payload.push(0x00);
		payload.extend_from_slice(&cid_bytes);

		// Add tag(42) usando ciborium::tag::Required<_, 42>
		let tagged = Required::<ByteBuf, 42>(ByteBuf::from(payload));
		tagged.serialize(s)
	}
}

impl<'de> Deserialize<'de> for CborCid {
	fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
		let Required::<ByteBuf, 42>(bytes) = Required::deserialize(d)?;

		if bytes[0] != 0x00 {
			return Err(de::Error::custom(format!("Multibase prefix invalid: 0x{:02x} (esperado 0x00)", bytes[0])));
		}

		let cid = Cid::try_from(&bytes[1..]).map_err(|e| de::Error::custom(format!("CID invalid: {}", e)))?;

		Ok(Self(cid))
	}
}

#[cfg(feature = "std")]
impl fmt::Debug for CborCid {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str(&self.0.to_string())
	}
}
