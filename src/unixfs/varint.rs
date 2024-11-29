use std::io::{Error as IoError, Read};
use thiserror_no_std::Error;

#[derive(Debug, Error)]
pub enum VarintReaderError {
	Varint,
	Io(#[from] IoError),
	Ufg8(#[from] std::string::FromUtf8Error),
}

pub type VarResult<T> = std::result::Result<T, VarintReaderError>;

pub struct VarintRead<'a, R: Read> {
	pub reader: &'a mut R,
}

impl<'a, R: Read> VarintRead<'a, R> {
	pub fn new(reader: &'a mut R) -> Self {
		Self { reader }
	}

	fn read_u8(&mut self) -> VarResult<u8> {
		let mut buf = [0u8; 1];
		self.reader.read_exact(&mut buf)?;
		Ok(buf[0])
	}

	fn read_varint32(&mut self) -> VarResult<u32> {
		let b = self.read_u8()?;
		if b & 0x80 == 0 {
			return Ok(b as u32);
		}
		let mut r = (b & 0x7f) as u32;

		let b = self.read_u8()?;
		r |= ((b & 0x7f) as u32) << 7;
		if b & 0x80 == 0 {
			return Ok(r);
		}

		let b = self.read_u8()?;
		r |= ((b & 0x7f) as u32) << 14;
		if b & 0x80 == 0 {
			return Ok(r);
		}

		let b = self.read_u8()?;
		r |= ((b & 0x7f) as u32) << 21;
		if b & 0x80 == 0 {
			return Ok(r);
		}

		let b = self.read_u8()?; // byte4
		r |= ((b & 0xf) as u32) << 28; // silently prevent overflow; only mask 0xF
		if b & 0x80 == 0 {
			// WARNING ABOUT TRUNCATION
			//
			// In this case, byte4 takes the form 0ZZZ_YYYY where:
			//     Y: part of the resulting 32-bit number
			//     Z: beyond 32 bits (excess bits,not used)
			//
			// If the Z bits were set, it might indicate that the number being
			// decoded was intended to be bigger than 32 bits, suggesting an
			// error somewhere else.
			//
			// However, for the sake of consistency with Google's own protobuf
			// implementation, and also to allow for any efficient use of those
			// extra bits by users if they wish (this crate is meant for speed
			// optimization anyway) we shall not check for this here.
			//
			// Therefore, THIS FUNCTION SIMPLY IGNORES THE EXTRA BITS, WHICH IS
			// ESSENTIALLY A SILENT TRUNCATION!
			return Ok(r);
		}

		// ANOTHER WARNING ABOUT TRUNCATION
		//
		// Again, we do not check whether the byte representation fits within 32
		// bits, and simply ignore extra bytes, CONSTITUTING A SILENT
		// TRUNCATION!
		//
		// Therefore, if the user wants this function to avoid ignoring any
		// bits/bytes, they need to ensure that the input is a varint
		// representing a value within EITHER u32 OR i32 range. Since at this
		// point we are beyond 5 bits, the only possible case is a negative i32
		// (since negative numbers are always 10 bytes in protobuf). We must
		// have exactly 5 bytes more to go.
		//
		// Since we know it must be a negative number, and this function is
		// meant to read 32-bit ints (there is a different function for reading
		// 64-bit ints), the user might want to take care to ensure that this
		// negative number is within valid i32 range, i.e. at least
		// -2,147,483,648. Otherwise, this function simply ignores the extra
		// bits, essentially constituting a silent truncation!
		//
		// What this means in the end is that the user should ensure that the
		// resulting number, once decoded from the varint format, takes such a
		// form:
		//
		// 11111111_11111111_11111111_11111111_1XXXXXXX_XXXXXXXX_XXXXXXXX_XXXXXXXX
		// ^(MSB bit 63)                       ^(bit 31 is set)                  ^(LSB bit 0)

		// discards extra bytes
		for _ in 0..5 {
			if self.read_u8()? & 0x80 == 0 {
				return Ok(r);
			}
		}

		// cannot read more than 10 bytes
		Err(VarintReaderError::Varint)
	}

	fn read_len_varint(&mut self) -> VarResult<Vec<u8>> {
		let len = self.read_varint32()? as usize;
		let mut bytes = vec![0; len];
		self.reader.read_exact(bytes.as_mut_slice())?;
		Ok(bytes)
	}

	#[inline]
	pub fn read_message(&mut self) -> VarResult<Vec<u8>> {
		self.read_len_varint()
	}

	pub fn next_tag(&mut self) -> VarResult<u32> {
		self.read_varint32()
	}

	#[inline]
	pub fn read_bytes(&mut self) -> VarResult<Vec<u8>> {
		self.read_len_varint()
	}
}
