use crate::bounded_reader::traits::{Bounded, BoundedIndex, CloneAndRewind};
use std::{io::Read, ops::RangeInclusive};

pub fn slice_ref<R>(reader: R, pattern: &[u8]) -> Option<R>
where
	R: Read + CloneAndRewind + Bounded,
	RangeInclusive<u64>: BoundedIndex<R>,
{
	if pattern.is_empty() {
		return None;
	}

	const BUF_SIZE: usize = 4096;
	let mut buf = [0u8; BUF_SIZE];

	let last_reader_pos = reader.bound_len().saturating_sub(pattern.len() as u64 - 1);
	let mut reader_pos = 0u64;
	let mut pattern_pos = 0usize;

	let mut src = reader.clone_and_rewind();
	loop {
		let n = match src.read(&mut buf) {
			Ok(0) | Err(_) => break,
			Ok(n) => n,
		};
		for &byte in &buf[..n] {
			if reader_pos > last_reader_pos && pattern_pos == 0 {
				return None;
			}

			if byte == pattern[pattern_pos] {
				pattern_pos += 1;
				if pattern_pos >= pattern.len() {
					let start = reader_pos.saturating_sub(pattern.len() as u64 - 1);
					return Some(reader.clamped_sub(start..=reader_pos));
				}
			} else {
				pattern_pos = 0;
			}

			reader_pos += 1;
		}
	}

	None
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::bounded_reader::sync::BoundedReader;

	use anyhow::Result;
	use std::io::{self, Cursor};
	use test_case::case;

	#[case( b"Hello world!!!", b"world", Some(b"world"); "middle")]
	#[case( b"Hello world!!!", b"Hello", Some(b"Hello"); "begin")]
	#[case( b"Hello world!!!", b"ld!!!", Some(b"ld!!!"); "end")]
	#[case( b"Hello world!!!", b"universe", None; "none")]
	fn slice_ref_test(data: &[u8], pattern: &[u8], exp_slice: Option<&[u8]>) -> Result<()> {
		let reader = BoundedReader::from_reader(Cursor::new(data))?;
		let slice = slice_ref(reader, pattern)
			.map(|mut sliced_reader| {
				let mut content = Vec::<u8>::with_capacity(sliced_reader.bound_len() as usize);
				sliced_reader.read_to_end(&mut content)?;
				Ok::<_, io::Error>(content)
			})
			.transpose()?;

		assert_eq!(slice.as_deref(), exp_slice);
		Ok(())
	}
}
