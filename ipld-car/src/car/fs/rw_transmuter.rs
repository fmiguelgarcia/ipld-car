use std::{
	convert::Infallible,
	fs::File,
	io::{self, BufReader, BufWriter, IntoInnerError, Read, Write},
};

use bytes::{
	buf::{Reader as BytesReader, Writer as BytesWriter},
	Buf, BufMut, BytesMut,
};

/// It facilitates the transform from `Write` to `Read` and bise versa
pub trait RWTransmuter {
	type Writer: Write + RWTransmuter;
	type Reader: Read;
	type IntoWriterErr;
	type IntoReaderErr;

	fn temporal() -> io::Result<Self>
	where
		Self: Sized;

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr>
	where
		Self: Sized;

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr>
	where
		Self: Sized;
}

impl RWTransmuter for File {
	type Writer = File;
	type Reader = File;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> io::Result<Self> {
		tempfile::tempfile()
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(self)
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		Ok(self)
	}
}

impl<F: RWTransmuter + Write + Read> RWTransmuter for BufReader<F> {
	type Writer = BufWriter<F>;
	type Reader = BufReader<F>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> io::Result<Self> {
		F::temporal().map(BufReader::new)
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(BufWriter::new(self.into_inner()))
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		Ok(self)
	}
}

impl<F: RWTransmuter + Write + Read> RWTransmuter for BufWriter<F> {
	type Writer = BufWriter<F>;
	type Reader = BufReader<F>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = IntoInnerError<Self>;

	fn temporal() -> io::Result<Self> {
		F::temporal().map(BufWriter::new)
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(self)
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		self.into_inner().map(BufReader::new)
	}
}

impl RWTransmuter for BytesMut {
	type Writer = BytesWriter<BytesMut>;
	type Reader = BytesReader<BytesMut>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> io::Result<Self> {
		Ok(BytesMut::new())
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(self.writer())
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		Ok(self.reader())
	}
}

impl RWTransmuter for BytesWriter<BytesMut> {
	type Writer = BytesWriter<BytesMut>;
	type Reader = BytesReader<BytesMut>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> io::Result<Self> {
		Ok(BytesMut::new().writer())
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(self)
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		Ok(self.into_inner().reader())
	}
}

impl RWTransmuter for BytesReader<BytesMut> {
	type Writer = BytesWriter<BytesMut>;
	type Reader = BytesReader<BytesMut>;
	type IntoWriterErr = Infallible;
	type IntoReaderErr = Infallible;

	fn temporal() -> io::Result<Self> {
		Ok(BytesMut::new().reader())
	}

	fn into_writer(self) -> Result<Self::Writer, Self::IntoWriterErr> {
		Ok(self.into_inner().writer())
	}

	fn into_reader(self) -> Result<Self::Reader, Self::IntoReaderErr> {
		Ok(self)
	}
}
