use ipfs_unixfs::FileSystemWriter;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use humansize::{format_size, BINARY};
use quick_protobuf::message::MessageWrite as _;
use rand::prelude::*;
use std::{
	cmp::min,
	fs::File,
	io::{BufReader, BufWriter, Seek as _, Write as _},
};
use tempfile::tempfile;
use uom::si::{
	information::{byte, gibibyte as GiB, kibibyte as KiB, mebibyte as MiB},
	usize::Information,
};

fn gen_file(size: Information) -> BufReader<File> {
	static RNG_BUF_LEN: usize = 4_096;
	let mut rng = rand::thread_rng();
	let mut rng_data = [0u8; RNG_BUF_LEN];

	let mut writer = BufWriter::new(tempfile().unwrap());
	let mut size = size.get::<byte>();

	while size != 0 {
		let max_copiable = min(size, RNG_BUF_LEN);
		rng.fill(&mut rng_data[..max_copiable]);
		writer.write_all(&rng_data[..max_copiable]).unwrap();
		size = size.saturating_sub(max_copiable)
	}

	let mut file = writer.into_inner().unwrap();
	file.rewind().unwrap();

	BufReader::new(file)
}

fn bench_fs_writer(c: &mut Criterion) {
	let mut group = c.benchmark_group("fs_writer");
	for bench_conf in [
		(Information::new::<KiB>(2), BatchSize::PerIteration),
		(Information::new::<KiB>(32), BatchSize::PerIteration),
		(Information::new::<KiB>(512), BatchSize::PerIteration),
		(Information::new::<MiB>(4), BatchSize::PerIteration),
		(Information::new::<MiB>(32), BatchSize::PerIteration),
		(Information::new::<MiB>(256), BatchSize::NumIterations(5)),
		(Information::new::<GiB>(1), BatchSize::NumIterations(5)),
	]
	.iter()
	{
		let (bench_size, batch_size) = bench_conf;
		let size_bytes = bench_size.get::<byte>();
		// let bench_name = format!("fs_writer_{}", );
		let bench_id = BenchmarkId::new("fs_writer", format_size(size_bytes, BINARY));
		group.throughput(Throughput::Bytes(size_bytes as u64));
		group.bench_with_input(bench_id, bench_size, |b, &size| {
			b.iter_batched(
				|| gen_file(size),
				|file| {
					let unixfs = FileSystemWriter::default().add_data(file, "").build().unwrap();

					let node_enc_len = unixfs.node.as_ref().map(|node| node.get_size()).unwrap_or_default();
					let package_len = node_enc_len + size.get::<byte>();
					assert_eq!(unixfs.len(), package_len as u64);
				},
				*batch_size,
			);
		});
	}
	group.finish();
}

criterion_group!(benches, bench_fs_writer);
criterion_main!(benches);
