use ipfs_unixfs::FileSystemWriter;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use quick_protobuf::message::MessageWrite as _;
use rand::prelude::*;
use std::{
	fs::File,
	io::{BufReader, BufWriter, Seek as _, Write as _},
};
use tempfile::tempfile;

fn gen_file(size_kb: usize) -> BufReader<File> {
	let mut rng = rand::thread_rng();
	let mut rng_data = [0u8; 1_024];

	let mut writer = BufWriter::new(tempfile().unwrap());
	for _ in 0..size_kb {
		rng.fill(&mut rng_data);
		writer.write_all(&rng_data).unwrap();
	}

	let mut file = writer.into_inner().unwrap();
	file.rewind().unwrap();

	BufReader::new(file)
}

fn bench_fs_writer(c: &mut Criterion) {
	let mut group = c.benchmark_group("fs_writer");
	for size_kb in [2usize, 32, 512, 4 * 1_024, 32 * 1_024, 256 * 1_024, 1_024 * 1_024].iter() {
		group.throughput(Throughput::Bytes((*size_kb * 1_024) as u64));
		group.bench_with_input(BenchmarkId::from_parameter(size_kb), size_kb, |b, &size_kb| {
			b.iter_batched(
				|| gen_file(size_kb),
				|file| {
					let unixfs = FileSystemWriter::default().add_data(file, "").build().unwrap();

					let node_enc_len = unixfs.node.as_ref().map(|node| node.get_size()).unwrap_or_default();
					let package_len = node_enc_len + size_kb * 1_024;
					assert_eq!(unixfs.len(), package_len as u64);
				},
				BatchSize::PerIteration,
			);
		});
	}
	group.finish();
}

criterion_group!(benches, bench_fs_writer);
criterion_main!(benches);
