use ipfs_unixfs::{ConfigBuilder, FileSystemReader, FileSystemWriter, LeafPolicy, WellKnownChunkSize};
use std::io::BufReader;

fn main() {
	// Crear un archivo de ejemplo con múltiples chunks
	let data = b"Hello World! ".repeat(2000); // ~24KB
	let cursor = std::io::Cursor::new(data);
	let reader = BufReader::new(cursor);

	// Configurar con chunks pequeños para generar múltiples chunks
	let config = ConfigBuilder::default()
		.leaf_policy(LeafPolicy::Raw)
		.chunk_policy(WellKnownChunkSize::F1KiB.into())
		.build()
		.unwrap();

	// Escribir el archivo a UnixFS
	let unixfs = FileSystemWriter::default().config(config).add_data(reader, "example.txt").build().unwrap();

	let cid = unixfs.cid;
	println!("CID del archivo: {}", cid);

	// Cargar con FileSystemReader
	let fs = FileSystemReader::try_from(unixfs).unwrap();

	// Iterar sobre los links con offsets
	println!("\nChunks del archivo:");
	println!("{:-<80}", "");
	println!("{:<5} {:<52} {:<10} {:<10}", "Index", "CID", "Offset", "Size");
	println!("{:-<80}", "");

	for (i, link) in fs.links_with_offsets(".").unwrap().enumerate() {
		println!("{i:<5} {:<52} {:<10} {:<10}", link.cid, link.absolute_offset, link.chunk_size());
	}

	println!("{:-<80}", "");
}
