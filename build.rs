use prost_build::Config;

const PROTOS: &[&str] = &["src/schema/unixfs.proto"];

fn main() {
	// prost_build::compile_protos(PROTOS, &["src/schema"]).unwrap();
	let mut config = Config::new();

	config.type_attribute("unixfs.Data", "#[derive(PartialEq)]");
	config.compile_protos(PROTOS, &["src/schema"]).unwrap();
}
