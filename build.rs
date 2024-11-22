
const PROTOS :&[&str]= &["src/schema/unixfs.proto"];

fn main() {
	prost_build::compile_protos(PROTOS, &["src/schema"]).unwrap();
}
