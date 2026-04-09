# IPLD-CAR & CLI

A Rust implementation of the [CAR v1](https://ipld.io/specs/transport/car/carv1/) formats (well only _Dag-pb_ at the moment), designed for reading and writing content-addressable archives without external IPFS infrastructure.

This workspace contains two crates:

- **`ipld-car`** — library for reading and writing CAR files, with an optional filesystem interface backed by the [`vfs`](https://crates.io/crates/vfs) trait.
- **`carcli`** — command-line tool for inspecting and extracting CAR files.

---

## Library — `ipld-car`

### Filesystem interface

`CarFs<T>` wraps a `ContentAddressableArchive` and implements [`vfs::FileSystem`](https://docs.rs/vfs), giving you a familiar, path-based API over the archive contents.

```rust
use ipld_car::{CarFs, ContentAddressableArchive};
use vfs::FileSystem;
use std::fs::File;

let fs = CarFs::from(ContentAddressableArchive::load(File::open("archive.car")?)?);

// Read a directory
for entry in fs.read_dir("/subdir")? {
    println!("{entry}");
}

// Stream a file to stdout
let mut reader = fs.open_file("/subdir/hello.txt")?;
std::io::copy(&mut reader, &mut std::io::stdout())?;
```

You can also build an archive:

```rust
use ipld_car::{CarFs, Config, ContentAddressableArchive};
use vfs::FileSystem;
use std::{fs::File, io::{BufWriter, Write}};

let fs = CarFs::from(ContentAddressableArchive::new(Config::default())?);

fs.create_dir("/docs")?;
let mut file = fs.create_file("/docs/readme.txt")?;
write!(file, "hello")?;

// NOTE: the block is committed to the CAR on `drop`.
drop(file);

// Extract the CAR from the FS wrapper and write to disk.
let mut car = fs.into_inner().unwrap();
car.write(&mut BufWriter::new(File::create("out.car")?))?;
```

### Features

| Feature        | Default | Description                                              |
| -------------- | ------- | -------------------------------------------------------- |
| `std`          | ✓       | Standard library support, CBOR header, `vfs`             |
| `vfs`          | ✓       | `CarFs<T>` and `vfs::FileSystem` impl (enabled by `std`) |
| `cli`          |         | Exposes `Config` as `clap::Args`                         |
| `jumbo-chunks` |         | Enables chunk sizes from 1 MiB up to 512 MiB             |
| `test_helpers` |         | Exposes test utilities for downstream crates             |

---

## CLI — `carcli`

### Installation

```bash
cargo install --path carcli
```

### Commands

#### `info` — archive summary

```bash
carcli info archive.car
```

```
File:            archive.car
Blocks:          4
  Roots:         1
  Non-roots:     3
Total DAG-PB:    208B
Total Data:      43B
  [0] bafybeietjm63oynimmv5yyqay33nui4y4wx6u3peezwetxgiwvfmelutzu
```

#### `ls` — list directory contents

```bash
carcli ls archive.car               # flat list of /
carcli ls archive.car /subdir       # list a specific path
carcli ls -T -B archive.car         # recursive tree with byte sizes
```

The `-T` flag renders the full directory tree; `-B` shows exact byte counts instead of SI prefixes:

```
Permissions  Size  User   Group  Date Modified         Name
drwxr-xr-x   -     alice  staff  2026-03-12T18:45:02Z  └──  subdir/
.r--r--r--   31B   alice  staff  2026-03-12T18:45:02Z      ├──  ascii.txt
.r--r--r--   12B   alice  staff  2026-03-12T18:45:02Z      └──  hello.txt
```

#### `cat` — print a file

```bash
carcli cat archive.car /subdir/hello.txt
```

```
hello world
```

Streams directly to stdout, so it composes naturally with other Unix tools:

```bash
carcli cat archive.car /data/records.json | jq '.items[]'
carcli cat archive.car /report.pdf > report.pdf
```

#### `create` — pack a directory or file into a CAR file

Recursively adds a local directory or file, preserving the directory structure:

```bash
carcli create out.car ./my-directory
carcli create out.car ./single-file.txt
```

#### `write` — pack individual files into a CAR file

Adds specific files at explicit destination paths using `DEST=SRC` mappings:

```bash
carcli write -o out.car --add /docs/readme.txt=./README.md --add /data/a.json=./a.json
```

---

## License

MIT
