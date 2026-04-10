# Changelog

All notable changes to this project are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-04-10

### Added

- CAR v1 wire-format reader and writer (`ContentAddressableArchive`)
- VFS-compatible filesystem interface (`CarFs<T>`) backed by an in-memory block DAG
- `carcli` binary with `info`, `ls`, `cat`, and `write` subcommands

[0.1.0]: https://github.com/fmiguelgarcia/ipld-car/releases/tag/v0.1.0
