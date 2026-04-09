use derive_new::new;

#[derive(Debug, Clone, new)]
pub struct Symlink {
	#[new(into)]
	pub posix_path: String,
}
