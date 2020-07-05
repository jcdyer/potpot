use std::path::{Path, PathBuf};

/// TempPath acts as a PathBuf, but removes any file found at the
/// path when it goes out of scope.
pub(crate) struct TempPath {
    path: PathBuf,
}

impl TempPath {
    pub(crate) fn new(path: PathBuf) -> TempPath {
        TempPath { path }
    }
}
impl Drop for TempPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self);
    }
}

impl std::convert::AsRef<Path> for TempPath {
    fn as_ref(&self) -> &Path {
        &self.path
    }
}

impl std::ops::Deref for TempPath {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.path
    }
}


pub(crate) fn create_test_path<P: AsRef<Path>>(p: P) -> TempPath {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("data");
    path.push("test");
    path.push(p);
    TempPath::new(path)
}
