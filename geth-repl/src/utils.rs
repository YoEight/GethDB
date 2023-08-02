use directories::UserDirs;
use std::path::{Component, PathBuf};

pub fn expand_path(path: PathBuf) -> PathBuf {
    let mut expanded = PathBuf::new();
    let user_dirs = UserDirs::new();

    for component in path.components() {
        if let Component::Normal(os_str) = component {
            if let Some("~") = os_str.to_str() {
                if let Some(user_dirs) = &user_dirs {
                    expanded.push(user_dirs.home_dir());
                    continue;
                }
            }
        }

        expanded.push(component);
    }

    expanded
}
