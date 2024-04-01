use std::process::Command;

fn main() {
    Command::new("flatc")
        .arg("--rust")
        .current_dir("src")
        .arg("schema.fbs")
        .output()
        .unwrap();
}
