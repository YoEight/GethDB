use std::process::Command;

fn main() {
    let files = &["commands", "events"];

    for file in files {
        Command::new("flatc")
            .arg("--rust")
            .current_dir("src")
            .arg(format!("{}.fbs", file))
            .output()
            .unwrap();
    }
}
