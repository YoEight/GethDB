use std::io;

fn main() -> io::Result<()> {
    prost_build::Config::new()
        .bytes([".RecordedEvent.data", ".RecordedEvent.metadata"])
        .format(true)
        .default_package_filename("model")
        .compile_protos(&["protos/events.proto"], &["protos/"])?;

    Ok(())
}
