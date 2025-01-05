mod indexing;
mod interactions;
mod reading;
mod subscribing;
mod writing;

#[ctor::ctor]
fn test_init() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_target(true)
        .init();
}
