use geth_client::Client;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let options = glyph::Options::default();
    let mut inputs = glyph::in_memory_inputs(options)?;
    let mut client = Client::new("[::1]:2113").await?;

    while let Some(input) = inputs.next_input()? {
        match input {
            glyph::Input::Exit => break,

            glyph::Input::String(s) => {
                println!(">> {}", s);
            }

            glyph::Input::Command { name, params } => {}

            _ => {}
        }
    }

    Ok(())
}
