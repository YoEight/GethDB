fn main() -> std::io::Result<()> {
    let options = glyph::Options::default();
    let mut inputs = glyph::in_memory_inputs(options)?;

    while let Some(input) = inputs.next_input()? {
        match input {
            glyph::Input::Exit => break,

            glyph::Input::String(s) => {
                println!(">> {}", s);
            }

            _ => {}
        }
    }

    Ok(())
}
