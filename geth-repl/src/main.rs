use geth_client::Client;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let options = glyph::Options::default();
    let mut inputs = glyph::in_memory_inputs(options)?;
    // let mut client = Client::new("http://[::1]:2113").await?;

    while let Some(input) = inputs.next_input()? {
        match input {
            glyph::Input::Exit => break,

            glyph::Input::String(s) => {
                println!(">> {}", s);
            }

            glyph::Input::Command { name, params } => {
                let mut cmd = vec![":".to_string(), name];

                cmd.extend(params);

                match Cmd::from_iter_safe(&cmd) {
                    Err(e) => {
                        println!("{}", e);
                    }

                    Ok(cmd) => match cmd {
                        Cmd::Exit => break,
                        Cmd::ReadStream(opts) => {
                            println!("{:?}", opts);
                        }
                    },
                }
            }

            _ => {}
        }
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = ":")]
enum Cmd {
    #[structopt(name = "read", about = "Read a stream")]
    ReadStream(ReadStream),

    #[structopt(about = "Exit the application")]
    Exit,
}

#[derive(StructOpt, Debug)]
struct ReadStream {
    #[structopt(long)]
    stream_name: String,
}
