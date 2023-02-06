use std::{fs::File, path::PathBuf};

use geth_client::Client;
use geth_common::{Direction, ExpectedRevision, Propose, Revision};
use structopt::StructOpt;
use uuid::Uuid;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let options = glyph::Options::default();
    let mut inputs = glyph::in_memory_inputs(options)?;
    let mut client = Client::new("http://[::1]:2113").await?;

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
                            read_stream(&mut client, opts).await?;
                        }

                        Cmd::AppendStream(opts) => {
                            append_stream(&mut client, opts).await?;
                        }
                    },
                }
            }
        }
    }

    Ok(())
}

#[derive(StructOpt, Debug)]
#[structopt(name = ":")]
enum Cmd {
    #[structopt(name = "read", about = "Read a stream")]
    ReadStream(ReadStream),

    #[structopt(name = "append", about = "Append a stream")]
    AppendStream(AppendStream),

    #[structopt(about = "Exit the application")]
    Exit,
}

#[derive(StructOpt, Debug)]
struct ReadStream {
    #[structopt(long, short)]
    stream_name: String,
}

#[derive(StructOpt, Debug)]
struct AppendStream {
    #[structopt(long, short)]
    stream_name: String,

    #[structopt(long, short = "f")]
    json_file: PathBuf,
}

async fn read_stream(client: &mut Client, params: ReadStream) -> eyre::Result<()> {
    let mut stream = client
        .read_stream(params.stream_name, Revision::Start, Direction::Forward)
        .await?;

    while let Some(record) = stream.next().await? {
        let data = serde_json::from_slice::<serde_json::Value>(&record.data)?;
        let record = serde_json::json!({
            "stream_name": record.stream_name,
            "id": record.id,
            "revision": record.revision,
            "position": record.position.raw(),
            "data": data,
        });

        println!("{}", serde_json::to_string_pretty(&record)?);
    }

    println!();
    Ok(())
}

async fn append_stream(client: &mut Client, params: AppendStream) -> eyre::Result<()> {
    let file = File::open(params.json_file)?;
    let events = serde_json::from_reader::<_, Vec<serde_json::Value>>(file)?;
    let mut proposes = Vec::new();

    for event in events {
        proposes.push(Propose {
            id: Uuid::new_v4(),
            data: serde_json::to_vec(&event)?.into(),
        });
    }

    client
        .append_stream(params.stream_name, proposes, ExpectedRevision::Any)
        .await?;

    println!();
    Ok(())
}
