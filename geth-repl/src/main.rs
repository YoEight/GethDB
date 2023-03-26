use std::{fs::File, path::PathBuf};

use geth_client::Client;
use geth_common::{Direction, ExpectedRevision, Propose, Revision};
use geth_mikoshi::Mikoshi;
use structopt::StructOpt;
use uuid::Uuid;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let options = glyph::Options::default();
    let mut inputs = glyph::in_memory_inputs(options)?;
    let mut client = None;
    let mut state = ReplState {
        mikoshi_backend: Mikoshi::in_memory(),
    };

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
                        Cmd::Connect => {
                            if client.is_none() {
                                client = Some(Client::new("http://[::1]:2113").await?);
                            }
                        }
                        Cmd::ReadStream(opts) => {
                            if let Some(client) = client.as_mut() {
                                read_stream(client, opts).await?;
                                continue;
                            }

                            println!("ERR: Not connected")
                        }

                        Cmd::AppendStream(opts) => {
                            if let Some(client) = client.as_mut() {
                                append_stream(client, opts).await?;
                                continue;
                            }

                            println!("ERR: Not connected")
                        }

                        Cmd::Mikoshi(cmd) => match cmd {
                            MikoshiCmd::Root(args) => {
                                // println!();
                                mikoshi_root(&mut state, args)?;
                            }
                        },
                    },
                }
            }
        }
    }

    Ok(())
}

struct ReplState {
    mikoshi_backend: Mikoshi,
}

#[derive(StructOpt, Debug)]
#[structopt(name = ":")]
enum Cmd {
    #[structopt(name = "read", about = "Read a stream")]
    ReadStream(ReadStream),

    #[structopt(name = "append", about = "Append a stream")]
    AppendStream(AppendStream),

    #[structopt(name = "mikoshi", about = "Database files management")]
    Mikoshi(MikoshiCmd),

    #[structopt(name = "connect", about = "Connect to a GethDB node")]
    Connect,

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

#[derive(StructOpt, Debug)]
enum MikoshiCmd {
    #[structopt(name = "root", about = "database root folder")]
    Root(MikoshiRoot),
}

#[derive(StructOpt, Debug)]
struct MikoshiRoot {
    #[structopt(long, short)]
    directory: Option<PathBuf>,
}

async fn read_stream(client: &mut Client, params: ReadStream) -> eyre::Result<()> {
    let mut stream = client
        .read_stream(params.stream_name, Revision::Start, Direction::Forward)
        .await?;

    while let Some(record) = stream.next().await? {
        println!(">> {:?}", record);
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
            r#type: "<repl-type>".to_string(),
            data: serde_json::to_vec(&event)?.into(),
        });
    }

    let result = client
        .append_stream(params.stream_name, proposes, ExpectedRevision::Any)
        .await?;

    println!(">> {:?}", result);
    Ok(())
}

fn mikoshi_root(state: &mut ReplState, args: MikoshiRoot) -> eyre::Result<()> {
    match args.directory {
        None => {
            println!(">> {}", state.mikoshi_backend.root());
        }
        Some(dir) => {
            let new_backend = Mikoshi::esdb(dir)?;
            state.mikoshi_backend = new_backend;

            println!(">> Mikoshi backend root directory was updated successfully");
        }
    }

    Ok(())
}
