use std::path::Path;
use std::{fs, fs::File, io, path::PathBuf};

use cli::AppendStream;
use directories::UserDirs;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use geth_engine::{start_process_manager_with_catalog, Catalog, Options, Proc};
use glyph::{FileBackedInputs, Input, PromptOptions};
use local::LocalClient;
use serde::Deserialize;
use uuid::Uuid;

use geth_client::GrpcClient;
use geth_common::{
    AppendError, AppendStreamCompleted, Client, DeleteError, DeleteStreamCompleted, Direction,
    EndPoint, ExpectedRevision, GetProgramError, ProgramObtained, Propose, Record, Revision,
    SubscriptionEvent,
};

use crate::cli::{
    Cli, Mikoshi, MikoshiCommands, Offline, OfflineCommands, Online, OnlineCommands,
    ProcessCommands, ReadStream, SubscribeCommands,
};
use crate::utils::expand_path;

mod cli;
mod local;
mod utils;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let user_dirs = UserDirs::new().expect("to be defined");
    let history_path = PathBuf::from(user_dirs.home_dir()).join(".geth-repl");
    let options = glyph::Options::default()
        .header(include_str!("header.txt"))
        .author("Yo Eight")
        .version("master")
        .disable_free_expression();
    let mut inputs = glyph::file_backed_inputs(options, history_path)?;
    let mut repl_state = ReplState::Offline;

    while let Some(input) = repl_state.next_input(&mut inputs)? {
        match input {
            Input::Exit => break,

            Input::String(s) => {
                println!(">> {}", s);
            }

            Input::Command(cmd) => match cmd {
                Cli::Offline(cmd) => match cmd.command {
                    OfflineCommands::Connect { host, port } => {
                        let host = host.unwrap_or_else(|| "localhost".to_string());
                        let port = port.unwrap_or(2_113);

                        repl_state = ReplState::Online(OnlineState {
                            host: host.clone(),
                            port,
                            client: GrpcClient::new(EndPoint::new(host.clone(), port)),
                        });
                    }

                    OfflineCommands::Mikoshi { directory } => {
                        let directory = expand_path(directory);
                        let catalog = Catalog::builder()
                            .register(Proc::Writing)
                            .register(Proc::Reading)
                            .register(Proc::PubSub)
                            .register(Proc::Indexing)
                            .build();

                        let options = Options::new(
                            "<repl>".to_string(),
                            0,
                            directory.clone().to_string_lossy().to_string(),
                        );

                        let mgr = start_process_manager_with_catalog(options, catalog).await?;
                        let client = LocalClient::new(mgr).await?;
                        repl_state = ReplState::Mikoshi(MikoshiState { directory, client });
                    }

                    _ => unreachable!(),
                },

                Cli::Online(cmd) => match cmd.command {
                    OnlineCommands::Read(args) => {
                        let state = repl_state.online();
                        read_stream(&state.client, &args).await;
                    }

                    OnlineCommands::Subscribe(cmd) => {
                        let state = repl_state.online();
                        let mut stream = match cmd.command {
                            SubscribeCommands::Stream(opts) => {
                                state
                                    .client
                                    .subscribe_to_stream(&opts.stream, Revision::Start)
                                    .await
                            }

                            SubscribeCommands::Program(opts) => {
                                let source_code = match fs::read_to_string(opts.path.as_path()) {
                                    Err(e) => {
                                        println!(
                                            "ERR: error when reading file {:?}: {}",
                                            opts.path, e
                                        );
                                        continue;
                                    }

                                    Ok(string) => string,
                                };

                                state
                                    .client
                                    .subscribe_to_process(&opts.name, &source_code)
                                    .await
                            }
                        };

                        let reading = Box::pin(async_stream::try_stream! {
                            while let Some(event) = stream.try_next().await? {
                                if let SubscriptionEvent::EventAppeared(record) = event {
                                    yield record;
                                }
                            }
                        });

                        display_stream(reading).await;
                    }

                    OnlineCommands::Disconnect => {
                        repl_state = ReplState::Offline;
                    }

                    OnlineCommands::Process(cmd) => {
                        let state = repl_state.online();
                        match cmd.commands {
                            ProcessCommands::Kill { id } => {
                                kill_programmable_subscription(state, id).await;
                            }

                            ProcessCommands::Stats { id } => {
                                get_programmable_subscription_stats(state, id).await;
                            }

                            ProcessCommands::List => {
                                list_programmable_subscriptions(state).await;
                            }
                        }
                    }

                    OnlineCommands::Append(opts) => {
                        let state = repl_state.online();
                        append_stream(&state.client, &opts).await;
                    }

                    OnlineCommands::Delete(opts) => {
                        let state = repl_state.online();

                        match state
                            .client
                            .delete_stream(opts.stream.as_str(), ExpectedRevision::Any)
                            .await
                        {
                            Err(e) => {
                                println!("ERR: error when deleting stream {}: {}", opts.stream, e);
                            }

                            Ok(result) => {
                                match result {
                                    DeleteStreamCompleted::Error(e) => match e {
                                        DeleteError::WrongExpectedRevision(e) => {
                                            println!(
                                            "ERR: wrong expected revision when deleting stream '{}', expected: {} but got {}",
                                            opts.stream,
                                            e.expected,
                                            e.current,
                                        );
                                        }
                                        DeleteError::NotLeaderException(_) => {
                                            println!("ERR: not leader exception when deleting stream '{}'", opts.stream);
                                        }

                                        DeleteError::StreamDeleted => {
                                            println!(
                                                "ERR: stream '{}' was already deleted",
                                                opts.stream
                                            );
                                        }
                                    },

                                    DeleteStreamCompleted::Success(p) => {
                                        println!(
                                            "stream '{}' deletion successful, position {}",
                                            opts.stream,
                                            p.position.raw(),
                                        );
                                    }
                                }
                            }
                        }
                    }

                    OnlineCommands::Exit => unreachable!(),
                },

                Cli::Mikoshi(cmd) => match cmd.commands {
                    MikoshiCommands::Read(args) => {
                        let state = repl_state.mikoshi();
                        read_stream(&state.client, &args).await;
                    }

                    MikoshiCommands::Append(args) => {
                        let state = repl_state.mikoshi();
                        append_stream(&state.client, &args).await
                    }

                    MikoshiCommands::Leave => {
                        repl_state = ReplState::Offline;
                    }
                },
            },
        }
    }

    Ok(())
}

enum ReplState {
    Offline,
    Online(OnlineState),
    Mikoshi(MikoshiState),
}

impl ReplState {
    fn next_input(&self, input: &mut FileBackedInputs) -> io::Result<Option<Input<Cli>>> {
        match self {
            ReplState::Offline => {
                let cmd = input.next_input_with_parser_and_options::<Offline>(
                    &PromptOptions::default().prompt("offline"),
                )?;

                Ok(cmd.map(|i| {
                    i.flat_map(|c| match c.command {
                        OfflineCommands::Exit => Input::Exit,
                        other => Input::Command(Cli::Offline(Offline { command: other })),
                    })
                }))
            }

            ReplState::Online(state) => {
                let prompt = format!("online {}:{}", state.host, state.port);
                let cmd = input.next_input_with_parser_and_options::<Online>(
                    &PromptOptions::default().prompt(prompt),
                )?;

                Ok(cmd.map(|i| {
                    i.flat_map(|c| match c.command {
                        OnlineCommands::Exit => Input::Exit,
                        other => Input::Command(Cli::Online(Online { command: other })),
                    })
                }))
            }

            ReplState::Mikoshi(state) => {
                let prompt = format!("db_directory {:?}", state.directory);
                let cmd = input.next_input_with_parser_and_options::<Mikoshi>(
                    &PromptOptions::default().prompt(prompt),
                )?;

                Ok(cmd.map(|i| i.map(Cli::Mikoshi)))
            }
        }
    }

    fn online(&mut self) -> &mut OnlineState {
        if let ReplState::Online(state) = self {
            return state;
        }

        unreachable!()
    }

    fn mikoshi(&mut self) -> &mut MikoshiState {
        if let ReplState::Mikoshi(state) = self {
            return state;
        }

        unreachable!()
    }
}

struct OnlineState {
    host: String,
    port: u16,
    client: GrpcClient,
}

struct MikoshiState {
    directory: PathBuf,
    client: LocalClient,
}

#[derive(Deserialize)]
struct JsonEvent {
    r#type: String,
    payload: serde_json::Value,
}

fn load_events_from_file(path: impl AsRef<Path>) -> eyre::Result<Vec<Propose>> {
    let file = File::open(path)?;
    let events = serde_json::from_reader::<_, Vec<JsonEvent>>(file)?;
    let mut proposes = Vec::new();

    for event in events {
        proposes.push(Propose {
            id: Uuid::new_v4(),
            content_type: geth_common::ContentType::Json,
            class: event.r#type,
            data: serde_json::to_vec(&event.payload)?.into(),
        });
    }

    Ok(proposes)
}

async fn append_stream<C>(client: &C, opts: &AppendStream)
where
    C: Client + 'static,
{
    let proposes = match load_events_from_file(&opts.json) {
        Err(e) => {
            println!(
                "ERR: error when loading events from file {:?}: {}",
                opts.json, e
            );
            return;
        }
        Ok(es) => es,
    };

    match client
        .append_stream(&opts.stream, ExpectedRevision::Any, proposes)
        .await
    {
        Err(e) => {
            println!(
                "ERR: error when appending events to stream {}: {}",
                opts.stream, e
            );
        }

        Ok(result) => match result {
            AppendStreamCompleted::Error(e) => match e {
                AppendError::StreamDeleted => {
                    println!("ERR: stream '{}' has been deleted", opts.stream);
                }
                AppendError::WrongExpectedRevision(_) => {
                    println!("ERR: {}", e);
                }
            },
            AppendStreamCompleted::Success(result) => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "position": result.position.raw(),
                        "next_expected_version": result.next_expected_version.raw(),
                        "next_logical_position": result.next_logical_position,
                    }))
                    .unwrap()
                );
            }
        },
    }
}

async fn read_stream<C>(client: &C, opts: &ReadStream)
where
    C: Client + 'static,
{
    let reading = client
        .read_stream(
            opts.stream.as_str(),
            Direction::Forward,
            Revision::Start,
            50,
        )
        .await;

    display_stream(reading).await
}

async fn display_stream(mut stream: BoxStream<'static, eyre::Result<Record>>) {
    loop {
        match stream.try_next().await {
            Err(e) => {
                println!("ERR: error when reading stream: {}", e);
                break;
            }

            Ok(Some(record)) => {
                let data = serde_json::from_slice::<serde_json::Value>(&record.data).unwrap();
                let record = serde_json::json!({
                    "stream_name": record.stream_name,
                    "id": record.id,
                    "revision": record.revision,
                    "position": record.position.raw(),
                    "data": data,
                });

                println!("{}", serde_json::to_string_pretty(&record).unwrap());
            }

            _ => break,
        }
    }
}

async fn list_programmable_subscriptions(state: &mut OnlineState) {
    let summaries = match state.client.list_programs().await {
        Err(e) => {
            println!("Err: Error when listing programmable subscriptions: {}", e);
            return;
        }

        Ok(s) => s,
    };

    for summary in summaries {
        let summary = serde_json::json!({
            "id": summary.id,
            "name": summary.name,
            "started_at": summary.started_at,
        });

        println!("{}", serde_json::to_string_pretty(&summary).unwrap());
    }
}

async fn kill_programmable_subscription(state: &mut OnlineState, id: String) {
    let id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(e) => {
            println!(
                "Err: provided programmable subscription id is not a valid UUID: {}",
                e
            );

            return;
        }
    };

    if let Err(e) = state.client.kill_program(id).await {
        println!("Err: Error when killing programmable subscription: {}", e);
    }
}

async fn get_programmable_subscription_stats(state: &mut OnlineState, id: String) {
    let id = match id.parse::<Uuid>() {
        Ok(id) => id,
        Err(e) => {
            println!(
                "Err: provided programmable subscription id is not a valid UUID: {}",
                e
            );

            return;
        }
    };

    let stats = match state.client.get_program(id).await {
        Err(e) => {
            println!("Err: Error when getting programmable subscription: {}", e);
            return;
        }

        Ok(prog) => match prog {
            ProgramObtained::Success(stats) => stats,
            ProgramObtained::Error(e) => match e {
                GetProgramError::NotExists => {
                    println!(
                        "Err: programmable subscription with id {} does not exist",
                        id
                    );
                    return;
                }
            },
        },
    };

    // let source_code = stats.source_code;

    let js = serde_json::json!({
        "id": stats.id,
        "name": stats.name,
        "started": stats.started,
        "subscriptions": stats.subscriptions,
        "pushed_events": stats.pushed_events,
    });

    println!("{}", serde_json::to_string_pretty(&js).unwrap());
    println!("Source code:");
    println!("{}", stats.source_code);
}
