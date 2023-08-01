mod cli;
mod utils;

use directories::UserDirs;
use glyph::{FileBackedInputs, Input};
use std::collections::VecDeque;
use std::path::Path;
use std::{fs, fs::File, io, path::PathBuf};

use crate::cli::{
    Cli, Mikoshi, MikoshiCommands, Offline, OfflineCommands, Online, OnlineCommands,
    ProcessCommands, ReadStream, SubscribeCommands,
};
use crate::utils::expand_path;
use geth_client::Client;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::{Lsm, LsmSettings};
use geth_mikoshi::storage::{FileSystemStorage, Storage};
use geth_mikoshi::wal::ChunkManager;
use geth_mikoshi::IteratorIO;
use serde::Deserialize;
use uuid::Uuid;

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let user_dirs = UserDirs::new().expect("to be defined");
    let history_path = PathBuf::from(user_dirs.home_dir()).join(".geth-repl");
    let options = glyph::Options::default()
        .header(include_str!("header.txt"))
        .author("Yo Eight")
        .version("master");
    let mut inputs = glyph::file_backed_inputs(options, history_path)?;
    let mut repl_state = NewReplState::Offline;

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

                        match Client::new(format!("http://{}:{}", host, port)).await {
                            Err(e) => {
                                println!("ERR: error when connecting to {}:{}: {}", host, port, e)
                            }
                            Ok(client) => {
                                repl_state =
                                    NewReplState::Online(OnlineState { host, port, client });
                            }
                        }
                    }

                    OfflineCommands::Mikoshi { directory } => {
                        let directory = expand_path(directory);
                        let storage = match FileSystemStorage::new(directory.clone()) {
                            Err(e) => {
                                println!(
                                    "ERR: Error when loading {:?} as a GethDB root directory: {}",
                                    directory, e
                                );
                                continue;
                            }
                            Ok(s) => s,
                        };

                        let index = match Lsm::load(LsmSettings::default(), storage.clone()) {
                            Err(e) => {
                                println!(
                                    "ERR: Error when loading index data structure in {:?}: {}",
                                    directory, e
                                );
                                continue;
                            }

                            Ok(i) => i,
                        };

                        let manager = match ChunkManager::load(storage.clone()) {
                            Err(e) => {
                                println!(
                                    "ERR: Error when loading chunk manager in {:?}: {}",
                                    directory, e
                                );
                                continue;
                            }

                            Ok(m) => m,
                        };

                        repl_state = NewReplState::Mikoshi(MikoshiState {
                            directory,
                            storage,
                            index,
                            manager,
                        });
                    }

                    _ => unreachable!(),
                },

                Cli::Online(cmd) => match cmd.command {
                    OnlineCommands::Read(args) => {
                        let state = repl_state.online();
                        let result = state
                            .client
                            .read_stream(args.stream, Revision::Start, Direction::Forward)
                            .await?;

                        let mut reading = Reading::Stream(result);

                        reading.display().await?;
                    }

                    OnlineCommands::Subscribe(cmd) => {
                        let state = repl_state.online();
                        let stream = match cmd.command {
                            SubscribeCommands::Stream(opts) => {
                                state
                                    .client
                                    .subscribe_to_stream(opts.stream, Revision::Start)
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
                                    .subscribe_to_process(opts.name, source_code)
                                    .await
                            }
                        };

                        match stream {
                            Err(e) => {
                                println!("ERR: Error when subscribing: {}", e)
                            }

                            Ok(stream) => {
                                let mut reading = Reading::Stream(stream);
                                reading.display().await?;
                            }
                        }
                    }

                    OnlineCommands::Disconnect => {
                        repl_state = NewReplState::Offline;
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
                        let proposes = match load_events_from_file(&opts.json) {
                            Err(e) => {
                                println!(
                                    "ERR: Error when loading events from file {:?}: {}",
                                    opts.json, e
                                );
                                continue;
                            }
                            Ok(es) => es,
                        };

                        match state
                            .client
                            .append_stream(&opts.stream, proposes, ExpectedRevision::Any)
                            .await
                        {
                            Err(e) => {
                                println!(
                                    "ERR: Error when appending events to stream {}: {}",
                                    opts.stream, e
                                );
                            }

                            Ok(result) => {
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
                        }
                    }

                    OnlineCommands::Exit => unreachable!(),
                },

                Cli::Mikoshi(cmd) => {
                    match cmd.commands {
                        MikoshiCommands::Read(args) => {
                            let state = repl_state.mikoshi();
                            match storage_read_stream(&state.index, &state.manager, args) {
                                Err(e) => {
                                    println!("ERR: Error when reading directly events from GethDB files: {}", e);
                                    continue;
                                }

                                Ok(events) => {
                                    let mut reading = Reading::Sync(events);
                                    reading.display().await?;
                                }
                            }
                        }

                        MikoshiCommands::Append(args) => {
                            let state = repl_state.mikoshi();
                            let proposes = match load_events_from_file(args.json.as_path()) {
                                Err(e) => {
                                    println!(
                                        "ERR: Error when loading events from file {:?}: {}",
                                        args.json, e
                                    );
                                    continue;
                                }
                                Ok(es) => es,
                            };

                            match storage_append_stream(
                                &state.index,
                                &state.manager,
                                args.stream.clone(),
                                proposes,
                            ) {
                                Err(e) => {
                                    println!(
                                        "ERR: Error when appending events to stream {}: {}",
                                        args.stream, e
                                    );
                                }

                                Ok(result) => {
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
                            };
                        }

                        MikoshiCommands::Leave => {
                            repl_state = NewReplState::Offline;
                        }
                    }
                }
            },
        }
    }

    Ok(())
}

enum NewReplState {
    Offline,
    Online(OnlineState),
    Mikoshi(MikoshiState),
}

impl NewReplState {
    fn next_input(&self, input: &mut FileBackedInputs) -> io::Result<Option<Input<Cli>>> {
        match self {
            NewReplState::Offline => {
                let cmd = input.next_input_with_parser::<Offline>()?;

                Ok(cmd.map(|i| {
                    i.flat_map(|c| match c.command {
                        OfflineCommands::Exit => Input::Exit,
                        other => Input::Command(Cli::Offline(Offline { command: other })),
                    })
                }))
            }

            NewReplState::Online(_) => {
                let cmd = input.next_input_with_parser::<Online>()?;

                Ok(cmd.map(|i| {
                    i.flat_map(|c| match c.command {
                        OnlineCommands::Exit => Input::Exit,
                        other => Input::Command(Cli::Online(Online { command: other })),
                    })
                }))
            }

            NewReplState::Mikoshi(_) => {
                let cmd = input.next_input_with_parser::<Mikoshi>()?;

                Ok(cmd.map(|i| i.map(Cli::Mikoshi)))
            }
        }
    }

    fn online(&mut self) -> &mut OnlineState {
        if let NewReplState::Online(state) = self {
            return state;
        }

        unreachable!()
    }

    fn mikoshi(&mut self) -> &mut MikoshiState {
        if let NewReplState::Mikoshi(state) = self {
            return state;
        }

        unreachable!()
    }
}

struct OnlineState {
    host: String,
    port: u16,
    client: Client,
}

struct MikoshiState {
    directory: PathBuf,
    storage: FileSystemStorage,
    index: Lsm<FileSystemStorage>,
    manager: ChunkManager<FileSystemStorage>,
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
            r#type: event.r#type,
            data: serde_json::to_vec(&event.payload)?.into(),
        });
    }

    Ok(proposes)
}

fn storage_append_stream<S>(
    index: &Lsm<S>,
    manager: &ChunkManager<S>,
    stream_name: String,
    proposes: Vec<Propose>,
) -> io::Result<WriteResult>
where
    S: Storage + Send + Sync + 'static,
{
    let stream_key = mikoshi_hash(&stream_name);
    let next_revision = index
        .highest_revision(stream_key)?
        .map_or_else(|| 0, |x| x + 1);
    let result = manager.append(stream_name, next_revision, proposes)?;

    let records = manager.prepare_logs(result.position.0).map(|record| {
        let key = mikoshi_hash(&record.event_stream_id);

        (key, record.revision, record.logical_position)
    });

    index.put(records)?;

    Ok(result)
}

fn storage_read_stream<S>(
    index: &Lsm<S>,
    manager: &ChunkManager<S>,
    args: ReadStream,
) -> io::Result<VecDeque<Record>>
where
    S: Storage + Send + Sync + 'static,
{
    let stream_key = mikoshi_hash(&args.stream);
    let mut entries = index.scan(stream_key, Direction::Forward, Revision::Start, usize::MAX);
    let mut records = VecDeque::new();

    while let Some(entry) = entries.next()? {
        let prepare = manager.read_at(entry.position)?;

        records.push_back(Record {
            id: prepare.event_id,
            r#type: prepare.event_type,
            stream_name: prepare.event_stream_id,
            position: Position(prepare.logical_position),
            revision: prepare.revision,
            data: prepare.data,
        });
    }

    Ok(records)
}

async fn list_programmable_subscriptions(state: &mut OnlineState) {
    let summaries = match state.client.list_programmable_subscriptions().await {
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
            "started": summary.started,
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

    if let Err(e) = state.client.kill_programmable_subscription(id).await {
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

    let stats = match state.client.get_programmable_subscription_stats(id).await {
        Err(e) => {
            println!("Err: Error when getting programmable subscription: {}", e);
            return;
        }

        Ok(stats) => stats,
    };

    let source_code = stats.source_code;

    let stats = serde_json::json!({
        "id": stats.id,
        "name": stats.name,
        "started": stats.started,
        "subscriptions": stats.subscriptions,
        "pushed_events": stats.pushed_events,
    });

    println!("{}", serde_json::to_string_pretty(&stats).unwrap());
    println!("Source code:");
    println!("{}", source_code);
}

enum Reading {
    Sync(VecDeque<Record>),
    Stream(geth_client::ReadStream),
}

impl Reading {
    async fn next(&mut self) -> eyre::Result<Option<Record>> {
        match self {
            Reading::Sync(vec) => Ok(vec.pop_front()),
            Reading::Stream(stream) => stream.next().await,
        }
    }

    async fn display(&mut self) -> eyre::Result<()> {
        while let Some(record) = self.next().await? {
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

        Ok(())
    }
}
