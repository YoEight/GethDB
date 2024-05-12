use std::collections::VecDeque;
use std::path::Path;
use std::{fs, fs::File, io, path::PathBuf};

use bytes::BytesMut;
use chrono::Utc;
use directories::UserDirs;
use glyph::{FileBackedInputs, Input, PromptOptions};
use serde::Deserialize;
use uuid::Uuid;

use geth_client::Client;
use geth_common::{
    DeleteResult, Direction, ExpectedRevision, IteratorIO, Position, Propose, Record, Revision,
    WriteResult,
};
use geth_domain::binary::events::Event;
use geth_domain::{parse_event, parse_event_io, AppendProposes, Lsm, LsmSettings, RecordedEvent};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::storage::{FileSystemStorage, Storage};
use geth_mikoshi::wal::chunks::ChunkBasedWAL;
use geth_mikoshi::wal::{WALRef, WriteAheadLog};

use crate::cli::{
    Cli, Mikoshi, MikoshiCommands, Offline, OfflineCommands, Online, OnlineCommands,
    ProcessCommands, ReadStream, SubscribeCommands,
};
use crate::utils::expand_path;

mod cli;
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

                        match Client::new(format!("http://{}:{}", host, port)).await {
                            Err(e) => {
                                println!("ERR: error when connecting to {}:{}: {}", host, port, e)
                            }
                            Ok(client) => {
                                repl_state = ReplState::Online(OnlineState { host, port, client });
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

                        let wal = match ChunkBasedWAL::load(storage.clone()) {
                            Err(e) => {
                                println!(
                                    "ERR: Error when loading chunk manager in {:?}: {}",
                                    directory, e
                                );
                                continue;
                            }

                            Ok(m) => WALRef::new(m),
                        };

                        if let Err(e) = index.rebuild(&wal) {
                            println!("ERR: Error when rebuilding index: {}", e);
                            continue;
                        }

                        repl_state = ReplState::Mikoshi(MikoshiState {
                            directory,
                            index,
                            wal,
                        });
                    }

                    _ => unreachable!(),
                },

                Cli::Online(cmd) => match cmd.command {
                    OnlineCommands::Read(args) => {
                        let state = repl_state.online();
                        let result = state
                            .client
                            .read_stream(args.stream.as_str(), Revision::Start, Direction::Forward)
                            .await;

                        match result {
                            Err(e) => {
                                println!("ERR: Error when reading from '{}': {}", args.stream, e);
                            }
                            Ok(result) => {
                                let mut reading = Reading::Stream(result);

                                reading.display().await?;
                            }
                        }
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

                    OnlineCommands::Delete(opts) => {
                        let state = repl_state.online();

                        match state
                            .client
                            .delete_stream(opts.stream.as_str(), ExpectedRevision::Any)
                            .await
                        {
                            Err(e) => {
                                println!("ERR: Error when deleting stream {}: {}", opts.stream, e);
                            }

                            Ok(result) => match result {
                                DeleteResult::WrongExpectedRevision(e) => {
                                    println!(
                                        "ERR: Wrong expected revision when deleting stream '{}', expected: {} but got {}",
                                        opts.stream,
                                        e.expected,
                                        e.current,
                                    );
                                }

                                DeleteResult::Success(p) => {
                                    println!(
                                        "Stream '{}' deletion successful, position {}",
                                        opts.stream,
                                        p.raw(),
                                    );
                                }
                            },
                        }
                    }

                    OnlineCommands::Exit => unreachable!(),
                },

                Cli::Mikoshi(cmd) => {
                    match cmd.commands {
                        MikoshiCommands::Read(args) => {
                            let state = repl_state.mikoshi();
                            match storage_read_stream(&state.index, &state.wal, args) {
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
                                &state.wal,
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
                            repl_state = ReplState::Offline;
                        }
                    }
                }
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
    client: Client,
}

struct MikoshiState {
    directory: PathBuf,
    index: Lsm<FileSystemStorage>,
    wal: WALRef<ChunkBasedWAL<FileSystemStorage>>,
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

fn storage_append_stream<WAL, S>(
    index: &Lsm<S>,
    wal: &WALRef<WAL>,
    stream_name: String,
    proposes: Vec<Propose>,
) -> io::Result<WriteResult>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let mut buffer = BytesMut::new();
    let stream_key = mikoshi_hash(&stream_name);
    let revision = index
        .highest_revision(stream_key)?
        .map_or_else(|| 0, |x| x + 1);

    let len = proposes.len() as u64;
    let events = AppendProposes::new(
        stream_name.clone(),
        Utc::now(),
        revision,
        &mut buffer,
        proposes.into_iter(),
    );

    let receipt = wal.append(events)?;
    let position = receipt.start_position;
    let result = WriteResult {
        next_expected_version: ExpectedRevision::Revision(revision + len),
        position: Position(position),
        next_logical_position: receipt.next_position,
    };

    let records = wal.entries(position).map_io(|entry| {
        let event = parse_event_io(&entry.payload)?;

        if let Event::RecordedEvent(event) = event.event.unwrap() {
            Ok((
                mikoshi_hash(&event.stream_name),
                event.revision,
                entry.position,
            ))
        } else {
            panic!("we are not dealing a recorded event")
        }
    });

    index.put(records)?;

    Ok(result)
}

fn storage_read_stream<WAL, S>(
    index: &Lsm<S>,
    wal: &WALRef<WAL>,
    args: ReadStream,
) -> io::Result<VecDeque<Record>>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let records = if args.disable_index {
        storage_read_stream_without_index(wal, args)
    } else {
        storage_read_stream_with_index(index, wal, args)
    }?;

    Ok(records)
}

fn storage_read_stream_with_index<WAL, S>(
    index: &Lsm<S>,
    manager: &WALRef<WAL>,
    args: ReadStream,
) -> io::Result<VecDeque<Record>>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    let stream_key = mikoshi_hash(&args.stream);
    let mut entries = index.scan(stream_key, Direction::Forward, Revision::Start, usize::MAX);
    let mut records = VecDeque::new();

    while let Some(entry) = entries.next()? {
        let record = manager.read_at(entry.position)?;
        match parse_event(&record.payload) {
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            Ok(event) => {
                if let Event::RecordedEvent(event) = event.event.unwrap() {
                    let event = RecordedEvent::from(event);
                    records.push_back(Record {
                        id: event.id,
                        r#type: event.class,
                        stream_name: event.stream_name,
                        position: Position(record.position),
                        revision: event.revision,
                        data: event.data,
                    });
                }
            }
        }
    }

    Ok(records)
}

fn storage_read_stream_without_index<WAL>(
    wal: &WALRef<WAL>,
    args: ReadStream,
) -> io::Result<VecDeque<Record>>
where
    WAL: WriteAheadLog + Send + Sync + 'static,
{
    let mut records = VecDeque::new();
    let mut iter = wal.entries(0);

    while let Some(entry) = iter.next()? {
        let event = parse_event_io(&entry.payload)?;

        if let Event::RecordedEvent(event) = event.event.unwrap() {
            let event = RecordedEvent::from(event);

            if event.stream_name != args.stream {
                continue;
            }

            records.push_back(Record {
                id: event.id,
                r#type: event.class,
                stream_name: event.stream_name,
                position: Position(entry.position),
                revision: event.revision,
                data: event.data,
            });
        }
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
