mod cli;

use directories::UserDirs;
use glyph::{FileBackedInputs, Input};
use std::collections::VecDeque;
use std::{fs, fs::File, io, path::PathBuf};

use crate::cli::{
    AppendStream, Cli, KillProcess, Offline, OfflineCommands, Online, OnlineCommands, ProcessStats,
    ReadStream, SubscribeCommands, SubscribeToProgram, SubscribeToStream,
};
use geth_client::Client;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::{Lsm, LsmSettings};
use geth_mikoshi::storage::{FileSystemStorage, InMemoryStorage, Storage};
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
    let storage = InMemoryStorage::new();
    let manager = ChunkManager::load(storage.clone())?;
    let index = Lsm::load(LsmSettings::default(), storage.clone())?;
    let mut state = ReplState {
        target: Target::InMem(InMemory { index, manager }),
    };

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
                            Err(e) => println!("ERR: error when connecting to {}:{}: {}", host, port, e),
                            Ok(client) => {
                                repl_state = NewReplState::Online(OnlineState {
                                    host,
                                    port,
                                    client,
                                });
                            }
                        }
                    }

                    _ => unreachable!()
                }

                Cli::Online(cmd) => match cmd.command {
                    OnlineCommands::Read(args) => {
                        let state = repl_state.online();
                        let result = state.client
                            .read_stream(args.stream, Revision::Start, Direction::Forward)
                            .await?;

                        let mut reading = Reading::Stream(result);

                        reading.display().await?;
                    }

                    OnlineCommands::Subscribe(cmd) => {
                        let state = repl_state.online();
                        let stream = match cmd.command {
                            SubscribeCommands::Stream(opts) => {
                                state.client.subscribe_to_stream(opts.stream, Revision::Start).await
                            }
                            SubscribeCommands::Program(opts) => {
                                let source_code = match fs::read_to_string(opts.path.as_path()) {
                                    Err(e) => {
                                        println!("ERR: error when reading file {:?}: {}", opts.path, e);
                                        continue;
                                    }

                                    Ok(string) => string,
                                };

                                state.client.subscribe_to_process(opts.name, source_code).await
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

                    OnlineCommands::Exit => unreachable!(),
                }

                // Cmd::Exit => break,
                // 
                // Cmd::Connect => {
                //     let client = Client::new("http://[::1]:2113").await?;
                //     state.target = Target::Grpc(client);
                // }
                // 
                // Cmd::ReadStream(opts) => read_stream(&mut state, opts).await?,
                // Cmd::AppendStream(opts) => append_stream(&mut state, opts).await?,
                // Cmd::SubscribeToStream(opts) => subscribe_to_stream(&mut state, opts).await?,
                // Cmd::SubscribeToProcess(opts) => subscribe_to_process(&mut state, opts).await?,
                // Cmd::Mikoshi(cmd) => match cmd {
                //     MikoshiCmd::Root(args) => mikoshi_root(&user_dirs, &mut state, args)?,
                // },
                // 
                // Cmd::ListProcesses => list_programmable_subscriptions(&mut state).await?,
                // 
                // Cmd::ProcessStats(opts) => {
                //     get_programmable_subscription_stats(&mut state, opts).await?
                // }
                // Cmd::KillProcess(opts) => kill_programmable_subscription(&mut state, opts).await?,
            },
        }
    }

    Ok(())
}

// #[derive(StructOpt, Debug)]
// #[structopt(name = ":")]
// enum Cmd {
//     #[structopt(name = "read", about = "Read a stream")]
//     ReadStream(ReadStream),
//
//     #[structopt(name = "append", about = "Append a stream")]
//     AppendStream(AppendStream),
//
//     #[structopt(name = "subscribe", about = "Subscribe to a stream")]
//     SubscribeToStream(SubscribeToStream),
//
//     #[structopt(
//         name = "subscribe-process",
//         about = "Subscribe to a programmable subscription"
//     )]
//     SubscribeToProcess(SubscribeToProcess),
//
//     #[structopt(name = "list-processes", about = "List all programmable subscriptions")]
//     ListProcesses,
//
//     #[structopt(
//         name = "process-stats",
//         about = "Get a programmable subscription stats"
//     )]
//     ProcessStats(ProcessStats),
//
//     #[structopt(name = "kill-process", about = "Kill a process")]
//     KillProcess(KillProcess),
//
//     #[structopt(name = "mikoshi", about = "Database files management")]
//     Mikoshi(MikoshiCmd),
//
//     #[structopt(name = "connect", about = "Connect to a GethDB node")]
//     Connect,
//
//     #[structopt(about = "Exit the application")]
//     Exit,
// }
//
// #[derive(StructOpt, Debug)]
// struct ReadStream {
//     #[structopt(long, short)]
//     stream_name: String,
// }
//
// #[derive(StructOpt, Debug)]
// struct AppendStream {
//     #[structopt(long, short)]
//     stream_name: String,
//
//     #[structopt(long, short = "f")]
//     json_file: PathBuf,
// }
//
// #[derive(StructOpt, Debug)]
// struct SubscribeToStream {
//     #[structopt(long, short)]
//     stream_name: String,
// }
//
// #[derive(StructOpt, Debug)]
// struct SubscribeToProcess {
//     #[structopt(long, short, help = "name of the process")]
//     name: String,
//
//     #[structopt(long = "file", short = "f", help = "source code of the process")]
//     code: PathBuf,
// }
//
// #[derive(StructOpt, Debug)]
// enum MikoshiCmd {
//     #[structopt(name = "root", about = "database root folder")]
//     Root(MikoshiRoot),
// }
//
// #[derive(StructOpt, Debug)]
// struct MikoshiRoot {
//     #[structopt(long, short)]
//     directory: Option<String>,
// }
//
// #[derive(StructOpt, Debug)]
// struct KillProcess {
//     #[structopt(long)]
//     id: String,
// }
//
// #[derive(StructOpt, Debug)]
// struct ProcessStats {
//     #[structopt(long)]
//     id: String,
// }

enum Target {
    InMem(InMemory),
    FS(FileSystem),
    Grpc(Client),
}

struct InMemory {
    index: Lsm<InMemoryStorage>,
    manager: ChunkManager<InMemoryStorage>,
}

struct FileSystem {
    storage: FileSystemStorage,
    index: Lsm<FileSystemStorage>,
    manager: ChunkManager<FileSystemStorage>,
}

enum NewReplState {
    Offline,
    Online(OnlineState),
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
        }
    }

    fn online(&mut self) -> &mut OnlineState {
        if let NewReplState::Online(state) = self {
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

struct ReplState {
    target: Target,
}

#[derive(Deserialize)]
struct JsonEvent {
    r#type: String,
    payload: serde_json::Value,
}

// fn mikoshi_root(
//     user_dirs: &UserDirs,
//     state: &mut ReplState,
//     args: MikoshiRoot,
// ) -> eyre::Result<()> {
//     match args.directory {
//         None => match &state.target {
//             Target::InMem(_) => println!(">> <in-memory>"),
//             Target::FS(fs) => println!(">> Folder: {:?}", fs.storage.root()),
//             Target::Grpc(_) => println!(">> <gRPC>"),
//         },
//
//         Some(dir) => {
//             let mut path_buf = PathBuf::new();
//             for (idx, part) in dir.split(std::path::MAIN_SEPARATOR).enumerate() {
//                 if idx == 0 && part == "~" {
//                     path_buf.push(user_dirs.home_dir());
//                 } else {
//                     path_buf.push(part);
//                 }
//             }
//
//             let new_storage = FileSystemStorage::new(path_buf)?;
//             let new_index = Lsm::load(LsmSettings::default(), new_storage.clone())?;
//             let new_manager = ChunkManager::load(new_storage.clone())?;
//
//             new_index.rebuild(&new_manager)?;
//
//             state.target = Target::FS(FileSystem {
//                 storage: new_storage,
//                 index: new_index,
//                 manager: new_manager,
//             });
//
//             println!(">> Mikoshi backend root directory was updated successfully");
//         }
//     }
//
//     Ok(())
// }

async fn append_stream(state: &mut ReplState, args: AppendStream) -> eyre::Result<()> {
    let file = File::open(args.json)?;
    let events = serde_json::from_reader::<_, Vec<JsonEvent>>(file)?;
    let mut proposes = Vec::new();

    for event in events {
        proposes.push(Propose {
            id: Uuid::new_v4(),
            r#type: event.r#type,
            data: serde_json::to_vec(&event.payload)?.into(),
        });
    }

    let result = match &mut state.target {
        Target::InMem(in_mem) => {
            storage_append_stream(&in_mem.index, &in_mem.manager, args.stream, proposes)?
        }

        Target::FS(fs) => storage_append_stream(&fs.index, &fs.manager, args.stream, proposes)?,

        Target::Grpc(client) => {
            client
                .append_stream(&args.stream, proposes, ExpectedRevision::Any)
                .await?
        }
    };

    println!(">> {:?}", result);

    Ok(())
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

async fn subscribe_to_stream(state: &mut ReplState, opts: SubscribeToStream) -> eyre::Result<()> {
    if let Target::Grpc(client) = &mut state.target {
        let mut stream = client
            .subscribe_to_stream(opts.stream, Revision::Start)
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

        return Ok(());
    }

    println!("ERR: You need to be connected to a node to subscribe to a stream");
    Ok(())
}

async fn subscribe_to_process(state: &mut ReplState, opts: SubscribeToProgram) -> eyre::Result<()> {
    if let Target::Grpc(client) = &mut state.target {
        let source_code = fs::read_to_string(opts.path)?;

        let mut stream = match client.subscribe_to_process(opts.name, source_code).await {
            Err(e) => {
                println!("Err: {}", e.message());
                return Ok(());
            }

            Ok(stream) => stream,
        };

        loop {
            match stream.next().await {
                Err(e) => {
                    println!("Err: {}", e);
                    return Ok(());
                }

                Ok(msg) => {
                    let record = if let Some(msg) = msg {
                        msg
                    } else {
                        return Ok(());
                    };

                    let data = serde_json::from_slice::<serde_json::Value>(&record.data)?;
                    let record = serde_json::json!({
                        "type": record.r#type,
                        "stream_name": record.stream_name,
                        "id": record.id,
                        "revision": record.revision,
                        "position": record.position.raw(),
                        "data": data,
                    });

                    println!("{}", serde_json::to_string_pretty(&record)?);
                }
            }
        }
    }

    println!("ERR: You need to be connected to a node to subscribe to a process");
    Ok(())
}

async fn list_programmable_subscriptions(state: &mut ReplState) -> eyre::Result<()> {
    if let Target::Grpc(client) = &mut state.target {
        let summaries = client.list_programmable_subscriptions().await?;

        for summary in summaries {
            let summary = serde_json::json!({
                "id": summary.id,
                "name": summary.name,
                "started": summary.started,
            });

            println!("{}", serde_json::to_string_pretty(&summary)?);
        }

        return Ok(());
    }

    println!("ERR: You need to be connected to a node to list programmable subscriptions");
    Ok(())
}

async fn kill_programmable_subscription(
    state: &mut ReplState,
    opts: KillProcess,
) -> eyre::Result<()> {
    let id = match opts.id.parse::<Uuid>() {
        Ok(id) => id,
        Err(e) => {
            println!(
                "Err: provided programmable subscription id is not a valid UUID: {}",
                e
            );

            return Ok(());
        }
    };

    if let Target::Grpc(client) = &mut state.target {
        client.kill_programmable_subscription(id).await?;
        return Ok(());
    }

    println!("ERR: You need to be connected to a node to kill a programmable subscription");
    Ok(())
}

async fn get_programmable_subscription_stats(
    state: &mut ReplState,
    opts: ProcessStats,
) -> eyre::Result<()> {
    let id = match opts.id.parse::<Uuid>() {
        Ok(id) => id,
        Err(e) => {
            println!(
                "Err: provided programmable subscription id is not a valid UUID: {}",
                e
            );

            return Ok(());
        }
    };

    if let Target::Grpc(client) = &mut state.target {
        let stats = client.get_programmable_subscription_stats(id).await?;
        let source_code = stats.source_code;

        let stats = serde_json::json!({
            "id": stats.id,
            "name": stats.name,
            "started": stats.started,
            "subscriptions": stats.subscriptions,
            "pushed_events": stats.pushed_events,
        });

        println!("{}", serde_json::to_string_pretty(&stats)?);
        println!("Source code:");
        println!("{}", source_code);

        return Ok(());
    }

    println!("ERR: You need to be connected to a node to get a programmable subscription's stats");
    Ok(())
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

async fn read_stream(state: &mut OnlineState, args: ReadStream) -> eyre::Result<()> {
    // let mut reading = match &mut state.target {
    //     Target::InMem(in_mem) => {
    //         Reading::Sync(storage_read_stream(&in_mem.index, &in_mem.manager, args)?)
    //     }
    //
    //     Target::FS(fs) => Reading::Sync(storage_read_stream(&fs.index, &fs.manager, args)?),
    //
    //     Target::Grpc(client) => {
    //         let result = client
    //             .read_stream(args.stream, Revision::Start, Direction::Forward)
    //             .await?;
    //
    //         Reading::Stream(result)
    //     }
    // };

    // while let Some(record) = reading.next().await? {
    //     let data = serde_json::from_slice::<serde_json::Value>(&record.data)?;
    //     let record = serde_json::json!({
    //         "stream_name": record.stream_name,
    //         "id": record.id,
    //         "revision": record.revision,
    //         "position": record.position.raw(),
    //         "data": data,
    //     });
    //
    //     println!("{}", serde_json::to_string_pretty(&record)?);
    // }

    Ok(())
}
