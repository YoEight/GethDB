use directories::UserDirs;
use std::collections::VecDeque;
use std::{fs, fs::File, io, path::PathBuf};

use geth_client::Client;
use geth_common::{Direction, ExpectedRevision, Position, Propose, Record, Revision, WriteResult};
use geth_mikoshi::hashing::mikoshi_hash;
use geth_mikoshi::index::{Lsm, LsmSettings};
use geth_mikoshi::storage::{FileSystemStorage, InMemoryStorage, Storage};
use geth_mikoshi::wal::ChunkManager;
use geth_mikoshi::IteratorIO;
use serde::Deserialize;
use structopt::StructOpt;
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
                            let client = Client::new("http://[::1]:2113").await?;
                            state.target = Target::Grpc(client);
                        }

                        Cmd::ReadStream(opts) => read_stream(&mut state, opts).await?,
                        Cmd::AppendStream(opts) => append_stream(&mut state, opts).await?,
                        Cmd::SubscribeToStream(opts) => {
                            subscribe_to_stream(&mut state, opts).await?
                        }
                        Cmd::SubscribeToProcess(opts) => {
                            subscribe_to_process(&mut state, opts).await?
                        }
                        Cmd::Mikoshi(cmd) => match cmd {
                            MikoshiCmd::Root(args) => mikoshi_root(&user_dirs, &mut state, args)?,
                        },
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

    #[structopt(name = "subscribe", about = "Subscribe to a stream")]
    SubscribeToStream(SubscribeToStream),

    #[structopt(name = "subscribe-process", about = "Subscribe to a process")]
    SubscribeToProcess(SubscribeToProcess),

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
struct SubscribeToStream {
    #[structopt(long, short)]
    stream_name: String,
}

#[derive(StructOpt, Debug)]
struct SubscribeToProcess {
    #[structopt(long, short, help = "name of the process")]
    name: String,

    #[structopt(long = "file", short = "f", help = "source code of the process")]
    code: PathBuf,
}

#[derive(StructOpt, Debug)]
enum MikoshiCmd {
    #[structopt(name = "root", about = "database root folder")]
    Root(MikoshiRoot),
}

#[derive(StructOpt, Debug)]
struct MikoshiRoot {
    #[structopt(long, short)]
    directory: Option<String>,
}

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

struct ReplState {
    target: Target,
}

#[derive(Deserialize)]
struct JsonEvent {
    r#type: String,
    payload: serde_json::Value,
}

fn mikoshi_root(
    user_dirs: &UserDirs,
    state: &mut ReplState,
    args: MikoshiRoot,
) -> eyre::Result<()> {
    match args.directory {
        None => match &state.target {
            Target::InMem(_) => println!(">> <in-memory>"),
            Target::FS(fs) => println!(">> Folder: {:?}", fs.storage.root()),
            Target::Grpc(_) => println!(">> <gRPC>"),
        },

        Some(dir) => {
            let mut path_buf = PathBuf::new();
            for (idx, part) in dir.split(std::path::MAIN_SEPARATOR).enumerate() {
                if idx == 0 && part == "~" {
                    path_buf.push(user_dirs.home_dir());
                } else {
                    path_buf.push(part);
                }
            }

            let new_storage = FileSystemStorage::new(path_buf)?;
            let new_index = Lsm::load(LsmSettings::default(), new_storage.clone())?;
            let new_manager = ChunkManager::load(new_storage.clone())?;

            new_index.rebuild(&new_manager)?;

            state.target = Target::FS(FileSystem {
                storage: new_storage,
                index: new_index,
                manager: new_manager,
            });

            println!(">> Mikoshi backend root directory was updated successfully");
        }
    }

    Ok(())
}

async fn append_stream(state: &mut ReplState, args: AppendStream) -> eyre::Result<()> {
    let file = File::open(args.json_file)?;
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
            storage_append_stream(&in_mem.index, &in_mem.manager, args.stream_name, proposes)?
        }

        Target::FS(fs) => {
            storage_append_stream(&fs.index, &fs.manager, args.stream_name, proposes)?
        }

        Target::Grpc(client) => {
            client
                .append_stream(&args.stream_name, proposes, ExpectedRevision::Any)
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
    let stream_key = mikoshi_hash(&args.stream_name);
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
            .subscribe_to_stream(opts.stream_name, Revision::Start)
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

async fn subscribe_to_process(state: &mut ReplState, opts: SubscribeToProcess) -> eyre::Result<()> {
    if let Target::Grpc(client) = &mut state.target {
        let source_code = fs::read_to_string(opts.code)?;

        let mut stream = client.subscribe_to_process(opts.name, source_code).await?;

        while let Some(record) = stream.next().await? {
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

        return Ok(());
    }

    println!("ERR: You need to be connected to a node to subscribe to a process");
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
}

async fn read_stream(state: &mut ReplState, args: ReadStream) -> eyre::Result<()> {
    let mut reading = match &mut state.target {
        Target::InMem(in_mem) => {
            Reading::Sync(storage_read_stream(&in_mem.index, &in_mem.manager, args)?)
        }

        Target::FS(fs) => Reading::Sync(storage_read_stream(&fs.index, &fs.manager, args)?),

        Target::Grpc(client) => {
            let result = client
                .read_stream(args.stream_name, Revision::Start, Direction::Forward)
                .await?;

            Reading::Stream(result)
        }
    };

    while let Some(record) = reading.next().await? {
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
