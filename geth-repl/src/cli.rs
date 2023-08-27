use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

pub enum Cli {
    Offline(Offline),
    Online(Online),
    Mikoshi(Mikoshi),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Online {
    #[command(subcommand)]
    pub command: OnlineCommands,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Offline {
    #[command(subcommand)]
    pub command: OfflineCommands,
}

#[derive(Subcommand, Debug)]
pub enum OfflineCommands {
    /// Connect to a GethDB node
    Connect {
        #[arg(long)]
        host: Option<String>,

        #[arg(long)]
        port: Option<u16>,
    },

    #[command(arg_required_else_help = true)]
    /// Use a Mikoshi directory
    Mikoshi { directory: PathBuf },

    /// Exit shell.
    Exit,
}

#[derive(Subcommand, Debug)]
pub enum OnlineCommands {
    #[command(arg_required_else_help = true)]
    /// Read a stream
    Read(ReadStream),

    #[command(arg_required_else_help = true)]
    /// Append a stream
    Append(AppendStream),

    #[command(arg_required_else_help = true)]
    /// Delete a stream
    Delete(DeleteStream),

    /// Subscription commands
    Subscribe(Subscribe),

    /// Disconnect from the current GethDB node
    Disconnect,

    /// Process commands
    Process(Process),

    /// Exit shell.
    Exit,
}

#[derive(Args, Debug)]
pub struct Subscribe {
    #[command(subcommand)]
    pub command: SubscribeCommands,
}

#[derive(Subcommand, Debug)]
pub enum SubscribeCommands {
    #[command(arg_required_else_help = true)]
    Stream(SubscribeToStream),

    #[command(arg_required_else_help = true)]
    Program(SubscribeToProgram),
}

#[derive(Args, Debug)]
pub struct SubscribeToStream {
    // Stream's name
    pub stream: String,
}

#[derive(Args, Debug)]
pub struct SubscribeToProgram {
    #[arg(long)]
    /// Programmable subscription's name.
    pub name: String,

    /// Path to programmable subscription script.
    pub path: PathBuf,
}

#[derive(Args, Debug)]
pub struct AppendStream {
    // Stream's name
    #[arg(long)]
    pub stream: String,

    /// Path to json file.
    pub json: PathBuf,
}

#[derive(Args, Debug)]
pub struct DeleteStream {
    // Stream's name
    pub stream: String,
}

#[derive(Args, Debug)]
pub struct ReadStream {
    #[arg(long)]
    pub disable_index: bool,
    pub stream: String,
}

#[derive(Args, Debug)]
pub struct Process {
    #[command(subcommand)]
    pub commands: ProcessCommands,
}

#[derive(Subcommand, Debug)]
pub enum ProcessCommands {
    Kill { id: String },
    Stats { id: String },
    List,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Mikoshi {
    #[command(subcommand)]
    pub commands: MikoshiCommands,
}

#[derive(Subcommand, Debug)]
pub enum MikoshiCommands {
    #[command(arg_required_else_help = true)]
    /// Read a stream
    Read(ReadStream),

    /// Append a stream
    Append(AppendStream),

    /// Leave Mikoshi directory
    Leave,
}
