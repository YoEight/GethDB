use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

pub enum Cli {
    Offline(Offline),
    Online(Online),
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

    /// Exit shell.
    Exit,
}

#[derive(Subcommand, Debug)]
pub enum OnlineCommands {
    #[command(arg_required_else_help = true)]
    /// Read a stream
    Read(ReadStream),

    /// Subscription command
    Subscribe(Subscribe),

    /// Disconnect from the current GethDB node
    Disconnect,

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
    /// Path to programmable subscription script.
    Program(SubscribeToProgram),
}

#[derive(Args, Debug)]
pub struct SubscribeToStream {
    pub stream_name: String,
}

#[derive(Args, Debug)]
pub struct SubscribeToProgram {
    #[arg(long)]
    pub name: String,

    pub path: PathBuf,
}

#[derive(Args, Debug)]
pub struct AppendStream {
    pub stream: String,

    #[arg(long)]
    pub json: PathBuf,
}

#[derive(Args, Debug)]
pub struct ReadStream {
    pub stream: String,
}

#[derive(Args, Debug)]
pub struct KillProcess {
    pub id: String,
}

#[derive(Args, Debug)]
pub struct ProcessStats {
    pub id: String,
}
