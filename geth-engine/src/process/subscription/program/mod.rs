use tokio::sync::mpsc::UnboundedSender;

use crate::process::messages::Messages;

mod pyro;

pub struct ProgramArgs {
    pub name: String,
    pub code: String,
    pub output: UnboundedSender<Messages>,
}
