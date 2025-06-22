use crate::process::Managed;

use super::ProcessEnv;

pub async fn run(_: ProcessEnv<Managed>) -> eyre::Result<()> {
    panic!("this process panic on purpose");
}
