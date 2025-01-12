use super::ProcessEnv;

pub async fn run(_: ProcessEnv) -> eyre::Result<()> {
    panic!("this process panic on purpose");
}
