use crate::{process::subscription::SubscriptionClient, start_process_manager, Options, Proc};

#[tokio::test]
pub async fn test_program_created() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let proc_id = manager.wait_for(Proc::PubSub).await?.must_succeed()?;

    let client = SubscriptionClient::new(proc_id, manager.clone());

    let mut streaming = client
        .subscribe_to_program("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    todo!("check that the program is echoing like it's supposed to");
}
