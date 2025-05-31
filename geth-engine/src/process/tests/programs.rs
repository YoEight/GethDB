use std::any::type_name;

use geth_common::{ExpectedRevision, Propose};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{process::tests::Foo, start_process_manager, Options};

#[derive(Serialize, Deserialize)]
pub struct PyroRecord<A> {
    pub class: String,
    pub event_revision: u64,
    pub id: Uuid,
    pub position: u64,
    pub stream_name: String,
    pub payload: A,
}

#[tokio::test]
pub async fn test_program_created() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_subscription_client().await?;
    let writer = manager.new_writer_client().await?;

    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = "foobar";
    let mut streaming = client
        .subscribe_to_program("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    writer
        .append(
            stream_name.to_string(),
            ExpectedRevision::Any,
            expected.clone(),
        )
        .await?;

    let mut count = 0usize;

    while let Some(event) = streaming.next().await? {
        let actual = event.as_value::<PyroRecord<Foo>>()?;

        assert_eq!(actual.class, type_name::<Foo>());
        assert_eq!(actual.stream_name.as_str(), stream_name);
        assert_eq!(actual.event_revision, count as u64);
        assert_eq!(actual.payload.baz, (count as u32) + 10);

        count += 1;

        if count >= expected.len() {
            break;
        }
    }

    assert_eq!(count, expected.len());

    Ok(())
}

#[tokio::test]
pub async fn test_program_list() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_subscription_client().await?;

    let mut _ignored = client
        .subscribe_to_program("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let programs = client.list_programs().await?;
    assert_eq!(programs.len(), 1);
    assert_eq!(programs[0].name, "echo");

    Ok(())
}

#[tokio::test]
pub async fn test_program_stats() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let client = manager.new_subscription_client().await?;

    let mut _ignored = client
        .subscribe_to_program("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let programs = client.list_programs().await?;
    let program = client.program_stats(programs[0].id).await?;
    assert!(program.is_some());

    let program = program.unwrap();
    assert_eq!(program.id, programs[0].id);
    assert_eq!(program.name, programs[0].name);
    assert_eq!(program.name, "echo");
    assert_eq!(
        program.source_code,
        include_str!("./resources/programs/echo.pyro")
    );
    assert_eq!(program.subscriptions, vec!["foobar".to_string()]);

    Ok(())
}
