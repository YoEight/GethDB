use crate::process::subscription::SubscriptionClient;
use crate::process::writing::WriterClient;
use crate::process::{start_process_manager, Proc};
use crate::Options;
use geth_common::{ExpectedRevision, Propose};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_pubsub_proc_simple() -> eyre::Result<()> {
    let manager = start_process_manager(Options::in_mem()).await?;
    let writer_id = manager.wait_for(Proc::Writing).await?.must_succeed()?;
    let pubsub_id = manager.wait_for(Proc::PubSub).await?.must_succeed()?;
    let writer_client = WriterClient::new(writer_id, manager.clone());
    let sub_client = SubscriptionClient::new(pubsub_id, manager.clone());
    let mut expected = vec![];
    let stream_name = Uuid::new_v4().to_string();

    let mut stream = sub_client.subscribe(&stream_name).await?;

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let _ = writer_client
        .append(stream_name.clone(), ExpectedRevision::Any, expected.clone())
        .await?
        .success()?;

    let mut count = 0;
    while let Some(record) = stream.next().await? {
        tracing::debug!("received entry {}/10", count + 1);
        let foo = record.as_value::<Foo>()?;

        assert_eq!(count, record.revision);
        assert_eq!(stream_name, record.stream_name);
        assert_eq!(foo.baz, count as u32 + 10);

        count += 1;

        if count >= 10 {
            break;
        }
    }

    assert_eq!(expected.len(), count as usize);

    Ok(())
}
