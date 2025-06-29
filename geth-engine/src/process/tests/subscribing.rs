use crate::Options;
use crate::RequestContext;
use geth_common::{ExpectedRevision, Propose, SubscriptionEvent};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Foo {
    baz: u32,
}

#[tokio::test]
async fn test_pubsub_proc_simple() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let writer_client = embedded.manager().new_writer_client().await?;
    let sub_client = embedded.manager().new_subscription_client().await?;
    let ctx = RequestContext::new();
    let mut expected = vec![];
    let stream_name = Uuid::new_v4().to_string();

    let mut stream = sub_client.subscribe_to_stream(ctx, &stream_name).await?;

    stream.wait_until_confirmation().await?;

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let _ = writer_client
        .append(
            ctx,
            stream_name.clone(),
            ExpectedRevision::Any,
            expected.clone(),
        )
        .await?
        .success()?;

    let mut count = 0;
    while let Some(event) = stream.next().await? {
        if let SubscriptionEvent::EventAppeared(record) = event {
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
    }

    assert_eq!(expected.len(), count as usize);

    embedded.shutdown().await
}
