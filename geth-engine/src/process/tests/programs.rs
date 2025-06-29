use std::any::type_name;

use bytes::Bytes;
use geth_common::{
    ContentType, ExpectedRevision, Propose, SubscriptionConfirmation, SubscriptionEvent,
    SubscriptionNotification,
};
use uuid::Uuid;

use crate::{process::tests::Foo, Options, RequestContext};

#[tokio::test]
pub async fn test_program_created() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_subscription_client().await?;
    let writer = embedded.manager().new_writer_client().await?;
    let ctx = RequestContext::new();

    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = "foobar";
    let mut streaming = client
        .subscribe_to_program(ctx, "echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    while let Some(e) = streaming.next().await? {
        if let SubscriptionEvent::Notification(n) = e {
            if let SubscriptionNotification::Subscribed(s) = n {
                if s != "foobar" {
                    continue;
                }

                writer
                    .append(
                        ctx,
                        stream_name.to_string(),
                        ExpectedRevision::Any,
                        expected.clone(),
                    )
                    .await?
                    .success()?;

                break;
            }
        }
    }

    let mut count = 0usize;

    while let Some(event) = streaming.next().await? {
        match event {
            SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(id)) => {
                tracing::debug!(id = id, "subscription to program is confirmed");
            }

            SubscriptionEvent::EventAppeared(event) => {
                let actual = event.as_pyro_value::<Foo>()?;

                assert_eq!(actual.class, type_name::<Foo>());
                assert_eq!(actual.stream_name.as_str(), stream_name);
                assert_eq!(actual.event_revision, count as u64);
                assert_eq!(actual.payload.baz, (count as u32) + 10);

                count += 1;

                if count >= expected.len() {
                    break;
                }
            }

            SubscriptionEvent::Unsubscribed(_) => break,

            _ => {}
        }
    }

    assert_eq!(count, expected.len());

    embedded.shutdown().await
}

#[tokio::test]
pub async fn test_program_list() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_subscription_client().await?;
    let ctx = RequestContext::new();

    let mut ignored = client
        .subscribe_to_program(ctx, "echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    ignored.wait_until_confirmation().await?;

    let programs = client.list_programs(ctx).await?;
    assert_eq!(programs.len(), 1);
    assert_eq!(programs[0].name, "echo");

    embedded.shutdown().await
}

#[tokio::test]
pub async fn test_program_stats() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_subscription_client().await?;
    let writer = embedded.manager().new_writer_client().await?;
    let ctx = RequestContext::new();

    let mut program_out = client
        .subscribe_to_program(ctx, "echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    program_out.wait_until_confirmation().await?;

    while let Some(e) = program_out.next().await? {
        if let SubscriptionEvent::Notification(n) = e {
            if let SubscriptionNotification::Subscribed(s) = n {
                if s != "foobar" {
                    continue;
                }

                writer
                    .append(
                        ctx,
                        "foobar".to_string(),
                        ExpectedRevision::Any,
                        vec![Propose {
                            id: Uuid::new_v4(),
                            content_type: ContentType::Binary,
                            class: "created".to_string(),
                            data: Bytes::default(),
                        }],
                    )
                    .await?
                    .success()?;

                break;
            }
        }
    }

    if program_out.next().await?.is_some() {
    } else {
        panic!("was expecting the program to send something");
    }

    let programs = client.list_programs(ctx).await?;
    let program = client.program_stats(ctx, programs[0].id).await?;
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

    embedded.shutdown().await
}

#[tokio::test]
pub async fn test_program_stop() -> eyre::Result<()> {
    let embedded = crate::run_embedded(&Options::in_mem_no_grpc()).await?;
    let client = embedded.manager().new_subscription_client().await?;
    let writer = embedded.manager().new_writer_client().await?;
    let ctx = RequestContext::new();

    let mut expected = vec![];

    for i in 0..10 {
        expected.push(Propose::from_value(&Foo { baz: i + 10 })?);
    }

    let stream_name = "foobar";
    let mut streaming = client
        .subscribe_to_program(ctx, "echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    streaming.wait_until_confirmation().await?;

    while let Some(e) = streaming.next().await? {
        if let SubscriptionEvent::Notification(n) = e {
            if let SubscriptionNotification::Subscribed(s) = n {
                if s != "foobar" {
                    continue;
                }

                writer
                    .append(
                        ctx,
                        stream_name.to_string(),
                        ExpectedRevision::Any,
                        expected.clone(),
                    )
                    .await?;

                break;
            }
        }
    }

    let mut count = 0usize;

    while let Some(event) = streaming.next().await? {
        if !event.is_event_appeared() {
            continue;
        }

        count += 1;

        if count >= expected.len() {
            break;
        }
    }

    assert_eq!(count, expected.len());

    let programs = client.list_programs(ctx).await?;
    client.program_stop(ctx, programs[0].id).await?;

    let result = client.program_stats(ctx, programs[0].id).await?;
    assert!(result.is_none());

    let result = streaming.next().await?;
    assert!(result.is_none());

    embedded.shutdown().await
}
