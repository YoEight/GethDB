use fake::{faker::name::en::Name, Fake};
use geth_client::{Client, GrpcClient};
use geth_common::{ContentType, ExpectedRevision, Propose, SubscriptionConfirmation};
use temp_dir::TempDir;
use uuid::Uuid;

use crate::tests::{client_endpoint, random_valid_options, Toto};

#[tokio::test]
async fn start_program_subscriptions() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let embedded = geth_engine::run_embedded(&options).await?;
    let client = GrpcClient::connect(client_endpoint(&options)).await?;

    let class: String = Name().fake();
    let content_type = ContentType::Json;
    let expecteds = fake::vec![Toto; 10];

    let mut stream = client
        .subscribe_to_process("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let proposes = expecteds
        .iter()
        .map(|x| Propose {
            id: Uuid::new_v4(),
            content_type,
            class: class.clone(),
            data: serde_json::to_vec(x).unwrap().into(),
        })
        .collect();

    client
        .append_stream("foobar", ExpectedRevision::Any, proposes)
        .await?
        .success()?;

    tracing::debug!("wrote to foobar stream successfully");

    let mut count = 0;
    let mut id = u64::MAX;

    while let Some(event) = stream.next().await? {
        match event {
            geth_common::SubscriptionEvent::Confirmed(SubscriptionConfirmation::ProcessId(pid)) => {
                id = pid;
            }

            geth_common::SubscriptionEvent::EventAppeared(record) => {
                let expected = expecteds.get(count).unwrap();
                let actual = record.as_pyro_value::<Toto>()?;

                assert_eq!(expected, &actual.payload);
                count += 1;
                if count >= 10 {
                    assert_ne!(id, u64::MAX);
                    client.stop_program(id).await?;
                }
            }

            geth_common::SubscriptionEvent::Unsubscribed(reason) => {
                tracing::debug!(reason = ?reason, "subscription to program is unsubscribed");
            }

            _ => {}
        }
    }

    embedded.shutdown().await?;

    assert_eq!(count, 10);

    Ok(())
}

#[tokio::test]
async fn get_program_stats() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let embedded = geth_engine::run_embedded(&options).await?;
    let client = GrpcClient::connect(client_endpoint(&options)).await?;

    let class: String = Name().fake();
    let content_type = ContentType::Json;
    let expecteds = fake::vec![Toto; 10];

    let mut stream = client
        .subscribe_to_process("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let id = stream.wait_until_confirmed().await?.try_into_process_id()?;

    let proposes = expecteds
        .iter()
        .map(|x| Propose {
            id: Uuid::new_v4(),
            content_type,
            class: class.clone(),
            data: serde_json::to_vec(x).unwrap().into(),
        })
        .collect();

    client
        .append_stream("foobar", ExpectedRevision::Any, proposes)
        .await?
        .success()?;

    tracing::debug!("wrote to foobar stream successfully");

    let mut count = 0;
    while let Some(event) = stream.next().await? {
        match event {
            geth_common::SubscriptionEvent::EventAppeared(_) => {
                count += 1;
                if count >= 10 {
                    break;
                }
            }

            geth_common::SubscriptionEvent::Unsubscribed(reason) => {
                tracing::debug!(reason = ?reason, "subscription to program is unsubscribed");
                break;
            }

            _ => {}
        }
    }

    assert_eq!(count, 10);

    let stats = client.get_program(id).await?;
    assert!(stats.is_some());

    let stats = stats.unwrap();
    assert_eq!(id, stats.id);
    assert_eq!("echo", stats.name);
    assert_eq!(
        include_str!("./resources/programs/echo.pyro"),
        stats.source_code
    );
    assert_eq!(count as usize, stats.pushed_events);
    assert_eq!(vec!["foobar".to_string()], stats.subscriptions);

    embedded.shutdown().await
}

#[tokio::test]
async fn stop_program_subscription() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let embedded = geth_engine::run_embedded(&options).await?;
    let client = GrpcClient::connect(client_endpoint(&options)).await?;

    let mut stream = client
        .subscribe_to_process("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let proc_id = stream.wait_until_confirmed().await?.try_into_process_id()?;
    client.stop_program(proc_id).await?;

    // FIXME - it's possible that there are some events prior to the unsubscribed event.
    assert!(stream
        .next()
        .await?
        .is_some_and(|x| { matches!(x, geth_common::SubscriptionEvent::Unsubscribed(_)) }));

    assert!(stream.next().await?.is_none());

    embedded.shutdown().await
}

#[tokio::test]
async fn list_program_subscription() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let embedded = geth_engine::run_embedded(&options).await?;
    let mut procs = Vec::new();
    let expected_count = 3;
    let client = GrpcClient::connect(client_endpoint(&options)).await?;

    for i in 0..expected_count {
        let name = format!("echo-{i}");
        let mut stream = client
            .subscribe_to_process(
                name.as_str(),
                include_str!("./resources/programs/echo.pyro"),
            )
            .await?;

        let proc_id = stream.wait_until_confirmed().await?.try_into_process_id()?;

        procs.push((stream, name, proc_id));
    }

    let mut list = client.list_programs().await?;

    list.sort_by(|x, y| x.name.cmp(&y.name));

    for (i, sum) in list.iter().enumerate() {
        let (_, name, proc_id) = &procs[i];

        assert_eq!(*proc_id, sum.id);
        assert_eq!(name, &sum.name);
    }

    assert_eq!(list.len(), expected_count);

    embedded.shutdown().await
}
