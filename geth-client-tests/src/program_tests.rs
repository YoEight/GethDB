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

    tokio::spawn(geth_engine::run(options.clone()));
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
                tracing::error!(pid = pid, "WTF");
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

    assert_eq!(count, 10);

    Ok(())
}

#[tokio::test]
async fn stop_program_subscription() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    tokio::spawn(geth_engine::run(options.clone()));
    let client = GrpcClient::connect(client_endpoint(&options)).await?;

    let mut stream = client
        .subscribe_to_process("echo", include_str!("./resources/programs/echo.pyro"))
        .await?;

    let proc_id = stream.wait_until_confirmed().await?.try_into_process_id()?;
    client.stop_program(proc_id).await?;

    assert!(stream.next().await?.is_some_and(|x| {
        if let geth_common::SubscriptionEvent::Unsubscribed(_) = x {
            true
        } else {
            false
        }
    }));

    assert!(stream.next().await?.is_none());

    Ok(())
}
