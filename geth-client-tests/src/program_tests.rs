use fake::{faker::name::en::Name, Fake};
use geth_client::{Client, GrpcClient};
use geth_common::{ContentType, ExpectedRevision, Propose};
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

    while let Some(event) = stream.next().await? {
        match event {
            geth_common::SubscriptionEvent::EventAppeared(record) => {
                let expected = expecteds.get(count).unwrap();
                let actual = record.as_pyro_value::<Toto>()?;

                assert_eq!(expected, &actual.payload);
                count += 1;
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
