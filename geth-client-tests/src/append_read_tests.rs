use fake::faker::name::en::Name;
use fake::{Fake, Faker};
use futures::TryStreamExt;
use temp_dir::TempDir;
use uuid::Uuid;

use geth_client::GrpcClient;
use geth_common::{AppendStreamCompleted, Client, Direction, ExpectedRevision, Propose, Revision};

use crate::tests::{client_endpoint, random_valid_options, Toto};

#[tokio::test]
async fn simple_append() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::new(client_endpoint(&options));
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let event_type: String = Name().fake();
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::Any,
            vec![Propose {
                id: event_id,
                r#type: event_type.clone(),
                data: serde_json::to_vec(&expected)?.into(),
            }],
        )
        .await?;

    let write_result = match completed {
        AppendStreamCompleted::Success(r) => r,
        AppendStreamCompleted::Error(e) => panic!("error: {}", e),
    };

    assert_eq!(
        ExpectedRevision::Revision(1),
        write_result.next_expected_version
    );

    let mut stream = client
        .read_stream(&stream_name, Direction::Forward, Revision::Start, 1)
        .await;

    let event = stream.try_next().await?.unwrap();

    assert_eq!(event_id, event.id);
    assert_eq!(event_type, event.r#type);
    assert_eq!(stream_name, event.stream_name);
    assert_eq!(0, event.revision);

    let actual = serde_json::from_slice::<Toto>(&event.data)?;

    assert_eq!(expected.key, actual.key);
    assert_eq!(expected.value, actual.value);

    Ok(())
}
