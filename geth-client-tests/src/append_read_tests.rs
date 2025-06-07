use bytes::Bytes;
use eyre::bail;
use fake::faker::name::en::Name;
use fake::{Fake, Faker};
use temp_dir::TempDir;
use uuid::Uuid;

use geth_client::{Client, GrpcClient};
use geth_common::{
    AppendError, AppendStreamCompleted, ContentType, Direction, ExpectedRevision, Propose, Revision,
};

use crate::tests::{client_endpoint, random_valid_options, Toto};

#[tokio::test]
async fn simple_append() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let class: String = Name().fake();
    let content_type = ContentType::Json;
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::Any,
            vec![Propose {
                id: event_id,
                content_type,
                class: class.clone(),
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
        .await?
        .success()?;

    let event = stream.next().await?.unwrap();

    assert_eq!(event_id, event.id);
    assert_eq!(content_type, event.content_type);
    assert_eq!(stream_name, event.stream_name);
    assert_eq!(class, event.class);
    assert_eq!(0, event.revision);

    let actual = serde_json::from_slice::<Toto>(&event.data)?;

    assert_eq!(expected.key, actual.key);
    assert_eq!(expected.value, actual.value);

    Ok(())
}

#[tokio::test]
async fn simple_append_expecting_no_stream_on_non_existing_stream() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let content_type = ContentType::Json;
    let class: String = Name().fake();
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::NoStream,
            vec![Propose {
                id: event_id,
                content_type,
                class,
                data: serde_json::to_vec(&expected)?.into(),
            }],
        )
        .await?;

    let write_result = match completed {
        AppendStreamCompleted::Success(r) => r,
        AppendStreamCompleted::Error(e) => bail!("error: {}", e),
    };

    assert_eq!(
        ExpectedRevision::Revision(1),
        write_result.next_expected_version
    );

    Ok(())
}

#[tokio::test]
async fn simple_append_expecting_existence_on_non_existing_stream() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let content_type = ContentType::Json;
    let class: String = Name().fake();
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let err = client
        .append_stream(
            &stream_name,
            ExpectedRevision::StreamExists,
            vec![Propose {
                id: event_id,
                content_type,
                class,
                data: serde_json::to_vec(&expected)?.into(),
            }],
        )
        .await?
        .err()?;

    let err = match err {
        AppendError::WrongExpectedRevision(e) => e,
        _ => bail!("didn't expect stream deleted error"),
    };

    assert_eq!(ExpectedRevision::StreamExists, err.expected);
    assert_eq!(ExpectedRevision::NoStream, err.current);

    Ok(())
}

#[tokio::test]
async fn simple_append_expecting_revision_on_non_existing_stream() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let content_type = ContentType::Json;
    let class: String = Name().fake();
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::Revision(42),
            vec![Propose {
                id: event_id,
                content_type,
                class,
                data: serde_json::to_vec(&expected)?.into(),
            }],
        )
        .await?;

    let err = match completed {
        AppendStreamCompleted::Success(_) => bail!("we expected an error"),
        AppendStreamCompleted::Error(e) => e,
    };

    let err = match err {
        geth_common::AppendError::WrongExpectedRevision(e) => e,
        _ => bail!("expected wrong expected revision error"),
    };

    assert_eq!(ExpectedRevision::Revision(42), err.expected);
    assert_eq!(ExpectedRevision::NoStream, err.current);

    Ok(())
}

#[tokio::test]
async fn simple_append_expecting_revision_on_existing_stream() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let content_type = ContentType::Json;
    let class: String = Name().fake();
    let event_id = Uuid::new_v4();
    let expected: Toto = Faker.fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::Revision(42),
            vec![Propose {
                id: event_id,
                content_type,
                class,
                data: serde_json::to_vec(&expected)?.into(),
            }],
        )
        .await?;

    let err = match completed {
        AppendStreamCompleted::Success(_) => bail!("we expected an error"),
        AppendStreamCompleted::Error(e) => e,
    };

    let err = match err {
        geth_common::AppendError::WrongExpectedRevision(e) => e,
        _ => bail!("expected wrong expected revision error"),
    };

    assert_eq!(ExpectedRevision::Revision(42), err.expected);
    assert_eq!(ExpectedRevision::NoStream, err.current);

    Ok(())
}

#[tokio::test]
async fn read_whole_stream_forward() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::connect(client_endpoint(&options)).await?;
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let class: String = Name().fake();
    let content_type = ContentType::Json;
    let expected: Toto = Faker.fake();
    let data: Bytes = serde_json::to_vec(&expected)?.into();
    let mut events = vec![];

    for _ in 0..100 {
        events.push(Propose {
            id: Uuid::new_v4(),
            content_type,
            class: class.clone(),
            data: data.clone(),
        });
    }

    client
        .append_stream(&stream_name, ExpectedRevision::Any, events.clone())
        .await?;

    let mut stream = client
        .read_stream(&stream_name, Direction::Forward, Revision::Start, u64::MAX)
        .await?
        .success()?;

    let mut actuals = Vec::new();
    while let Some(event) = stream.next().await? {
        actuals.push(event);
    }

    assert_eq!(events.len(), actuals.len());

    for i in 0..100 {
        let expected = events.get(i).unwrap();
        let actual = actuals.get(i).unwrap();

        assert_eq!(expected.id, actual.id);
        assert_eq!(expected.content_type, actual.content_type);
        assert_eq!(expected.data, actual.data);
        assert_eq!(stream_name, actual.stream_name);
        assert_eq!(class, actual.class);
    }

    Ok(())
}
