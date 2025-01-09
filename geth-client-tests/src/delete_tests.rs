use std::u64;

use bytes::Bytes;
use fake::{faker::name::en::Name, Fake};
use geth_client::GrpcClient;
use geth_common::{Client, ContentType, Direction, ExpectedRevision, Propose, Revision};
use temp_dir::TempDir;
use uuid::Uuid;

use crate::tests::{client_endpoint, random_valid_options};

#[tokio::test]
async fn simple_delete() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::new(client_endpoint(&options));
    tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let class: String = Name().fake();
    let content_type = ContentType::Binary;
    let event_id = Uuid::new_v4();

    client
        .append_stream(
            &stream_name,
            ExpectedRevision::Any,
            vec![Propose {
                id: event_id,
                content_type,
                class: class.clone(),
                data: Bytes::default(),
            }],
        )
        .await?
        .success()?;

    client
        .delete_stream(&stream_name, ExpectedRevision::Any)
        .await?
        .success()?;

    let stream = client
        .read_stream(&stream_name, Direction::Forward, Revision::Start, u64::MAX)
        .await?;

    assert!(stream.is_stream_deleted());

    Ok(())
}
