use fake::Fake;
use fake::faker::name::en::Name;
use temp_dir::TempDir;
use uuid::Uuid;

use geth_client::GrpcClient;
use geth_common::{Client, ExpectedRevision, Propose};

use crate::tests::{client_endpoint, random_valid_options};

#[tokio::test]
async fn simple_append() -> eyre::Result<()> {
    let db_dir = TempDir::new()?;
    let options = random_valid_options(&db_dir);

    let client = GrpcClient::new(client_endpoint(&options));
    let node_handle = tokio::spawn(geth_engine::run(options.clone()));

    let stream_name: String = Name().fake();
    let event_type: String = Name().fake();

    let completed = client
        .append_stream(
            &stream_name,
            ExpectedRevision::Any,
            vec![Propose {
                id: Uuid::new_v4(),
                r#type: event_type.clone(),
                data: Default::default(),
            }],
        )
        .await?;

    Ok(())
}
