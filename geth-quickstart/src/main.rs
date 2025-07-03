use geth_client::{
    Client, ContentType, Direction, EndPoint, ExpectedRevision, GrpcClient, Propose, Revision,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug)]
struct Foobar {
    value: u64,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let client = GrpcClient::connect(EndPoint {
        host: "127.0.0.1".to_string(),
        port: 2_113,
    })
    .await?;

    let mut proposes = Vec::new();

    for i in 0..10 {
        proposes.push(Propose {
            id: Uuid::new_v4(),
            content_type: ContentType::Json,
            class: "foobar".to_string(),
            data: serde_json::to_vec(&Foobar { value: 10 * i })?.into(),
        });
    }

    client
        .append_stream("baz", ExpectedRevision::Any, proposes)
        .await?
        .success()?;

    let mut stream = client
        .read_stream("baz", Direction::Forward, Revision::Start, u64::MAX)
        .await?
        .success()?;

    while let Some(event) = stream.next().await? {
        println!("{event:?}");
    }

    Ok(())
}
