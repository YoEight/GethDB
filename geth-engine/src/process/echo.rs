use crate::process::{Item, ProcessEnv};

pub async fn run(mut env: ProcessEnv) -> eyre::Result<()> {
    while let Some(item) = env.queue.recv().await {
        if let Item::Mail(mail) = item {
            env.client
                .reply(mail.origin, mail.correlation, mail.payload)?;
        }
    }

    Ok(())
}
