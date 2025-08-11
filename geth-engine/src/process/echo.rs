use crate::process::{Item, ProcessEnv, env::Managed};

pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    while let Some(item) = env.recv().await {
        if let Item::Mail(mail) = item {
            env.client
                .reply(mail.context, mail.origin, mail.correlation, mail.payload)?;
        }
    }

    Ok(())
}
