use crate::bus::{Mailbox, Msg};

mod storage;

pub fn start(mut mailbox: Mailbox) {
    let storage_client = storage::start();

    tokio::spawn(async move {
        while let Some(msg) = mailbox.next().await {
            match msg {
                Msg::AppendStream(opts, callback) => {
                    storage_client.append_stream(opts, callback)?;
                }

                Msg::ReadStream(opts, callback) => {
                    storage_client.read_stream(opts, callback)?;
                }
            }
        }

        Ok::<_, eyre::Report>(())
    });
}
