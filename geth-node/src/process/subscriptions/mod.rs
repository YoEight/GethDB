use crate::bus::SubscribeMsg;
use tokio::sync::mpsc;

enum Msg {
    Subscribe(SubscribeMsg),
}

pub struct SubscriptionsClient {
    inner: mpsc::UnboundedSender<Msg>,
}

impl SubscriptionsClient {
    pub async fn subscribe(&self, msg: SubscribeMsg) -> eyre::Result<()> {
        if self.inner.send(Msg::Subscribe(msg)).is_err() {
            eyre::bail!("Main bus has shutdown!");
        }

        Ok(())
    }
}

pub fn start() -> SubscriptionsClient {
    let (sender, _mailbox) = mpsc::unbounded_channel();

    SubscriptionsClient { inner: sender }
}
