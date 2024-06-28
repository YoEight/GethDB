use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

pub type CancellationToken = Arc<Notify>;

#[derive(Clone)]
pub struct LocalStorage {
    inner: Arc<Mutex<HashMap<Uuid, Arc<Notify>>>>,
}

impl LocalStorage {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Default::default())),
        }
    }

    pub async fn new_cancellation_token(&self, correlation: Uuid) -> CancellationToken {
        let mut inner = self.inner.lock().await;
        let token = Arc::new(Notify::new());

        inner.insert(correlation, token.clone());

        token
    }

    pub async fn cancel(&self, correlation: &Uuid) {
        let mut inner = self.inner.lock().await;

        if let Some(token) = inner.remove(correlation) {
            token.notify_one();
            return;
        }

        tracing::warn!(
            "cancellation token associated to operation {} doesn't exist",
            correlation
        )
    }

    pub async fn complete(&self, correlation: &Uuid) {
        let mut inner = self.inner.lock().await;
        inner.remove(correlation);
    }
}
