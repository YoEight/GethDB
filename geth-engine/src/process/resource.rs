use bb8::{ManageConnection, Pool};
use bytes::BytesMut;

pub struct BufferManager;

impl ManageConnection for BufferManager {
    type Connection = BytesMut;

    type Error = eyre::Report;

    fn connect(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::Connection, Self::Error>> + Send {
        std::future::ready(Ok(BytesMut::with_capacity(4_096)))
    }

    fn is_valid(
        &self,
        _: &mut Self::Connection,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        std::future::ready(Ok(()))
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

pub async fn create_buffer_pool() -> eyre::Result<Pool<BufferManager>> {
    Pool::builder().max_size(32).build(BufferManager).await
}
