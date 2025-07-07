use std::{pin::Pin, sync::Arc};

use tokio::sync::Notify;
use tonic::{transport::Server, Code, Status};

use geth_grpc::generated::protocol::protocol_server::ProtocolServer;
use tracing::instrument;

use crate::{
    metrics::{get_metrics, Metrics},
    process::{manager::ManagerClient, Managed, ProcessEnv},
    Options,
};

mod protocol;

pub async fn start_server(
    client: ManagerClient,
    options: Arc<Options>,
    notify: Arc<Notify>,
) -> eyre::Result<()> {
    let addr = format!("{}:{}", options.host, options.port)
        .parse()
        .unwrap();

    let protocols = protocol::ProtocolImpl::connect(client).await?;

    tracing::info!(%addr, db = options.db, "GethDB is listening",);

    let layer = tower::ServiceBuilder::new()
        .layer(MetricsLayer)
        .into_inner();

    Server::builder()
        .layer(layer)
        .add_service(ProtocolServer::new(protocols))
        .serve_with_shutdown(addr, notify.notified())
        .await?;

    Ok(())
}

#[instrument(skip_all, fields(host = env.options.host, port = env.options.port, proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    let notify = Arc::new(Notify::new());
    let handle = tokio::spawn(start_server(
        env.client.clone(),
        env.options.clone(),
        notify.clone(),
    ));

    while env.recv().await.is_some() {
        // we don't care about any message from the process manager
    }

    shutdown(notify, handle).await;

    Ok(())
}

#[instrument(skip_all)]
async fn shutdown(notify: Arc<Notify>, handle: tokio::task::JoinHandle<eyre::Result<()>>) {
    tracing::info!("initate gRPC shutdown");
    notify.notify_one();
    let _ = handle.await;
    tracing::info!("completed");
}

#[derive(Clone)]
struct MetricsLayer;

impl<S> tower::Layer<S> for MetricsLayer {
    type Service = MetricsMiddleware<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MetricsMiddleware {
            metrics: get_metrics(),
            inner,
        }
    }
}

#[derive(Clone)]
struct MetricsMiddleware<S> {
    metrics: Metrics,
    inner: S,
}

type BoxFuture<'a, T> = Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

impl<S, ReqBody, ResBody> tower::Service<http::Request<ReqBody>> for MetricsMiddleware<S>
where
    S: tower::Service<http::Request<ReqBody>, Response = http::Response<ResBody>>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
        // See: https://docs.rs/tower/latest/tower/trait.Service.html#be-careful-when-cloning-inner-services
        let clone = self.inner.clone();
        let metrics = self.metrics.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);

        Box::pin(async move {
            let resp = inner.call(req).await?;
            if let Some(status) = Status::from_header_map(resp.headers()) {
                if status.code() != Code::Ok {
                    if is_client_error(status.code()) {
                        metrics.observe_client_error();
                    } else if is_server_error(status.code()) {
                        metrics.observe_server_error();
                    }
                }
            }

            Ok(resp)
        })
    }
}

fn is_client_error(code: Code) -> bool {
    matches!(
        code,
        Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::Unauthenticated
            | Code::OutOfRange
    )
}

fn is_server_error(code: Code) -> bool {
    matches!(
        code,
        Code::Internal
            | Code::Unavailable
            | Code::DataLoss
            | Code::Unimplemented
            | Code::ResourceExhausted
    )
}
