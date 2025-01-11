use std::collections::HashMap;
use std::time::Duration;

use eyre::bail;
use uuid::Uuid;

use geth_common::generated::next::protocol;
use geth_common::{EndPoint, Operation, OperationIn, OperationOut, Reply};

use crate::next::{connect_to_node, Command, ConnErr, Connection, Mailbox};

pub struct Driver {
    endpoint: EndPoint,
    mailbox: Mailbox,
    connection: Option<Connection>,
    registry: HashMap<Uuid, Command>,
}

impl Driver {
    pub fn new(endpoint: EndPoint, mailbox: Mailbox) -> Self {
        Self {
            mailbox,
            endpoint,
            connection: None,
            registry: HashMap::new(),
        }
    }

    pub async fn handle_command(&mut self, command: Command) -> eyre::Result<()> {
        let mut did_we_reconnect = false;
        let mut retries = 1;

        loop {
            if self.connection.is_none() {
                self.connect().await?;

                if did_we_reconnect {
                    tracing::info!(
                        "reconnected to node {}:{}",
                        self.endpoint.host,
                        self.endpoint.port,
                    );
                    // TODO - We need to re-send passed inflight commands.
                }
            }

            let correlation = command.operation_in.correlation;
            let operation_in: protocol::OperationIn = command.operation_in.clone().into();

            if self.connection.as_ref().unwrap().send(operation_in).is_ok() {
                self.registry.insert(correlation, command);
                return Ok(());
            }

            tracing::error!(
                "lost connection to node {}:{} when pushing command. Retrying... {}/inf",
                self.endpoint.host,
                self.endpoint.port,
                retries,
            );

            // If we can't send the operation, it means we lost the connection.
            self.connection = None;
            did_we_reconnect = true;
            retries += 1;
        }
    }

    pub fn handle_event(&mut self, event: OperationOut) {
        if let Some(command) = self.registry.remove(&event.correlation) {
            let is_streaming = event.is_streaming();
            if command.resp.send(event).is_err() {
                tracing::warn!(
                    "user didn't care about the command {}",
                    command.operation_in.correlation
                );

                // If we were dealing with a streaming operation, we notify the server that there is
                // no need to keep those resources up for nothing.
                if is_streaming {
                    if let Some(connection) = &self.connection {
                        let _ = connection.send(
                            OperationIn {
                                correlation: command.operation_in.correlation,
                                operation: Operation::Unsubscribe,
                            }
                            .into(),
                        );
                    }
                }
                return;
            }

            // If we are dealing with a streaming operation, it means we need to keep that command in the
            // registry until the user decides to unsubscribe or the server disconnects.
            if is_streaming {
                self.registry
                    .insert(command.operation_in.correlation, command);
            }

            return;
        }

        tracing::warn!("received an event that is not related to any command");
    }

    pub fn handle_disconnect(&mut self) {
        tracing::warn!("connection was closed by the server");
        self.connection = None;

        for (correlation, cmd) in self.registry.drain() {
            let _ = cmd.resp.send(OperationOut {
                correlation,
                reply: Reply::ServerDisconnected,
            });
        }
    }

    /// We might consider implementing a retry logic here.
    async fn connect(&mut self) -> eyre::Result<()> {
        let uri = format!("http://{}:{}", self.endpoint.host, self.endpoint.port)
            .parse()
            .unwrap();

        let mut attempts = 0;
        let max_attempts = 10;
        loop {
            match connect_to_node(&uri, self.mailbox.clone()).await {
                Err(e) => match e {
                    ConnErr::Transport(e) => {
                        if e.to_string() == "transport error" {
                            attempts += 1;

                            if attempts > max_attempts {
                                tracing::error!(
                                    "max connection attempt reached ({})",
                                    max_attempts
                                );

                                bail!("max connection attempt reached");
                            }

                            tracing::error!(
                                "error when connecting to {}:{} {}/{}",
                                self.endpoint.host,
                                self.endpoint.port,
                                attempts,
                                max_attempts,
                            );

                            tokio::time::sleep(Duration::from_secs(1)).await;
                            continue;
                        }

                        bail!("fatal error when connecting: {}", e);
                    }

                    ConnErr::Status(e) => {
                        // TODO - we might retry if the server is not ready or under too much load.
                        tracing::error!("error when reaching endpoint: {}", e);
                        bail!("error when reaching endpoint");
                    }
                },

                Ok(c) => {
                    self.connection = Some(c);
                    return Ok(());
                }
            }
        }
    }
}
