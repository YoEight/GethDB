use std::collections::HashMap;

use uuid::Uuid;

use geth_common::EndPoint;
use geth_common::generated::next::protocol::OperationIn;

use crate::next::{Command, connect_to_node, Connection, Event, Mailbox};

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
                // TODO - We need to re-send passed inflight commands.
            }

            let correlation = command.correlation;
            let operation = command.operation.clone().into();
            let input = OperationIn {
                correlation: Some(correlation.into()),
                operation: Some(operation),
            };

            if self.connection.as_ref().unwrap().send(input).is_ok() {
                self.registry.insert(correlation, command);
                return Ok(());
            }

            // If we can't send the operation, it means we lost the connection.
            self.connection = None;
            did_we_reconnect = true;

            tracing::error!(
                "lost connection to node {}:{} when pushing command. Retrying... {}/inf",
                self.endpoint.host,
                self.endpoint.port,
                retries,
            );
        }
    }

    pub fn handle_event(&mut self, event: Event) {
        if let Some(command) = self.registry.remove(&event.correlation) {
            // If we are dealing with a subscription, it means we need to keep that command in the
            // registry until the user decides to unsubscribe or the server disconnects.
            if event.is_subscription_related() && command.resp.send(event).is_ok() {
                self.registry.insert(command.correlation, command);
            }

            return;
        }

        tracing::warn!("received an event that is not related to any command");
    }

    /// We might consider implementing a retry logic here.
    async fn connect(&mut self) -> eyre::Result<()> {
        let uri = format!("http://{}:{}", self.endpoint.host, self.endpoint.port)
            .parse()
            .unwrap();

        let conn = connect_to_node(uri, self.mailbox.clone()).await?;
        self.connection = Some(conn);

        Ok(())
    }
}
