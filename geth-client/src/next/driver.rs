use std::collections::HashMap;

use uuid::Uuid;

use geth_common::EndPoint;

use crate::next::{connect_to_node, Command, Connection, Mailbox};

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
        if self.connection.is_none() {
            self.connect().await?;
        }

        Ok(())
    }

    /// We might consider implementing a retry logic here.
    pub async fn connect(&mut self) -> eyre::Result<()> {
        let uri = format!("http://{}:{}", self.endpoint.host, self.endpoint.port)
            .parse()
            .unwrap();

        let conn = connect_to_node(uri, self.mailbox.clone()).await?;
        self.connection = Some(conn);

        Ok(())
    }
}
