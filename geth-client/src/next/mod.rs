use tokio::sync::mpsc::UnboundedReceiver;

use geth_common::{AppendStream, AppendStreamCompleted, DeleteStream, DeleteStreamCompleted, ReadStream, StreamRead, Subscribe, SubscriptionEvent};

enum Msg {
    Request(Request),
    Response(Response),
}

enum Request {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
    Subscribe(Subscribe),
}

enum Response {
    AppendStreamCompleted(AppendStreamCompleted),
    StreamRead(StreamRead),
    SubscriptionEvent(SubscriptionEvent),
    DeleteStreamCompleted(DeleteStreamCompleted),
}

async fn multiplex_loop(mut receiver: UnboundedReceiver<Msg>) {
    while let Some(msg) = receiver.recv().await {
        match msg {
            Msg::Request(req) => {
                match req {
                    Request::AppendStream(req) => {
                        // handle append stream request
                    }
                    Request::DeleteStream(req) => {
                        // handle delete stream request
                    }
                    Request::ReadStream(req) => {
                        // handle read stream request
                    }
                    Request::Subscribe(req) => {
                        // handle subscribe request
                    }
                }
            }
            Msg::Response(_) => {}
        }
    }
}
