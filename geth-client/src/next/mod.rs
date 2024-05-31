use geth_common::{AppendStream, DeleteStream, ReadStream};

enum Msg {
    AppendStream(AppendStream),
    DeleteStream(DeleteStream),
    ReadStream(ReadStream),
}

async fn multiplex_loop() {}
