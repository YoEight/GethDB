fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes([
            ".geth.AppendStreamRequest.Propose.payload",
            ".geth.AppendStreamRequest.Propose.metadata",
            ".geth.RecordedEvent.payload",
            ".geth.RecordedEvent.metadata",
        ])
        .compile(&["../protos/protocol.proto"], &["../protos/"])?;

    Ok(())
}
