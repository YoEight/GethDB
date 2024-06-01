fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes([
            ".geth.OperationIn.AppendStream.Propose.payload",
            ".geth.OperationIn.AppendStream.Propose.metadata",
            ".geth.RecordedEvent.payload",
            ".geth.RecordedEvent.metadata",
        ])
        .compile(
            &["../protos/streams.proto", "../protos/protocol.proto"],
            &["../protos/"],
        )?;

    Ok(())
}
