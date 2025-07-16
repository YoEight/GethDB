
#[test]
fn test_parsing_from_events_with_top_identity_projection() -> eyre::Result<()> {
    let query = include_str!("./resources/from_events_with_top_identity_projection.eql");

    crate::parse(query)?;

    Ok(())
}
