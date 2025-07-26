#[test]
fn test_infer_wrong_where_clause_1() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_1.eql");
    let query = crate::parse(query)?;
    let renamed = crate::rename(query)?;

    assert!(crate::infer(renamed).is_err());

    Ok(())
}

#[test]
fn test_infer_wrong_where_clause_2() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_2.eql");
    let query = crate::parse(query)?;
    let renamed = crate::rename(query)?;

    assert!(crate::infer(renamed).is_err());

    Ok(())
}
