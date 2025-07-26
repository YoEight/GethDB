#[test]
fn test_rename_on_subquery() -> eyre::Result<()> {
    let query = include_str!("./resources/rename_subquery.eql");
    let query = crate::parse(query)?;
    let renamed = crate::rename(query)?;

    assert_eq!(2, renamed.scopes.len());

    let scope_1 = renamed.scopes.scope(0);

    assert!(scope_1.var_properties("e").is_empty());

    let scope_2 = renamed.scopes.scope(1);
    let props_2 = scope_2
        .var_properties("e")
        .iter()
        .map(|x| x.as_str())
        .collect::<Vec<_>>();

    assert_eq!(vec!["foobar", "total"], props_2);

    Ok(())
}

#[test]
fn test_rename_non_existing_variable() -> eyre::Result<()> {
    let query = include_str!("./resources/rename_non_existing_variable.eql");
    let query = crate::parse(query)?;

    assert!(crate::rename(query).is_err());

    Ok(())
}

#[test]
fn test_rename_duplicate_variable_names() -> eyre::Result<()> {
    let query = include_str!("./resources/rename_duplicate_variable_names.eql");
    let query = crate::parse(query)?;

    assert!(crate::rename(query).is_err());

    Ok(())
}
