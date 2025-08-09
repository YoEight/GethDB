use crate::error::RenameError;

#[test]
fn test_rename_on_subquery() -> crate::Result<()> {
    let query = include_str!("./resources/rename_subquery.eql");
    let mut query = crate::parse(query)?;
    let scopes = crate::rename(&mut query)?;

    assert_eq!(2, scopes.len());

    let scope_1 = scopes.scope(0);

    assert!(scope_1.var_properties("e").is_empty());

    let scope_2 = scopes.scope(1);
    let mut props_2 = scope_2
        .var_properties("e")
        .iter()
        .map(|x| x.as_str())
        .collect::<Vec<_>>();

    props_2.sort();
    assert_eq!(vec!["foobar", "total"], props_2);

    Ok(())
}

#[test]
fn test_rename_non_existing_variable() -> crate::Result<()> {
    let query = include_str!("./resources/rename_non_existing_variable.eql");
    let mut query = crate::parse(query)?;

    let e = crate::rename(&mut query).err().expect("to return an error");

    assert_eq!(e.kind, RenameError::VariableDoesNotExist("f".to_string()));

    Ok(())
}

#[test]
fn test_rename_duplicate_variable_names() -> crate::Result<()> {
    let query = include_str!("./resources/rename_duplicate_variable_names.eql");
    let mut query = crate::parse(query)?;

    let e = crate::rename(&mut query).err().expect("to return an error");

    assert_eq!(e.kind, RenameError::VariableAlreadyExists("e".to_string()));

    Ok(())
}
