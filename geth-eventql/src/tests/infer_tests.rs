use crate::{Type, error::InferError};

#[test]
fn test_infer_wrong_where_clause_1() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_1.eql");
    let mut query = crate::parse(query)?;
    let scopes = crate::rename(&mut query)?;

    let e = crate::infer(scopes, query)
        .err()
        .expect("to return an error");

    assert_eq!(e.kind, InferError::TypeMismatch(Type::Bool, Type::Integer));

    Ok(())
}

#[test]
fn test_infer_wrong_where_clause_2() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_2.eql");
    let mut query = crate::parse(query)?;
    let scopes = crate::rename(&mut query)?;

    let e = crate::infer(scopes, query)
        .err()
        .expect("to return an error");

    assert_eq!(
        e.kind,
        InferError::TypeMismatch(Type::String, Type::Integer)
    );

    Ok(())
}
