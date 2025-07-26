use crate::{Type, error::InferError};

#[test]
fn test_infer_wrong_where_clause_1() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_1.eql");
    let query = crate::parse(query)?;
    let renamed = crate::rename(query)?;

    let e = crate::infer(renamed).err().expect("to return an error");

    assert_eq!(e.kind, InferError::TypeMismatch(Type::Bool, Type::Integer));

    Ok(())
}

#[test]
fn test_infer_wrong_where_clause_2() -> crate::Result<()> {
    let query = include_str!("./resources/infer_wrong_where_clause_2.eql");
    let query = crate::parse(query)?;
    let renamed = crate::rename(query)?;

    let e = crate::infer(renamed).err().expect("to return an error");

    assert_eq!(
        e.kind,
        InferError::TypeMismatch(Type::String, Type::Integer)
    );

    Ok(())
}
