use crate::{Limit, LimitKind, Order, sym::Operation};

#[test]
fn test_parsing_from_events_with_top_identity_projection() -> eyre::Result<()> {
    let query = include_str!("./resources/from_events_with_top_identity_projection.eql");

    let mut query = crate::parse(query)?;

    assert_eq!(1, query.from_stmts.len());
    let from = query.from_stmts.pop().unwrap();

    assert_eq!("e", from.ident);
    assert!(from.source.inner.targets_events());

    assert!(query.predicate.is_none());

    let order_by_ident = query.order_by.as_ref().and_then(|x| x.expr.as_path());
    let order_by_order = query.order_by.as_ref().map(|x| x.order);

    assert_eq!(
        Some(&vec!["e".to_string(), "time".to_string()]),
        order_by_ident
    );

    assert_eq!(Some(Order::Desc), order_by_order);

    assert_eq!(
        Some(Limit {
            kind: LimitKind::Top,
            value: 100
        }),
        query.limit
    );

    assert_eq!(Some(&vec!["e".to_string()]), query.projection.as_path());

    Ok(())
}

#[test]
fn test_from_events_with_type_to_project_record() -> eyre::Result<()> {
    let query = include_str!("./resources/from_events_with_type_to_project_record.eql");

    let mut query = crate::parse(query)?;

    assert_eq!(1, query.from_stmts.len());
    let from = query.from_stmts.pop().unwrap();

    assert_eq!("e", from.ident);
    assert!(from.source.inner.targets_events());

    assert!(query.group_by.is_none());
    assert!(query.order_by.is_none());

    assert!(query.predicate.is_some());
    let pred = query.predicate.as_ref().unwrap();
    let bin_op = pred.expr.as_binary_op().unwrap();

    assert_eq!(
        &vec!["e".to_string(), "type".to_string()],
        bin_op.lhs.as_path().unwrap()
    );

    assert_eq!(Operation::Equal, bin_op.op);

    assert_eq!(
        "io.eventsourcingdb.library.book-acquired",
        bin_op.rhs.as_string_literal().unwrap()
    );

    let projection = query.projection.as_record().unwrap();

    let id_value = projection.get("id").unwrap().as_path().unwrap();
    let book_value = projection.get("book").unwrap().as_path().unwrap();

    assert_eq!(&vec!["e".to_string(), "id".to_string()], id_value);
    assert_eq!(
        &vec!["e".to_string(), "data".to_string(), "title".to_string()],
        book_value
    );

    Ok(())
}
