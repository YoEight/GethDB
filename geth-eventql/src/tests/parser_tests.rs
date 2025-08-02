use crate::{Limit, LimitKind, Order, sym::Operation};

#[test]
fn test_parsing_from_events_with_top_identity_projection() -> crate::Result<()> {
    let query = include_str!("./resources/from_events_with_top_identity_projection.eql");

    let mut query = crate::parse(query)?;

    assert_eq!(1, query.from_stmts.len());
    let from = query.from_stmts.pop().unwrap();

    assert_eq!("e", from.ident);
    assert!(from.source.inner.targets_events());

    assert!(query.predicate.is_none());

    let order_by_var = query
        .order_by
        .as_ref()
        .and_then(|x| x.expr.as_var())
        .expect("a var");

    let order_by_order = query.order_by.as_ref().map(|x| x.order);

    assert_eq!("e", order_by_var.name);
    assert_eq!(&["time"], order_by_var.path.as_slice());
    assert_eq!(Some(Order::Desc), order_by_order);

    assert_eq!(
        Some(Limit {
            kind: LimitKind::Top,
            value: 100
        }),
        query.limit
    );

    let projection_var = query.projection.as_var().expect("a var");
    assert_eq!("e", projection_var.name);
    assert!(projection_var.path.is_empty());

    Ok(())
}

#[test]
fn test_from_events_with_type_to_project_record() -> crate::Result<()> {
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

    let lhs_var = bin_op.lhs.as_var().expect("a var");

    assert_eq!("e", lhs_var.name);
    assert_eq!(vec!["type".to_string()], lhs_var.path);
    assert_eq!(Operation::Equal, bin_op.op);

    assert_eq!(
        "io.eventsourcingdb.library.book-acquired",
        bin_op.rhs.as_string_literal().unwrap()
    );

    let projection = query.projection.as_record().expect("a record");

    let id_value_var = projection.get("id").unwrap().as_var().expect("a var");
    let book_value_var = projection.get("book").unwrap().as_var().expect("a var");

    assert_eq!("e", id_value_var.name);
    assert_eq!("e", book_value_var.name);

    assert_eq!(&["id"], id_value_var.path.as_slice());
    assert_eq!(
        vec!["data".to_string(), "title".to_string()],
        book_value_var.path
    );

    Ok(())
}

#[test]
fn test_from_events_where_subject_project_record_with_count() -> crate::Result<()> {
    let query = include_str!("./resources/from_events_where_subject_project_record_with_count.eql");

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

    let lhs_var = bin_op.lhs.as_var().expect("a var");
    assert_eq!("e", lhs_var.name);
    assert_eq!(&["subject"], lhs_var.path.as_slice());
    assert_eq!(Operation::Equal, bin_op.op);

    assert_eq!("/books/42", bin_op.rhs.as_string_literal().unwrap());

    let projection = query.projection.as_record().unwrap();
    let total_value = projection.get("total").unwrap().as_apply_fun().unwrap();

    assert_eq!("COUNT", total_value.name);
    assert_eq!(0, total_value.params.len());

    Ok(())
}

#[test]
fn test_from_events_nested_data() -> crate::Result<()> {
    let query = include_str!("./resources/from_events_nested_data.eql");

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
    let lhs_var = bin_op.lhs.as_var().expect("a var");

    assert_eq!("e", lhs_var.name);
    assert_eq!(&["data", "price"], lhs_var.path.as_slice());
    assert_eq!(Operation::GreaterThan, bin_op.op);

    assert_eq!(20, bin_op.rhs.as_i64_literal().unwrap());

    let projection = query.projection.as_record().unwrap();
    let id_var = projection.get("id").unwrap().as_var().unwrap();
    let price_var = projection.get("price").unwrap().as_var().unwrap();

    assert_eq!("e", id_var.name);
    assert_eq!(&["id"], id_var.path.as_slice());

    assert_eq!("e", price_var.name);
    assert_eq!(&["data", "price"], price_var.path.as_slice());

    Ok(())
}

#[test]
fn test_events_using_subquery() -> crate::Result<()> {
    let query = include_str!("./resources/from_events_using_subquery.eql");

    let mut query = crate::parse(query)?;

    assert_eq!(1, query.from_stmts.len());

    let from = query.from_stmts.pop().unwrap();

    assert_eq!("e", from.ident);

    let sub_query = from.source.as_subquery().unwrap();
    let sub_query_pred = sub_query.predicate.as_ref().unwrap();
    let sub_bin_op = sub_query_pred.expr.as_binary_op().unwrap();

    let lhs_var = sub_bin_op.lhs.as_var().expect("a var");

    assert_eq!("e", lhs_var.name);
    assert_eq!(&["type"], lhs_var.path.as_slice());
    assert_eq!(Operation::Equal, sub_bin_op.op);

    assert_eq!(
        "io.eventsourcingdb.library.book-acquired",
        sub_bin_op.rhs.as_string_literal().unwrap()
    );

    let sub_query_projection = sub_query.projection.as_record().unwrap();

    let order_id_var = sub_query_projection
        .get("orderId")
        .unwrap()
        .as_var()
        .expect("a var");

    let value_var = sub_query_projection
        .get("value")
        .unwrap()
        .as_var()
        .expect("a var");

    assert_eq!("e", order_id_var.name);
    assert_eq!(&["id"], order_id_var.path.as_slice());

    assert_eq!("e", value_var.name);
    assert_eq!(&["data", "total"], value_var.path.as_slice());

    let pred = query.predicate.as_ref().unwrap();
    let bin_op = pred.expr.as_binary_op().unwrap();

    let lhs_var = bin_op.lhs.as_var().expect("a var");

    assert_eq!("e", lhs_var.name);
    assert_eq!(&["value"], lhs_var.path.as_slice());
    assert_eq!(Operation::GreaterThan, bin_op.op);

    assert_eq!(100, bin_op.rhs.as_i64_literal().unwrap());
    let projection_var = query.projection.as_var().expect("a var");

    assert_eq!("e", projection_var.name);

    Ok(())
}

#[test]
fn test_parser_binary_op() -> crate::Result<()> {
    let query = include_str!("./resources/parser_binary_op.eql");

    let query = crate::parse(query)?;
    let pred = query.predicate.as_ref().expect("a predicate");
    let bin_op = pred.expr.as_binary_op().expect("a binary op");
    let lhs_bin_op = bin_op.lhs.as_binary_op().expect("a binary op");
    let rhs_bin_op = bin_op.rhs.as_binary_op().expect("a binary op");

    assert_eq!(Operation::And, bin_op.op);

    assert_eq!(
        "e.data.foo",
        lhs_bin_op.lhs.as_var().expect("a var").to_string()
    );

    assert_eq!(
        "foobar",
        lhs_bin_op.rhs.as_string_literal().expect("a string")
    );

    assert_eq!(Operation::Equal, lhs_bin_op.op);

    assert_eq!(
        "e.data.foo",
        rhs_bin_op.lhs.as_var().expect("a var").to_string()
    );

    assert_eq!(42, rhs_bin_op.rhs.as_i64_literal().expect("a integer"));

    assert_eq!(Operation::Equal, rhs_bin_op.op);

    Ok(())
}

#[test]
fn test_parser_inhinged_unary_op() -> crate::Result<()> {
    let query = include_str!("./resources/parser_unhinged_unary_op.eql");

    let query = crate::parse(query)?;
    let pred = query.predicate.as_ref().expect("a predicate");
    let unary_1 = pred.expr.as_unary_op().expect("a unary op");

    assert_eq!(Operation::Not, unary_1.op);

    let unary_2 = unary_1.expr.as_unary_op().expect("a unary op");

    assert_eq!(Operation::Not, unary_2.op);

    let unary_3 = unary_2.expr.as_unary_op().expect("a unary op");

    assert_eq!(Operation::Not, unary_3.op);

    let var = unary_3.expr.as_var().expect("a var");
    assert_eq!("e.enabled", var.to_string());

    Ok(())
}
