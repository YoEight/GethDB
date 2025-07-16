use crate::{Limit, LimitKind, Order};

#[test]
fn test_parsing_from_events_with_top_identity_projection() -> eyre::Result<()> {
    let query = include_str!("./resources/from_events_with_top_identity_projection.eql");

    let mut query = crate::parse(query)?;

    assert_eq!(1, query.from_stmts.len());
    let from = query.from_stmts.pop().unwrap();

    assert_eq!("e", from.ident);
    assert!(from.source.inner.targets_events());

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
