use geth_eventql::{
    ContextFrame, Expr, ExprVisitor, Literal, NodeAttributes, Operation, Query, QueryVisitor,
    Subject, Value, Var,
};
use std::collections::{HashMap, HashSet};

pub struct SourceDecl {
    pub scope: u64,
    pub ident: String,
}

pub struct Requirements {
    pub parent_queries: HashMap<u64, ParentQuery>,
    pub scoped_reqs: HashMap<u64, HashMap<String, ScopedRequirements>>,
}

pub fn collect_requirements(query: &Query) -> Requirements {
    let mut collect_reqs = CollectRequirements::default();
    query.dfs_pre_order(&mut collect_reqs);

    Requirements {
        parent_queries: collect_reqs.parent_queries,
        scoped_reqs: collect_reqs.scoped_reqs,
    }
}

#[derive(Clone)]
pub struct ParentQuery {
    pub scope: u64,
    pub ident: String,
}

#[derive(Clone)]
pub struct ScopedRequirements {
    pub scope: u64,
    pub ident: String,
    pub needed_props: HashSet<Vec<String>>,
    pub pushable_preds: HashMap<Vec<String>, Vec<Expr>>,
    pub subjects: HashSet<Subject>,
}

impl ScopedRequirements {
    fn new(scope: u64, ident: String) -> Self {
        Self {
            scope,
            ident,
            needed_props: HashSet::new(),
            pushable_preds: HashMap::new(),
            subjects: HashSet::new(),
        }
    }

    fn push_needed_props(&mut self, path: &Vec<String>) {
        if path.is_empty() {
            return;
        }

        self.needed_props.insert(path.clone());
    }

    fn push_subject(&mut self, new_sub: &Subject) {
        self.subjects.insert(new_sub.clone());
    }
}

#[derive(Default)]
struct CollectRequirements {
    context: ContextFrame,
    sources: Vec<SourceDecl>,
    parent_queries: HashMap<u64, ParentQuery>,
    scoped_reqs: HashMap<u64, HashMap<String, ScopedRequirements>>,
}

impl CollectRequirements {
    fn create_requirement(&mut self, scope: u64, ident: &str) {
        self.scoped_reqs.entry(scope).or_default().insert(
            ident.to_string(),
            ScopedRequirements::new(scope, ident.to_string()),
        );
    }

    fn get_requirement_mut(&mut self, scope: u64, ident: &str) -> &mut ScopedRequirements {
        self.scoped_reqs
            .get_mut(&scope)
            .expect("unexistent scope")
            .get_mut(ident)
            .expect("missing requirement")
    }
}

impl QueryVisitor for CollectRequirements {
    type Inner<'a> = CollectRequirementsFromExpr<'a>;

    fn on_source_events(&mut self, attrs: &NodeAttributes, ident: &str) {
        self.create_requirement(attrs.scope, ident);
    }

    fn on_source_subject(&mut self, attrs: &NodeAttributes, ident: &str, _: &Subject) {
        self.create_requirement(attrs.scope, ident);
    }

    fn on_source_subquery(&mut self, attrs: &NodeAttributes, ident: &str, query: &Query) -> bool {
        self.parent_queries.insert(
            query.attrs.scope,
            ParentQuery {
                scope: attrs.scope,
                ident: ident.to_string(),
            },
        );

        self.create_requirement(attrs.scope, ident);

        true
    }

    fn enter_where_clause(&mut self, _attrs: &NodeAttributes, _expr: &Expr) {
        self.context = ContextFrame::Where;
    }

    fn exit_where_clause(&mut self, _attrs: &NodeAttributes, expr: &Expr) {
        self.context = ContextFrame::Unspecified;
    }

    fn expr_visitor(&mut self) -> Self::Inner<'_> {
        CollectRequirementsFromExpr { inner: self }
    }
}

struct CollectRequirementsFromExpr<'a> {
    inner: &'a mut CollectRequirements,
}

impl ExprVisitor for CollectRequirementsFromExpr<'_> {
    fn on_var(&mut self, attrs: &NodeAttributes, var: &Var) {
        self.inner
            .get_requirement_mut(attrs.scope, &var.name)
            .push_needed_props(&var.path);
    }

    fn exit_field(&mut self, attrs: &NodeAttributes, label: &str, value: &Expr) {
        if let Value::Var(x) = &value.value {
            self.inner
                .get_requirement_mut(attrs.scope, &x.name)
                .push_needed_props(&x.path);
        }
    }

    fn exit_binary_op(&mut self, attrs: &NodeAttributes, op: &Operation, lhs: &Expr, rhs: &Expr) {
        match (&lhs.value, &rhs.value) {
            (Value::Var(_), Value::Var(_)) => {
                // TODO - a possible optimization could be to check if two variables are looking at the same subject.
                // if they do then we can merge the request into a singular read operation. Current thought of the matter: the way we deal
                // with binding to subjects needs to be extended to either we have a direct binding
                //  (a.k.a an explicit x.subject = "/foo/bar" or FROM x IN "/foo/bar") and an indirect where
                // we have something like x.subject ==  y.subject. By doing that we can simplify a convoluted requests like
                // those to a much direct one.
            }

            (Value::Var(x), value) | (value, Value::Var(x)) => {
                if self.inner.context != ContextFrame::Where {
                    return;
                }

                let reqs = self.inner.get_requirement_mut(attrs.scope, &x.name);

                match value {
                    Value::Literal(lit) => {
                        if let Literal::Subject(sub) = lit
                            && x.path.as_slice() == ["subject"]
                            && op == &Operation::Equal
                        {
                            reqs.push_subject(&sub);
                        }
                    }

                    Value::Array(xs) => {
                        if op == &Operation::Contains {
                            for x in xs.iter() {
                                if !x.is_literal() {
                                    return;
                                }
                            }
                        }
                    }

                    _ => return,
                }

                reqs.pushable_preds
                    .entry(x.path.clone())
                    .or_default()
                    .push(Expr {
                        attrs: *attrs,
                        value: Value::Binary {
                            lhs: Box::new(lhs.clone()),
                            op: *op,
                            rhs: Box::new(rhs.clone()),
                        },
                    });
            }

            _ => return,
        }
    }

    fn exit_unary_op(&mut self, attrs: &NodeAttributes, op: &Operation, expr: &Expr) {
        if let Value::Var(x) = &expr.value {
            if self.inner.context != ContextFrame::Where {
                return;
            }

            let reqs = self.inner.get_requirement_mut(attrs.scope, &x.name);

            reqs.pushable_preds
                .entry(x.path.clone())
                .or_default()
                .push(Expr {
                    attrs: *attrs,
                    value: Value::Unary {
                        op: *op,
                        expr: Box::new(expr.clone()),
                    },
                });
        }
    }
}
