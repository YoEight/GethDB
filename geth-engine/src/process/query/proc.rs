use geth_eventql::{Expr, Literal, Operation, Pos, Query, Var};

use crate::process::{
    messages::{QueryRequests, QueryResponses},
    Item, Managed, ProcessEnv,
};

#[tracing::instrument(skip_all, fields(proc_id = env.client.id(), proc = ?env.proc))]
pub async fn run(mut env: ProcessEnv<Managed>) -> eyre::Result<()> {
    while let Some(item) = env.recv().await {
        if let Item::Stream(stream) = item {
            if let Ok(QueryRequests::Query { query }) = stream.payload.try_into() {
                let query = match geth_eventql::parse(&query) {
                    Ok(q) => q,
                    Err(e) => {
                        let _ = stream.sender.send(QueryResponses::Error(e.into()).into());
                        continue;
                    }
                };

                let requirements = collect_requirements(&query);
            }
        }
    }

    Ok(())
}

struct Requirements {}

enum Stack<'a> {
    Operation(Operation),
    Expr(&'a Expr<Pos>),
}

enum Eval<'a> {
    Literal(&'a Literal),
    Var(&'a Var),
}

fn collect_requirements(query: &Query<Pos>) -> Requirements {
    for from_stmt in &query.from_stmts {
        if let Some(pred) = &query.predicate {
            let mut stack = vec![&pred.expr];
            let mut params = vec![];

            while let Some(expr) = stack.pop() {
                match &expr.value {
                    geth_eventql::Value::Literal(l) => {
                        params.push(Eval::Literal(l));
                    }

                    geth_eventql::Value::Var(p) => {
                        params.push(Eval::Var(p));
                    }

                    // this one is more about looking type cues about the expected payload of an
                    // event
                    geth_eventql::Value::Record(record) => todo!(),
                    geth_eventql::Value::Array(exprs) => todo!(),
                    geth_eventql::Value::App { fun, params } => todo!(),
                    geth_eventql::Value::Binary { lhs, op, rhs } => todo!(),
                    geth_eventql::Value::Unary { op, expr } => todo!(),
                }
            }
        }
    }

    Requirements {}
}
