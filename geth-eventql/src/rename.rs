use std::collections::{BTreeMap, HashMap};

use crate::{
    From, Pos, Query,
    parser::{Source, SourceType},
};

pub struct Lexical {
    pos: Pos,
    scope: u64,
}

#[derive(Default)]
struct Analysis {
    scope_id: u64,
    scopes: BTreeMap<u64, Scope>,
}

impl Analysis {
    fn scope_mut(&mut self) -> &mut Scope {
        self.scopes.entry(self.scope_id).or_insert_with(|| Scope {
            id: self.scope_id,
            properties: Default::default(),
        })
    }

    fn scope_id(&self) -> u64 {
        self.scope_id
    }
}

#[derive(Default)]
pub struct Properties {
    pub inner: Vec<String>,
}

pub struct Scope {
    id: u64,
    properties: HashMap<String, Properties>,
}

impl Scope {
    fn contains_variable(&self, name: &str) -> bool {
        self.properties.contains_key(name)
    }

    fn new_var(&mut self, name: String) {
        self.properties.insert(name, Properties::default());
    }
}

pub fn rename(query: Query<Pos>) -> eyre::Result<Query<Lexical>> {
    let mut analysis = Analysis::default();
    todo!()
}

fn remame_from(analysis: &mut Analysis, from: From<Pos>) -> eyre::Result<From<Lexical>> {
    let scope = {
        let scope = analysis.scope_mut();

        if scope.contains_variable(&from.ident) {
            eyre::bail!(
                "{}: variable '{}' already exists in this scope",
                from.tag,
                from.ident
            );
        }

        scope.new_var(from.ident.clone());
        scope.id
    };

    Ok(From {
        tag: Lexical {
            pos: from.tag,
            scope,
        },
        ident: from.ident,
        source: rename_source(analysis, from.source)?,
    })
}

fn rename_source(analysis: &mut Analysis, source: Source<Pos>) -> eyre::Result<Source<Lexical>> {
    Ok(Source {
        tag: Lexical {
            pos: source.tag,
            scope: analysis.scope_id(),
        },

        inner: rename_source_type(analysis, source.inner)?,
    })
}

fn rename_source_type(
    analysis: &mut Analysis,
    source_type: SourceType<Pos>,
) -> eyre::Result<SourceType<Lexical>> {
    todo!()
}
