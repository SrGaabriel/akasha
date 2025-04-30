use std::collections::HashMap;
use crate::frontend::ast::Arena;
use crate::table::TableCatalog;

pub struct AstToQueryTransformer<'a> {
    arena: &'a Arena,
    catalog: &'a TableCatalog,
    table_aliases: HashMap<String, String>,
    current_scope: Vec<crate::query::new_plan::SymbolTable>,
}
