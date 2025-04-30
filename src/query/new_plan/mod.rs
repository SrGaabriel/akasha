// This will probably have some redundant structs at the start, but we'll gradually be replacing old architecture with this one

pub mod transformer;

use std::collections::HashMap;
use crate::frontend::ast::NodeId;
use crate::page::tuple::Value;
use crate::query::BinaryOperator;

#[derive(Debug, Clone)]
pub enum PlanNode {
    Scan {
        table_name: String,
        predicate: Option<Box<Predicate>>,
    },
    Filter {
        predicate: Box<Predicate>,
        input: Box<PlanNode>,
    },
    Limit {
        limit: usize,
        offset: Option<usize>,
        input: Box<PlanNode>,
    },
    Pipe {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
    Update {
        table_name: String,
        assignments: Vec<(String, PlanExpr)>,
        predicate: Option<Box<Predicate>>,
    },
    Insert {
        table_name: String,
        columns: Vec<String>,
        values: Vec<PlanExpr>,
    },
    Delete {
        table_name: String,
        predicate: Option<Box<Predicate>>,
    },
    ValueStream {
        values: Vec<Value>,
    },
}

#[derive(Debug, Clone)]
pub enum PlanExpr {
    Column(String),
    Literal(Value),
    BinaryOp {
        left: Box<PlanExpr>,
        op: BinaryOperator,
        right: Box<PlanExpr>,
    },
    FunctionCall {
        name: String,
        args: Vec<PlanExpr>,
    },
    Case {
        when_clauses: Vec<(Box<Predicate>, Box<PlanExpr>)>,
        else_expr: Option<Box<PlanExpr>>,
    },
}

struct SymbolTable {
    symbols: HashMap<String, SymbolInfo>,
}

struct SymbolInfo {
    node_id: Option<NodeId>,
    plan_node: Option<PlanNode>,
}

#[derive(Debug, Clone)]
pub enum Predicate {
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
    Comparison {
        left: PlanExpr,
        op: BinaryOperator,
        right: PlanExpr,
    },
    Exists(Box<PlanNode>),
    InSubquery {
        expr: PlanExpr,
        subquery: Box<PlanNode>,
    },
    InList {
        expr: PlanExpr,
        list: Vec<Value>,
    },
    IsNull(PlanExpr),
    IsNotNull(PlanExpr),
    True,
    False,
}