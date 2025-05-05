// This will probably have some redundant structs at the start, but we'll gradually be replacing old architecture with this one

pub mod transformer;
pub mod err;
pub mod optimizer;

use crate::frontend::ast::{Expr, NodeId};
use crate::page::tuple::Value;
use crate::query::{BinaryOperator, ComparisonOperator};

#[derive(Debug, Clone)]
pub enum PlanNode {
    TableScan {
        table_name: String,
        filter: Option<Box<Predicate>>,
    },
    Filter {
        predicate: Box<Predicate>,
        input: Box<PlanNode>,
    },
    Map {
        projection: Vec<ProjectionExpr>,
        input: Box<PlanNode>,
    },
    Apply {
        func: Box<PlanNode>,
        args: Vec<PlanNode>,
    },
    Pipe {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
    },
    Limit {
        count: usize,
        offset: Option<usize>,
        input: Box<PlanNode>,
    },
    Lambda {
        params: Vec<String>,
        body: NodeId,
    },
    Values(Vec<Value>),
    Binding {
        name: String,
        value: Box<PlanNode>,
        body: Box<PlanNode>,
    },
    Reference(String),
    PartiallyApplied {
        func: String,
        args: Vec<PlanNode>,
    },
    BuiltInFunction {
        name: String
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
        op: ComparisonOperator,
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

#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    pub expr: Expr,
    pub alias: Option<String>,
}