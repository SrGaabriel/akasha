// This will probably have some redundant structs at the start, but we'll gradually be replacing old architecture with this one

pub mod transformer;
pub mod err;
pub mod optimizer;
pub mod compiler;
pub mod exec;
pub mod op;

use crate::frontend::ast::NodeId;
use crate::page::tuple::Value;
use crate::query::{BinaryOperator, ComparisonOperator};
use crate::query::new_plan::op::TableOp;

#[derive(Debug, Clone)]
pub enum Transaction {
    Insert {
        table: String,
        values: Vec<(String, TransactionValue)>,
        ops: Vec<TableOp>
    },
    Select {
        table: String,
        ops: Vec<TableOp>
    }
}
#[derive(Debug, Clone)]
pub struct TransactionExpr {
    pub typ: TransactionType,
    pub operations: Vec<TransactionOp>,
}

#[derive(Debug, Clone)]
pub enum TransactionValue {
    Row(Vec<(String, TransactionValue)>),
    Literal(Value),
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Transaction(TransactionExpr),

    Bind {
        input: Box<QueryExpr>,
        func: Box<QueryExpr>,
    },

    Lambda {
        params: Vec<String>,
        body: NodeId
    },

    Reference(String),
    Literal(Value),
    Column(String),

    BinaryOp {
        left: Box<QueryExpr>,
        op: BinaryOperator,
        right: Box<QueryExpr>,
    },

    Apply {
        func: Box<QueryExpr>,
        args: Vec<QueryExpr>,
    },

    Binding {
        name: String,
        value: Box<QueryExpr>,
        body: Box<QueryExpr>,
    },

    Predicate(Box<PredicateExpr>),
    Instance(Vec<(String, QueryExpr)>),

    BuiltInFunction {
        name: String,
    },
}

#[derive(Debug, Clone)]
pub enum TransactionType {
    Scan {
        table_name: String
    },
    Insert {
        table: String,
        value: Box<QueryExpr>
    }
}

#[derive(Debug, Clone)]
pub enum TransactionOp {
    Filter {
        predicate: Box<PredicateExpr>,
    },
    Limit {
        count: usize,
        offset: Option<usize>,
    }
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone)]
pub struct ProjectionExpr {
    pub expr: QueryExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub function: String,
    pub expr: QueryExpr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum PredicateExpr {
    Comparison {
        left: QueryExpr,
        op: ComparisonOperator,
        right: QueryExpr,
    },
    And(Box<PredicateExpr>, Box<PredicateExpr>),
    Or(Box<PredicateExpr>, Box<PredicateExpr>),
    Not(Box<PredicateExpr>),
    IsNull(QueryExpr),
    IsNotNull(QueryExpr),
    In(QueryExpr, Vec<QueryExpr>),
    Exists(Box<QueryExpr>)
}

type SymbolInfo = QueryExpr;