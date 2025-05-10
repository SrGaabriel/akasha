pub mod transformer;
pub mod err;
pub mod optimizer;
pub mod compiler;
pub mod exec;
pub mod op;

use crate::frontend::ast::NodeId;
use crate::page::tuple::Value;
use crate::query::op::TableOp;

#[derive(Debug, Clone)]
pub enum Transaction {
    Insert {
        table: String,
        values: Vec<(String, Value)>,
        ops: Vec<TableOp>,
        returning: bool
    },
    Select {
        table: String,
        ops: Vec<TableOp>
    }
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Transaction {
        typ: TransactionType,
        operations: Vec<TransactionOp>,
    },

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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulus,
    Power,
    Concat,
    And,
    Or,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Eq,
    Neq,
    Gt,
    GtEq,
    Lt,
    LtEq,
    Like,
    NotLike
}