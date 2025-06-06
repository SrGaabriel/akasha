mod builtins;
pub mod compiler;
pub mod err;
pub mod exec;
pub mod op;
pub mod optimizer;
mod stream;
pub mod transformer;

use crate::frontend::ast::NodeId;
use crate::page::tuple::Value;
use crate::query::op::TableOp;
use std::rc::Rc;

#[derive(Debug)]
pub enum Transaction {
    Insert {
        table: String,
        values: Vec<(u32, Value)>,
        ops: Vec<TableOp>,
        returning: Option<Vec<usize>>,
    },
    Select {
        table: String,
        ops: Vec<TableOp>,
    },
}

#[derive(Debug, Clone)]
pub enum QueryExpr {
    Transaction {
        typ: TransactionType,
        operations: Vec<TransactionOp>,
    },

    Bind {
        input: Rc<QueryExpr>,
        func: Rc<QueryExpr>,
    },

    Lambda {
        params: Vec<String>,
        body: NodeId,
    },

    Reference(String),
    Literal(Value),
    Column(String),

    BinaryOp {
        left: Rc<QueryExpr>,
        op: BinaryOperator,
        right: Rc<QueryExpr>,
    },

    Apply {
        func: Rc<QueryExpr>,
        args: Vec<QueryExpr>,
    },

    Binding {
        name: String,
        value: Rc<QueryExpr>,
        body: Rc<QueryExpr>,
    },

    Predicate(Rc<PredicateExpr>),
    Instance(Vec<(String, QueryExpr)>),
    Tuple(Vec<String>),

    BuiltInFunction {
        name: String,
    },
}

#[derive(Debug, Clone)]
pub enum TransactionType {
    Scan {
        table_name: String,
    },
    Insert {
        table_name: String,
        value: Rc<QueryExpr>,
        returning: Option<Vec<String>>,
    },
}

#[derive(Debug, Clone)]
pub enum TransactionOp {
    Filter { predicate: Rc<PredicateExpr> },
    Limit { count: i32 },
    Project { columns: Vec<String> },
    Offset { offset: i32 },
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
pub enum PredicateExpr {
    Comparison {
        left: QueryExpr,
        op: ComparisonOperator,
        right: QueryExpr,
    },
    And(Rc<PredicateExpr>, Rc<PredicateExpr>),
    Or(Rc<PredicateExpr>, Rc<PredicateExpr>),
    Not(Rc<PredicateExpr>),
    IsNull(QueryExpr),
    IsNotNull(QueryExpr),
    In(QueryExpr, Vec<QueryExpr>),
    Exists(Rc<QueryExpr>),
}

type SymbolInfo = Rc<QueryExpr>;

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
    NotLike,
}
