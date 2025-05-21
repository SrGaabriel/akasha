pub mod transformer;
pub mod err;
pub mod optimizer;
pub mod compiler;
pub mod exec;
pub mod op;

use std::rc::Rc;
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
        input: Rc<QueryExpr>,
        func: Rc<QueryExpr>,
    },

    Lambda {
        params: Vec<String>,
        body: NodeId
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
        table_name: String,
        value: Rc<QueryExpr>
    }
}

#[derive(Debug, Clone)]
pub enum TransactionOp {
    Filter {
        predicate: Rc<PredicateExpr>,
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
    Exists(Rc<QueryExpr>)
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
    NotLike
}