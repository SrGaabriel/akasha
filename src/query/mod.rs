pub mod err;
pub mod exec;
pub mod plan;
pub mod new_plan;

use std::collections::HashMap;
use crate::page::tuple::Value;

#[derive(Debug, Clone)]
pub enum Query {
    Select(SelectQuery),
    Insert(InsertQuery),
    Update(UpdateQuery),
    Delete(DeleteQuery),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub from: String,
    pub columns: Columns,
    pub conditions: Option<Condition>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertQuery {
    pub into: TableRef,
    pub columns: Vec<ColumnRef>,
    pub values: Vec<Value>,
    pub returning: Option<Columns>
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateQuery {
    pub table: TableRef,
    pub values: HashMap<ColumnRef, Value>,
    pub conditions: Option<Condition>,
    pub returning: Option<Columns>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteQuery {
    pub table: TableRef,
    pub conditions: Option<Condition>,
    pub returning: Option<Columns>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
    Compare {
        left: Expression,
        op: Operator,
        right: Expression,
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Column(ColumnRef),
    Value(Value),
    Function {
        name: String,
        args: Vec<Expression>,
    },
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    Subquery(Box<SelectQuery>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Like,
    NotLike
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulus
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum OrderBy {
    Asc(ColumnRef),
    Desc(ColumnRef),
}

pub type TableRef = String;
pub type ColumnRef = String;

#[derive(Debug, Clone, PartialEq)]
pub enum Columns {
    All,
    List(Vec<ColumnRef>),
}