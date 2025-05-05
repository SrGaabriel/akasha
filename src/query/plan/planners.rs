use std::sync::Arc;
use crate::page::tuple::{Tuple, Value};
use crate::query::err::{QueryError, QueryResult};
use crate::query::plan::{PlanResult, QueryPlanner, TableOp};
use crate::query::{Condition, Expression, InsertQuery, ComparisonOperator, Query, SelectQuery};
use crate::table::PhysicalTable;

pub struct DefaultQueryPlanner;

impl QueryPlanner for DefaultQueryPlanner {
    fn plan(&self, table: &PhysicalTable, query: &Query) -> QueryResult<PlanResult> {
        match query {
            Query::Select(select) => self.plan_select(table, select),
            Query::Insert(insert) => self.plan_insert(table, insert),
            Query::Update(_) => Err(QueryError::NotImplemented("Update queries not yet implemented".to_string())),
            Query::Delete(_) => Err(QueryError::NotImplemented("Delete queries not yet implemented".to_string())),
        }
    }
}

impl DefaultQueryPlanner {
    fn plan_select(&self, table: &PhysicalTable, select: &SelectQuery) -> QueryResult<PlanResult> {
        let mut ops = Vec::new();

        if let Some(condition) = &select.conditions {
            self.plan_condition(table, condition, &mut ops)?;
        }

        if let Some(limit) = select.limit {
            ops.push(TableOp::Limit(limit));
        }

        if let Some(offset) = select.offset {
            ops.push(TableOp::Offset(offset));
        }

        match &select.columns {
            crate::query::Columns::List(columns) => {
                let column_indices = columns.iter()
                    .map(|col| table.info.get_column_index(col)
                        .ok_or_else(|| QueryError::ColumnNotFound(col.clone())))
                    .collect::<Result<Vec<_>, _>>()?;
                ops.push(TableOp::Project(column_indices));
            },
            crate::query::Columns::All => {}
        }

        Ok(PlanResult::Stream(ops))
    }

    fn plan_insert(&self, table: &PhysicalTable, insert: &InsertQuery) -> QueryResult<PlanResult> {
        let specified_column_indices = insert.columns
            .iter()
            .map(|col| table.info.get_column_index(col)
                .ok_or_else(|| QueryError::ColumnNotFound(col.clone())))
            .collect::<Result<Vec<_>, _>>()?;

        let mut all_tuples = Vec::new();

        if insert.values.len() != insert.columns.len() {
            return Err(QueryError::ValueAndColumnMismatch(insert.columns.len(), insert.values.len()));
        }

        let mut tuple_values = vec![Value::Null; table.info.columns.len()];

        for (i, value) in specified_column_indices.iter().zip(insert.values.iter()) {
            tuple_values[*i] = value.clone();
        }

        for (i, column) in table.info.columns.values().enumerate() {
            if !specified_column_indices.contains(&i) && tuple_values[i] == Value::Null { // we can simplify this check probably, just not sure right now
                if let Some(default_value) = &column.default {
                    tuple_values[i] = default_value.clone();
                } else {
                    return Err(QueryError::ValueAndDefaultMissing(
                        table.info.columns.keys().collect::<Vec<_>>()[i].clone(),
                    ));
                }
            }
        }

        all_tuples.push(TableOp::Insert(tuple_values));

        let returning = match &insert.returning {
            Some(cols) => match cols {
                crate::query::Columns::List(requested_columns) => {
                    Some(requested_columns.iter()
                        .map(|col| table.info.get_column_index(col)
                            .ok_or_else(|| QueryError::ColumnNotFound(col.clone())))
                        .collect::<Result<Vec<_>, _>>()?)
                }
                crate::query::Columns::All => Some((0..table.info.columns.len()).collect()),
            },
            None => None,
        };

        Ok(PlanResult::ModifyData {
            ops: all_tuples,
            returning,
        })
    }

    fn plan_condition(&self, table: &PhysicalTable, condition: &Condition, ops: &mut Vec<TableOp>) -> QueryResult<()> {
        match condition {
            Condition::Compare { left, op, right } => {
                if let (Expression::Column(col_name), Expression::Value(value)) = (left, right) {
                    let column_index = table.info.get_column_index(col_name)
                        .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;

                    ops.push(TableOp::Filter {
                        column_index,
                        operator: op.clone(),
                        value: value.clone(),
                    });
                } else {
                    let filter_fn = self.compile_expression_filter(table, left, op, right)?;
                    ops.push(TableOp::PredicativeFilter(Arc::new(filter_fn)));
                }
            },
            Condition::And(conditions) => {
                for subcondition in conditions {
                    self.plan_condition(table, subcondition, ops)?;
                }
            },
            _ => return Err(QueryError::NotImplemented(
                "This condition type is not yet implemented".to_string()
            )),
        }
        Ok(())
    }

    fn compile_expression_filter(
        &self,
        table: &PhysicalTable,
        left: &Expression,
        op: &ComparisonOperator,
        right: &Expression,
    ) -> QueryResult<impl Fn(&Tuple) -> bool + Send + Sync + 'static> {
        let left_accessor = self.compile_expression_accessor(table, left)?;
        let right_accessor = self.compile_expression_accessor(table, right)?;

        let operator = op.clone();
        Ok(move |tuple: &Tuple| {
            let left_val = left_accessor(tuple);
            let right_val = right_accessor(tuple);

            // TODO: remove boilerplate
            match operator {
                ComparisonOperator::Eq => left_val == right_val,
                ComparisonOperator::NotEq => left_val != right_val,
                ComparisonOperator::Gt => left_val > right_val,
                ComparisonOperator::GtEq => left_val >= right_val,
                ComparisonOperator::Lt => left_val < right_val,
                ComparisonOperator::LtEq => left_val <= right_val,
                ComparisonOperator::Like => {
                    if let Value::Text(ref left_str) = left_val && let Value::Text(ref right_str) = right_val {
                        left_str.contains(right_str)
                    } else {
                        false
                    }
                },
                ComparisonOperator::NotLike => {
                    if let Value::Text(ref left_str) = left_val && let Value::Text(ref right_str) = right_val {
                        !left_str.contains(right_str)
                    } else {
                        false
                    }
                }
            }
        })
    }

    fn compile_expression_accessor(
        &self,
        table: &PhysicalTable,
        expr: &Expression,
    ) -> QueryResult<impl Fn(&Tuple) -> Value + Send + Sync + 'static> {
        match expr {
            Expression::Column(col_name) => {
                let column_index = table.info.get_column_index(col_name)
                    .ok_or_else(|| QueryError::ColumnNotFound(col_name.clone()))?;
                Ok(move |tuple: &Tuple| tuple.values[column_index].clone())
            },
            Expression::Value(value) => {
                let value = value.clone();
                Err(QueryError::NotImplemented(
                    format!("Value expressions not yet implemented: {:?}", value)
                ))
            },
            other => Err(QueryError::NotImplemented(
                format!("Expression type {:?} not yet implemented", other)
            )),
        }
    }
}