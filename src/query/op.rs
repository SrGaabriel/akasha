use crate::page::tuple::{Tuple, Value};
use crate::query::ComparisonOperator;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Compares two values the way a filter expects. Numbers compare across widths, other
/// values only compare with values of the same type. A mismatch is never equal, less or greater.
pub fn compare_values(left: &Value, op: &ComparisonOperator, right: &Value) -> bool {
    match op {
        ComparisonOperator::Like | ComparisonOperator::NotLike => {
            let (Value::Text(haystack), Value::Text(needle)) = (left, right) else {
                return false;
            };
            return haystack.contains(needle.as_str()) == (*op == ComparisonOperator::Like);
        }
        _ => {}
    }

    let ordering = match (as_number(left), as_number(right)) {
        (Some(a), Some(b)) => a.partial_cmp(&b),
        _ if std::mem::discriminant(left) == std::mem::discriminant(right) => left.partial_cmp(right),
        _ => None,
    };

    match (op, ordering) {
        (ComparisonOperator::Neq, None) => true,
        (_, None) => false,
        (ComparisonOperator::Eq, Some(o)) => o == Ordering::Equal,
        (ComparisonOperator::Neq, Some(o)) => o != Ordering::Equal,
        (ComparisonOperator::Gt, Some(o)) => o == Ordering::Greater,
        (ComparisonOperator::GtEq, Some(o)) => o != Ordering::Less,
        (ComparisonOperator::Lt, Some(o)) => o == Ordering::Less,
        (ComparisonOperator::LtEq, Some(o)) => o != Ordering::Greater,
        (ComparisonOperator::Like | ComparisonOperator::NotLike, _) => unreachable!(),
    }
}

fn as_number(value: &Value) -> Option<f64> {
    match value {
        Value::Int(i) => Some(*i as f64),
        Value::Long(l) => Some(*l as f64),
        Value::Float(f) => Some(*f as f64),
        Value::Double(d) => Some(*d),
        Value::Byte(b) => Some(*b as f64),
        _ => None,
    }
}

pub enum TableOp {
    Filter {
        column_index: usize,
        operator: ComparisonOperator,
        value: Value,
    },
    Project(Vec<usize>),
    Limit(i32),
    Offset(i32),
    PredicativeFilter(Arc<dyn Fn(&Tuple) -> bool + Send + Sync>),
    Map(Arc<dyn Fn(&Tuple) -> Tuple + Send + Sync>),
}

impl Debug for TableOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TableOp::Filter {
                column_index,
                operator,
                value,
            } => {
                write!(
                    f,
                    "Filter(column_index: {}, operator: {:?}, value: {:?})",
                    column_index, operator, value
                )
            }
            TableOp::Project(indices) => {
                write!(f, "Project(indices: {:?})", indices)
            }
            TableOp::Limit(count) => {
                write!(f, "Limit({})", count)
            }
            TableOp::Offset(offset) => {
                write!(f, "Offset({})", offset)
            }
            TableOp::PredicativeFilter(_) => {
                write!(f, "PredicativeFilter")
            }
            TableOp::Map(_) => {
                write!(f, "Map")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ComparisonOperator::*;

    #[test]
    fn mismatched_types_never_match_ordering() {
        let text = Value::Text("users".into());
        assert!(!compare_values(&text, &Gt, &Value::Int(34)));
        assert!(!compare_values(&text, &Eq, &Value::Int(34)));
        assert!(compare_values(&text, &Neq, &Value::Int(34)));
    }

    #[test]
    fn numbers_compare_across_widths() {
        assert!(compare_values(&Value::Int(35), &Gt, &Value::Double(34.5)));
        assert!(compare_values(&Value::Long(7), &Eq, &Value::Int(7)));
        assert!(compare_values(&Value::Int(50), &GtEq, &Value::Int(50)));
        assert!(!compare_values(&Value::Int(50), &Lt, &Value::Int(50)));
    }

    #[test]
    fn like_only_applies_to_text() {
        let name = Value::Text("John Doe".into());
        assert!(compare_values(&name, &Like, &Value::Text("Doe".into())));
        assert!(compare_values(&name, &NotLike, &Value::Text("Smith".into())));
        assert!(!compare_values(&Value::Int(1), &Like, &Value::Text("1".into())));
    }
}
