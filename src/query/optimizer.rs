use crate::query::QueryExpr;

pub trait QueryOptimizer {
    fn optimize(&self, plan: QueryExpr) -> QueryExpr;
}

pub struct IdentityOptimizer;

impl QueryOptimizer for IdentityOptimizer {
    fn optimize(&self, plan: QueryExpr) -> QueryExpr {
        plan
    }
}
