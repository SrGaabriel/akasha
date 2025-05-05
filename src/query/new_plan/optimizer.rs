use crate::query::new_plan::PlanNode;

pub trait QueryOptimizer {
    fn optimize(&self, plan: PlanNode) -> PlanNode;
}

pub struct IdentityOptimizer;

impl QueryOptimizer for IdentityOptimizer {
    fn optimize(&self, plan: PlanNode) -> PlanNode {
        plan
    }
}