pub mod err;
pub mod exec;
pub mod planner;

use crate::page::tuple::Value;

pub struct Ref(usize);

pub struct Query {
    pub table: String,
    pub filter: Option<Filter> // TODO: improve this
}

pub enum Filter {
    Eq(String, Value),
    Ne(String, Value)
} // todo: improve

impl Filter {
    pub fn reference(&self) -> String {
        match self {
            Filter::Eq(r, _) => r.clone(),
            Filter::Ne(r, _) => r.clone()
        }
    }

    pub fn value(&self) -> Value {
        match self {
            Filter::Eq(_, v) => v.clone(),
            Filter::Ne(_, v) => v.clone()
        }
    }
}

impl Ref {

}