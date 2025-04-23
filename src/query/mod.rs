pub mod err;
pub mod exec;
pub mod planner;

use std::collections::HashMap;
use crate::page::tuple::Value;

pub struct Ref(usize);

pub struct Query {
    pub table: String,
    pub filter: Option<Filter>,
    pub insert: Option<HashMap<String, Value>>, // todo: remove hashmap
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