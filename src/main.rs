use crate::page::Page;
use crate::page::tuple::{Tuple, Value};

pub mod page;
mod table;

fn main() {
    let mut page = Page::new(0);

    let tup = Tuple {
        values: vec![
            Value::Int(42),
            Value::Bool(true),
            Value::String("hello".into()),
        ],
    };

    let id = page.insert_tuple(&tup).unwrap();
    let out = page.get_tuple(id).unwrap();

    dbg!(out);
}
