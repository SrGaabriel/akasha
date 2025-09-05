use std::collections::BTreeMap;

pub struct IndexedList<Value> {
    values: Vec<Value>,
    index_map: BTreeMap<u32, usize>,
}

impl<Value> IndexedList<Value> {
    pub fn new() -> Self {
        IndexedList {
            values: Vec::new(),
            index_map: BTreeMap::new(),
        }
    }

    pub fn from_vec(vec: Vec<(u32, Value)>) -> Self {
        let mut new = IndexedList::new();
        for (index, value) in vec {
            new.insert(index, value)
        }
        new
    }

    pub fn insert(&mut self, index: u32, value: Value) {
        self.values.push(value);
        self.index_map.insert(index, self.values.len() - 1);
    }

    pub fn get(&self, index: u32) -> Option<&Value> {
        self.index_map.get(&index).and_then(|&pos| self.values.get(pos))
    }
}

impl<T> Clone for IndexedList<T> where T: Clone {
    fn clone(&self) -> Self {
        IndexedList {
            values: self.values.clone(),
            index_map: self.index_map.clone()
        }
    }
}