use crate::frontend::lexer::TokenKind;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

#[derive(Default)]
pub struct Interner {
    strings: HashMap<Arc<str>, StrId>,
    ids: Vec<Arc<str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

impl Interner {
    pub fn new() -> Self {
        Self {
            strings: HashMap::new(),
            ids: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            strings: HashMap::with_capacity(capacity),
            ids: Vec::with_capacity(capacity),
        }
    }

    pub fn intern(&mut self, s: &str) -> StrId {
        if let Some(&id) = self.strings.get(s) {
            return id;
        }

        let s = Arc::from(s);
        let id = StrId(self.ids.len() as u32);
        self.strings.insert(Arc::clone(&s), id);
        self.ids.push(s);
        id
    }

    pub fn resolve(&self, id: StrId) -> &str {
        &self.ids[id.0 as usize]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(u32);

impl NodeId {
    pub const INVALID: NodeId = NodeId(u32::MAX);

    #[inline]
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone)]
pub enum Expr {
    Reference(StrId),
    Number(StrId),
    StringLit(StrId),
    Bool(bool),
    FieldAccess {
        base: NodeId,
        field: StrId,
    },

    UnaryOp {
        op: TokenKind,
        operand: NodeId,
    },

    BinaryOp {
        op: TokenKind,
        left: NodeId,
        right: NodeId,
    },
    FunctionCall {
        func: NodeId,
        args: SmallVec<NodeId, 4>,
    },
    Tuple(SmallVec<NodeId, 4>),
    Array(SmallVec<NodeId, 4>),
    Block(SmallVec<NodeId, 4>),
    Lambda {
        params: Vec<StrId>,
        body: NodeId,
    },
    Instance(SmallVec<(StrId, NodeId), 4>),
    Let {
        name: StrId,
        value: NodeId,
        body: NodeId,
    },
}
pub struct Arena {
    nodes: Vec<Expr>,
    interner: Interner,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            nodes: Vec::new(),
            interner: Interner::new(),
        }
    }

    pub fn with_capacity(node_capacity: usize, string_capacity: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(node_capacity),
            interner: Interner::with_capacity(string_capacity),
        }
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.interner = Interner::new();
    }

    #[inline]
    pub fn alloc(&mut self, expr: Expr) -> NodeId {
        let id = NodeId(self.nodes.len() as u32);
        self.nodes.push(expr);
        id
    }

    #[inline]
    pub fn get(&self, id: NodeId) -> &Expr {
        &self.nodes[id.index()]
    }

    #[inline]
    pub fn get_mut(&mut self, id: NodeId) -> &mut Expr {
        &mut self.nodes[id.index()]
    }

    pub fn intern_str(&mut self, s: &str) -> StrId {
        self.interner.intern(s)
    }

    pub fn resolve_str(&self, id: StrId) -> &str {
        self.interner.resolve(id)
    }

    pub fn create_reference(&mut self, name: &str) -> NodeId {
        let str_id = self.intern_str(name);
        self.alloc(Expr::Reference(str_id))
    }

    pub fn create_number(&mut self, value: &str) -> NodeId {
        let str_id = self.intern_str(value);
        self.alloc(Expr::Number(str_id))
    }

    pub fn create_string_lit(&mut self, value: &str) -> NodeId {
        let str_id = self.intern_str(value);
        self.alloc(Expr::StringLit(str_id))
    }

    pub fn create_bool(&mut self, value: bool) -> NodeId {
        self.alloc(Expr::Bool(value))
    }

    pub fn create_binary_op(&mut self, op: TokenKind, left: NodeId, right: NodeId) -> NodeId {
        self.alloc(Expr::BinaryOp { op, left, right })
    }

    pub fn create_function_call(&mut self, func: NodeId, args: &[NodeId]) -> NodeId {
        let mut args_vec = SmallVec::with_capacity(args.len());
        for &arg in args {
            args_vec.push(arg);
        }
        self.alloc(Expr::FunctionCall {
            func,
            args: args_vec,
        })
    }

    pub fn create_tuple(&mut self, items: &[NodeId]) -> NodeId {
        let mut tuple = SmallVec::with_capacity(items.len());
        for &item in items {
            tuple.push(item);
        }
        self.alloc(Expr::Tuple(tuple))
    }

    pub fn create_array(&mut self, items: &[NodeId]) -> NodeId {
        let mut array = SmallVec::with_capacity(items.len());
        for &item in items {
            array.push(item);
        }
        self.alloc(Expr::Array(array))
    }

    pub fn create_block(&mut self, exprs: &[NodeId]) -> NodeId {
        let mut block = SmallVec::with_capacity(exprs.len());
        for &expr in exprs {
            block.push(expr);
        }
        self.alloc(Expr::Block(block))
    }

    pub fn create_lambda(&mut self, param_names: &[&str], body: NodeId) -> NodeId {
        let params = param_names
            .iter()
            .map(|&name| self.intern_str(name))
            .collect();
        self.alloc(Expr::Lambda { params, body })
    }

    pub fn create_instance(&mut self, fields: &[(&str, NodeId)]) -> NodeId {
        let instance = fields
            .iter()
            .map(|&(name, value)| {
                let name_id = self.intern_str(name);
                (name_id, value)
            })
            .collect();
        self.alloc(Expr::Instance(instance))
    }

    pub fn create_field_access(&mut self, base: NodeId, field: &str) -> NodeId {
        let field_id = self.intern_str(field);
        self.alloc(Expr::FieldAccess {
            base,
            field: field_id,
        })
    }

    pub fn create_let(&mut self, name: &str, value: NodeId, body: NodeId) -> NodeId {
        let name_id = self.intern_str(name);
        self.alloc(Expr::Let {
            name: name_id,
            value,
            body,
        })
    }

    pub fn extract_function_call(&self, id: NodeId) -> Option<(NodeId, Vec<NodeId>)> {
        match self.get(id) {
            Expr::FunctionCall { func, args } => Some((*func, args.to_vec())),
            _ => None,
        }
    }
}

pub trait Visitor<'a> {
    type Result;

    fn visit(&mut self, arena: &'a Arena, node_id: NodeId) -> Self::Result;
}
