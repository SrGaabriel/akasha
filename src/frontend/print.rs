use crate::frontend::ast::{Arena, Expr, NodeId, Visitor};

pub struct PrettyPrinter {
    indent: usize,
}

impl PrettyPrinter {
    pub fn new() -> Self {
        Self { indent: 0 }
    }

    fn write_indent(&self) {
        for _ in 0..self.indent {
            print!("  ");
        }
    }
}

impl<'a> Visitor<'a> for PrettyPrinter {
    type Result = ();

    fn visit(&mut self, arena: &'a Arena, node_id: NodeId) -> Self::Result {
        let expr = arena.get(node_id);

        self.write_indent();
        match expr {
            Expr::Reference(name) => {
                println!("Reference({})", arena.resolve_str(*name));
            }
            Expr::Number(num) => {
                println!("Number({})", arena.resolve_str(*num));
            }
            Expr::StringLit(s) => {
                println!("StringLit({:?})", arena.resolve_str(*s));
            }
            Expr::Bool(b) => {
                println!("Bool({})", b);
            }
            Expr::FieldAccess { base, field } => {
                println!("FieldAccess:");
                self.indent += 1;
                self.visit(arena, *base);
                self.write_indent();
                println!("Field: {}", arena.resolve_str(*field));
                self.indent -= 1;
            }
            Expr::UnaryOp { op, operand } => {
                println!("UnaryOp({:?}):", op);
                self.indent += 1;
                self.visit(arena, *operand);
                self.indent -= 1;
            },
            Expr::Instance(fields) => {
                println!("Instance:");
                self.indent += 1;
                for (field, value) in fields.iter() {
                    self.write_indent();
                    println!("Field: {}", arena.resolve_str(*field));
                    self.indent += 1;
                    self.visit(arena, *value);
                    self.indent -= 1;
                }
                self.indent -= 1;
            }
            Expr::BinaryOp { op, left, right } => {
                println!("BinaryOp({:?}):", op);
                self.indent += 1;
                self.write_indent();
                println!("Left:");
                self.indent += 1;
                self.visit(arena, *left);
                self.indent -= 1;
                self.write_indent();
                println!("Right:");
                self.indent += 1;
                self.visit(arena, *right);
                self.indent -= 1;
                self.indent -= 1;
            }
            Expr::FunctionCall { func, args } => {
                println!("FunctionCall:");
                self.indent += 1;
                self.write_indent();
                println!("Function:");
                self.indent += 1;
                self.visit(arena, *func);
                self.indent -= 1;
                self.write_indent();
                println!("Args:");
                self.indent += 1;
                for arg in args.iter() {
                    self.visit(arena, *arg);
                }
                self.indent -= 1;
                self.indent -= 1;
            }
            Expr::Tuple(items) => {
                println!("Tuple:");
                self.indent += 1;
                for item in items.iter() {
                    self.visit(arena, *item);
                }
                self.indent -= 1;
            }
            Expr::Array(items) => {
                println!("Array:");
                self.indent += 1;
                for item in items.iter() {
                    self.visit(arena, *item);
                }
                self.indent -= 1;
            }
            Expr::Block(items) => {
                println!("Block:");
                self.indent += 1;
                for item in items.iter() {
                    self.visit(arena, *item);
                }
                self.indent -= 1;
            }
            Expr::Lambda { params, body } => {
                println!("Lambda:");
                self.indent += 1;
                self.write_indent();
                print!("Params: ");
                for param in params {
                    print!("{} ", arena.resolve_str(*param));
                }
                println!();
                self.write_indent();
                println!("Body:");
                self.indent += 1;
                self.visit(arena, *body);
                self.indent -= 1;
                self.indent -= 1;
            }
            Expr::Let { name, value, body } => {
                println!("Let {} =", arena.resolve_str(*name));
                self.indent += 1;
                self.write_indent();
                println!("Value:");
                self.indent += 1;
                self.visit(arena, *value);
                self.indent -= 1;
                self.write_indent();
                println!("Body:");
                self.indent += 1;
                self.visit(arena, *body);
                self.indent -= 1;
                self.indent -= 1;
            }
        }
    }
}
