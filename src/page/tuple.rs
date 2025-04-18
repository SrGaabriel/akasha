#[derive(Clone, Debug)]
pub struct Tuple {
    pub values: Vec<Value>,
}

impl Tuple {
    pub fn to_bytes(&self) -> Vec<u8> {
        self.values.iter().flat_map(Value::to_bytes).collect()
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let mut values = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            let (val, size) = Value::read_from_bytes(&data[offset..]);
            values.push(val);
            offset += size;
        }

        Self { values }
    }
}

#[derive(Clone, Debug)]
pub enum Value {
    Int(i32),
    Float(f32),
    Bool(bool),
    String(String),
}

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Int(i) => {
                let mut buf = vec![0x01];
                buf.extend(&i.to_le_bytes());
                buf
            }
            Value::Float(f) => {
                let mut buf = vec![0x02];
                buf.extend(&f.to_le_bytes());
                buf
            }
            Value::Bool(b) => vec![0x03, *b as u8],
            Value::String(s) => {
                let bytes = s.as_bytes();
                let mut buf = vec![0x04];
                buf.extend(&(bytes.len() as u16).to_le_bytes());
                buf.extend(bytes);
                buf
            }
        }
    }

    pub fn read_from_bytes(data: &[u8]) -> (Self, usize) {
        match data[0] {
            0x01 => {
                let i = i32::from_le_bytes(data[1..5].try_into().unwrap());
                (Value::Int(i), 5)
            }
            0x02 => {
                let f = f32::from_le_bytes(data[1..5].try_into().unwrap());
                (Value::Float(f), 5)
            }
            0x03 => {
                (Value::Bool(data[1] != 0), 2)
            }
            0x04 => {
                let len = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
                let s = String::from_utf8_lossy(&data[3..3 + len]).to_string();
                (Value::String(s), 3 + len)
            }
            _ => panic!("Unknown value type"),
        }
    }

    pub fn get_size(&self) -> usize {
        match self {
            Value::Int(_) => 5,
            Value::Float(_) => 5,
            Value::Bool(_) => 2,
            Value::String(s) => 3 + s.len(),
        }
    }
}
