use chrono::Datelike;
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, PartialEq, PartialOrd, Deserialize, Serialize)]
pub enum Value {
    Null, // 0x00
    Int(i32), // 0x01
    Long(i64), // 0x02
    Float(f32), // 0x03
    Double(f64), // 0x04
    Text(String), // 0x05
    Boolean(bool), // 0x06
    Date(chrono::NaiveDate), // 0x07
    DateTime(chrono::NaiveDateTime), // 0x08
    Blob(Vec<u8>), // 0x09
}

impl Value {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![self.id()],
            Value::Int(i) => {
                let mut buf = vec![self.id()];
                buf.extend(&i.to_le_bytes());
                buf
            }
            Value::Float(f) => {
                let mut buf = vec![self.id()];
                buf.extend(&f.to_le_bytes());
                buf
            }
            Value::Boolean(b) => vec![self.id(), *b as u8],
            Value::Text(s) => {
                let bytes = s.as_bytes();
                let mut buf = vec![0x04];
                buf.extend(&(bytes.len() as u16).to_le_bytes());
                buf.extend(bytes);
                buf
            },
            Value::Blob(b) => {
                let mut buf = vec![self.id()];
                buf.extend(&(b.len() as u16).to_le_bytes());
                buf.extend(b);
                buf
            },
            Value::Date(date) => {
                let mut buf = vec![self.id()];
                buf.extend(&date.year().to_le_bytes());
                buf.extend(&(date.month() as u16).to_le_bytes());
                buf.extend(&(date.day() as u16).to_le_bytes());
                buf
            },
            Value::DateTime(dt) => {
                let mut buf = vec![self.id()];
                buf.extend(&dt.timestamp().to_le_bytes());
                buf.extend(&(dt.timestamp_subsec_nanos()).to_le_bytes());
                buf
            },
            Value::Long(l) => {
                let mut buf = vec![self.id()];
                buf.extend(&l.to_le_bytes());
                buf
            },
            Value::Double(d) => {
                let mut buf = vec![self.id()];
                buf.extend(&d.to_le_bytes());
                buf
            }
        }
    }

    pub fn read_from_bytes(data: &[u8]) -> (Self, usize) {
        match data[0] {
            0x00 => (Value::Null, 1),
            0x01 => {
                let i = i32::from_le_bytes(data[1..5].try_into().unwrap());
                (Value::Int(i), 5)
            }
            0x02 => {
                let l = i64::from_le_bytes(data[1..9].try_into().unwrap());
                (Value::Long(l), 9)
            }
            0x03 => {
                let f = f32::from_le_bytes(data[1..5].try_into().unwrap());
                (Value::Float(f), 5)
            }
            0x04 => {
                let len = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
                let s = String::from_utf8_lossy(&data[3..3 + len]).to_string();
                (Value::Text(s), 3 + len)
            },
            0x05 => {
                let len = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
                let b = data[3..3 + len].to_vec();
                (Value::Blob(b), 3 + len)
            },
            0x06 => (Value::Boolean(data[1] != 0), 2),
            0x07 => {
                let year = i32::from_le_bytes(data[1..5].try_into().unwrap());
                let month = u16::from_le_bytes(data[5..7].try_into().unwrap()) as u32;
                let day = u16::from_le_bytes(data[7..9].try_into().unwrap()) as u32;
                let date = chrono::NaiveDate::from_ymd_opt(year, month, day).unwrap();
                (Value::Date(date), 9)
            }
            0x08 => {
                let timestamp = i64::from_le_bytes(data[1..9].try_into().unwrap());
                let nanos = u32::from_le_bytes(data[9..13].try_into().unwrap());
                let dt = chrono::NaiveDateTime::from_timestamp_opt(timestamp, nanos).unwrap();
                (Value::DateTime(dt), 13)
            },
            _ => panic!("Unknown value type: {}", data[0])
        }
    }

    pub fn get_size(&self) -> usize {
        match self {
            Value::Int(_) => 5,
            Value::Float(_) => 5,
            Value::Boolean(_) => 2,
            Value::Text(s) => 3 + s.len(),
            Value::Blob(b) => 3 + b.len(),
            Value::Date(_) => 9,
            Value::DateTime(_) => 13,
            Value::Null => 1,
            Value::Long(_) => 9,
            Value::Double(_) => 9,
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            Value::Null => 0x00,
            Value::Int(_) => 0x01,
            Value::Long(_) => 0x02,
            Value::Float(_) => 0x03,
            Value::Double(_) => 0x04,
            Value::Text(_) => 0x05,
            Value::Boolean(_) => 0x06,
            Value::Date(_) => 0x07,
            Value::DateTime(_) => 0x08,
            Value::Blob(_) => 0x09
        }
    }
}
