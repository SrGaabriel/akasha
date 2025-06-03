use chrono::Datelike;

#[derive(Debug)]
pub struct Tuple(pub Vec<Value>);

impl Tuple {
    pub fn to_bytes(&self) -> Vec<u8> {
        let total_size: usize = self.0.iter().map(Value::get_size).sum();
        let mut bytes = Vec::with_capacity(total_size);
        for value in &self.0 {
            value.to_bytes_into(&mut bytes);
        }
        bytes
    }

    pub fn from_bytes(data: &[u8]) -> Self {
        let mut values = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            let (val, size) = Value::read_from_bytes(&data[offset..]);
            values.push(val);
            offset += size;
        }
        Self(values)
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Text(String),
    Boolean(bool),
    Date(chrono::NaiveDate),
    DateTime(chrono::NaiveDateTime),
    Blob(Vec<u8>),
    Byte(u8),
}

impl Value {
    fn to_bytes_into(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Null => buf.push(self.id()),
            Value::Int(i) => {
                buf.push(self.id());
                buf.extend_from_slice(&i.to_le_bytes());
            }
            Value::Long(l) => {
                buf.push(self.id());
                buf.extend_from_slice(&l.to_le_bytes());
            }
            Value::Float(f) => {
                buf.push(self.id());
                buf.extend_from_slice(&f.to_le_bytes());
            }
            Value::Double(d) => {
                buf.push(self.id());
                buf.extend_from_slice(&d.to_le_bytes());
            }
            Value::Text(s) => {
                let bytes = s.as_bytes();
                buf.push(self.id());
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Value::Boolean(b) => {
                buf.push(self.id());
                buf.push(*b as u8);
            }
            Value::Date(date) => {
                buf.push(self.id());
                buf.extend_from_slice(&date.year().to_le_bytes());
                buf.extend_from_slice(&(date.month() as u16).to_le_bytes());
                buf.extend_from_slice(&(date.day() as u16).to_le_bytes());
            }
            Value::DateTime(dt) => {
                buf.push(self.id());
                buf.extend_from_slice(&dt.and_utc().timestamp().to_le_bytes());
                buf.extend_from_slice(&dt.and_utc().timestamp_subsec_nanos().to_le_bytes());
            }
            Value::Blob(b) => {
                buf.push(self.id());
                buf.extend_from_slice(&(b.len() as u16).to_le_bytes());
                buf.extend_from_slice(b);
            }
            Value::Byte(b) => {
                buf.push(self.id());
                buf.push(*b);
            }
        }
    }

    pub fn read_from_bytes(data: &[u8]) -> (Self, usize) {
        match data[0] {
            0x00 => (Value::Null, 1),
            0x01 => (
                Value::Int(i32::from_le_bytes(data[1..5].try_into().unwrap())),
                5,
            ),
            0x02 => (
                Value::Long(i64::from_le_bytes(data[1..9].try_into().unwrap())),
                9,
            ),
            0x03 => (
                Value::Float(f32::from_le_bytes(data[1..5].try_into().unwrap())),
                5,
            ),
            0x04 => (
                Value::Double(f64::from_le_bytes(data[1..9].try_into().unwrap())),
                9,
            ),
            0x05 => {
                let len = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
                let s = String::from_utf8_lossy(&data[3..3 + len]).to_string();
                (Value::Text(s), 3 + len)
            }
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
                #[allow(deprecated)]
                let dt = chrono::NaiveDateTime::from_timestamp(timestamp, nanos);
                (Value::DateTime(dt), 13)
            }
            0x09 => {
                let len = u16::from_le_bytes(data[1..3].try_into().unwrap()) as usize;
                let b = data[3..3 + len].to_vec();
                (Value::Blob(b), 3 + len)
            }
            0x0A => (Value::Byte(data[1]), 2),
            _ => panic!("Unknown value type: {}", data[0]),
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
            Value::Byte(_) => 2,
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
            Value::Blob(_) => 0x09,
            Value::Byte(_) => 0x0A,
        }
    }

    pub fn as_string(&self) -> Option<String> {
        if let Value::Text(s) = self {
            Some(s.clone())
        } else {
            None
        }
    }

    pub fn as_int(&self) -> Option<i32> {
        if let Value::Int(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        if let Value::Boolean(b) = self {
            Some(*b)
        } else {
            None
        }
    }

    pub fn as_byte(&self) -> Option<u8> {
        if let Value::Byte(b) = self {
            Some(*b)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub enum DataType {
    Null,     // 0x00
    Int,      // 0x01
    Long,     // 0x02
    Float,    // 0x03
    Double,   // 0x04
    Text,     // 0x05
    Boolean,  // 0x06
    Date,     // 0x07
    DateTime, // 0x08
    Blob,     // 0x09
    Byte,     // 0x0A
}

impl DataType {
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0x00 => Some(DataType::Null),
            0x01 => Some(DataType::Int),
            0x02 => Some(DataType::Long),
            0x03 => Some(DataType::Float),
            0x04 => Some(DataType::Double),
            0x05 => Some(DataType::Text),
            0x06 => Some(DataType::Boolean),
            0x07 => Some(DataType::Date),
            0x08 => Some(DataType::DateTime),
            0x09 => Some(DataType::Blob),
            0x0A => Some(DataType::Byte),
            _ => None,
        }
    }

    pub fn id(&self) -> u8 {
        match self {
            DataType::Null => 0x00,
            DataType::Int => 0x01,
            DataType::Long => 0x02,
            DataType::Float => 0x03,
            DataType::Double => 0x04,
            DataType::Text => 0x05,
            DataType::Boolean => 0x06,
            DataType::Date => 0x07,
            DataType::DateTime => 0x08,
            DataType::Blob => 0x09,
            DataType::Byte => 0x0A,
        }
    }
}
