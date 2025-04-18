pub mod file;
pub mod pool;
pub mod tuple;

use std::mem::size_of;
use crate::page::tuple::Tuple;

pub const PAGE_SIZE: usize = 4096;

pub struct Slot {
    pub offset: u16,
    pub length: u16,
}

#[derive(Clone, Debug)]
pub struct Page {
    pub index: u32,
    pub data: [u8; PAGE_SIZE],
    pub slot_count: usize,
    pub free_space_pointer: u16,
}

impl Page {
    pub fn new(index: u32) -> Self {
        Self {
            index,
            data: [0; PAGE_SIZE],
            slot_count: 0,
            free_space_pointer: PAGE_SIZE as u16,
        }
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<usize, String> {
        let bytes = tuple.to_bytes();
        let tuple_len = bytes.len() as u16;
        let slot_len = size_of::<Slot>() as u16;

        let slot_dir_end = 4 + (self.slot_count + 1) * size_of::<Slot>();
        let tuple_start = self.free_space_pointer as usize - tuple_len as usize;

        if tuple_start < slot_dir_end {
            return Err("Not enough space in page".to_string());
        }

        let offset = tuple_start as u16;
        self.data[offset as usize..(offset + tuple_len) as usize]
            .copy_from_slice(&bytes);

        let slot = Slot { offset, length: tuple_len };
        self.write_slot(self.slot_count, &slot);

        self.free_space_pointer = offset;
        let slot_index = self.slot_count;
        self.slot_count += 1;

        Ok(slot_index)
    }

    fn write_slot(&mut self, index: usize, slot: &Slot) {
        let pos = 4 + index * size_of::<Slot>();
        self.data[pos..pos + 2].copy_from_slice(&slot.offset.to_le_bytes());
        self.data[pos + 2..pos + 4].copy_from_slice(&slot.length.to_le_bytes());
    }

    fn read_slot(&self, index: usize) -> Option<Slot> {
        if index >= self.slot_count {
            return None;
        }

        let pos = 4 + index * size_of::<Slot>();
        let offset = u16::from_le_bytes(self.data[pos..pos + 2].try_into().ok()?);
        let length = u16::from_le_bytes(self.data[pos + 2..pos + 4].try_into().ok()?);
        Some(Slot { offset, length })
    }

    pub fn get_tuple(&self, index: usize) -> Option<Tuple> {
        let slot = self.read_slot(index)?;
        let end = (slot.offset + slot.length) as usize;
        let slice = self.data.get(slot.offset as usize..end)?;
        Some(Tuple::from_bytes(slice))
    }

    pub fn from_bytes(index: u32, data: [u8; PAGE_SIZE]) -> Self {
        let slot_count = u16::from_le_bytes([data[0], data[1]]) as usize;
        let free_space_pointer = u16::from_le_bytes([data[2], data[3]]);

        Self {
            index,
            data,
            slot_count,
            free_space_pointer,
        }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut out = self.data;

        out[0..2].copy_from_slice(&(self.slot_count as u16).to_le_bytes());
        out[2..4].copy_from_slice(&self.free_space_pointer.to_le_bytes());

        out
    }
}
