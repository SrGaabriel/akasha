pub mod err;
pub mod file;
pub mod io;
pub mod pool;
pub mod tuple;

use crate::page::tuple::{Tuple, Value};
use std::mem::size_of;
use crate::util::trees::IndexedList;

pub const PAGE_SIZE: usize = 4096;
const HEADER_SIZE: usize = size_of::<u16>() /* slot_count */ + size_of::<u16>() /* free_space_pointer */;
const SLOT_META_SIZE: usize = size_of::<SlotMeta>();

#[repr(C)]
struct SlotMeta {
    offset: u16,
    length: u16,
}

pub struct Page<'a> {
    pub index: u32,
    pub data: &'a mut [u8; PAGE_SIZE],
}

impl<'a> Page<'a> {
    pub unsafe fn from_raw(index: u32, ptr: *mut u8) -> Self {
        let data = unsafe { &mut *(ptr as *mut [u8; PAGE_SIZE]) };
        Page { index, data }
    }

    pub fn init_new(&mut self) {
        self.data.fill(0);
        self.data[0..2].copy_from_slice(&0u16.to_le_bytes());

        let init_free_ptr = std::cmp::min(PAGE_SIZE, u16::MAX as usize) as u16;
        self.data[2..4].copy_from_slice(&init_free_ptr.to_le_bytes());
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<usize, String> {
        let bytes = tuple.to_bytes();
        let len = bytes.len() as u16;
        let d = &mut self.data;

        let slot_count = u16::from_le_bytes([d[0], d[1]]) as usize;
        let free_ptr = u16::from_le_bytes([d[2], d[3]]) as usize;

        if free_ptr == 0 || free_ptr > PAGE_SIZE {
            return Err("invalid page state: corrupted free pointer".into());
        }

        if len as usize > free_ptr {
            return Err("page full: tuple too large".into());
        }

        let new_slot_end = HEADER_SIZE + (slot_count + 1) * SLOT_META_SIZE;
        let new_data_start = free_ptr - (len as usize);

        if new_data_start <= new_slot_end {
            return Err("page full: not enough space".into());
        }

        let start = new_data_start;
        d[start..start + (len as usize)].copy_from_slice(&bytes);

        let meta = SlotMeta {
            offset: start as u16,
            length: len,
        };

        let slot_pos = HEADER_SIZE + slot_count * SLOT_META_SIZE;
        d[slot_pos..slot_pos + 2].copy_from_slice(&meta.offset.to_le_bytes());
        d[slot_pos + 2..slot_pos + 4].copy_from_slice(&meta.length.to_le_bytes());

        d[0..2].copy_from_slice(&((slot_count as u16 + 1).to_le_bytes()));
        d[2..4].copy_from_slice(&(new_data_start as u16).to_le_bytes());

        Ok(slot_count)
    }

    pub fn update_tuple(&mut self, idx: usize, values: IndexedList<Value>) -> Result<(), String> {
        let (offset, length) = self.get_tuple_pos(idx).ok_or("update: slot not found")?;
        let Tuple(mut current) = Tuple::from_bytes(&self.data[offset..offset + length]);
        for (column, value) in current.iter_mut().enumerate() {
            if let Some(new_value) = values.get(column as u32) {
                *value = new_value.clone();
            }
        }
        let bytes = Tuple(current).to_bytes();
        let new_len = bytes.len();

        // Shrinking or same-size tuples stay in place. Growing ones move into the free region.
        let new_offset = if new_len <= length {
            offset
        } else {
            let d = &self.data;
            let slot_count = u16::from_le_bytes([d[0], d[1]]) as usize;
            let free_ptr = u16::from_le_bytes([d[2], d[3]]) as usize;
            let slot_end = HEADER_SIZE + slot_count * SLOT_META_SIZE;
            if free_ptr < new_len || free_ptr - new_len <= slot_end {
                return Err("page full: updated tuple does not fit".into());
            }
            let start = free_ptr - new_len;
            self.data[2..4].copy_from_slice(&(start as u16).to_le_bytes());
            start
        };

        self.data[new_offset..new_offset + new_len].copy_from_slice(&bytes);
        let slot_pos = HEADER_SIZE + idx * SLOT_META_SIZE;
        self.data[slot_pos..slot_pos + 2].copy_from_slice(&(new_offset as u16).to_le_bytes());
        self.data[slot_pos + 2..slot_pos + 4].copy_from_slice(&(new_len as u16).to_le_bytes());
        Ok(())
    }

    pub fn get_tuple(&self, idx: usize) -> Option<Tuple> {
        let (offset, length) = self.get_tuple_pos(idx)?;
        let slice = &self.data[offset..offset + length];
        Some(Tuple::from_bytes(slice))
    }

    fn get_tuple_pos(&self, idx: usize) -> Option<(usize, usize)> {
        let d = &self.data;
        let slot_count = u16::from_le_bytes([d[0], d[1]]) as usize;

        if idx >= slot_count {
            return None;
        }

        let slot_pos = HEADER_SIZE + idx * SLOT_META_SIZE;
        let offset = u16::from_le_bytes([d[slot_pos], d[slot_pos + 1]]) as usize;
        let length = u16::from_le_bytes([d[slot_pos + 2], d[slot_pos + 3]]) as usize;
        Some((offset, length))
    }

    pub fn from_bytes(index: u32, data: &'a mut [u8; PAGE_SIZE]) -> Self {
        let page = Page { index, data };
        let free_ptr = u16::from_le_bytes([page.data[2], page.data[3]]) as usize;

        if free_ptr == 0 || free_ptr > PAGE_SIZE {
            let init_free_ptr = std::cmp::min(PAGE_SIZE, u16::MAX as usize) as u16;
            page.data[2..4].copy_from_slice(&init_free_ptr.to_le_bytes());
        }

        page
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0; PAGE_SIZE];
        bytes.copy_from_slice(self.data);
        bytes
    }

    pub fn available_space(&self) -> usize {
        let d = &self.data;
        let slot_count = u16::from_le_bytes([d[0], d[1]]) as usize;
        let free_ptr = u16::from_le_bytes([d[2], d[3]]) as usize;

        let slot_end = HEADER_SIZE + slot_count * SLOT_META_SIZE;

        if free_ptr > slot_end {
            free_ptr - slot_end
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn page(buf: &mut [u8; PAGE_SIZE]) -> Page<'_> {
        let mut page = Page { index: 0, data: buf };
        page.init_new();
        page
    }

    fn row(name: &str, age: i32) -> Tuple {
        Tuple(vec![Value::Text(name.into()), Value::Int(age)])
    }

    #[test]
    fn update_that_grows_relocates_without_touching_neighbours() {
        let mut buf = [0u8; PAGE_SIZE];
        let mut page = page(&mut buf);
        page.insert_tuple(&row("a", 1)).unwrap();
        page.insert_tuple(&row("b", 2)).unwrap();

        let grown = IndexedList::from_vec(vec![(0, Value::Text("a much longer name".into()))]);
        page.update_tuple(0, grown).unwrap();

        assert_eq!(page.get_tuple(0).unwrap().0, row("a much longer name", 1).0);
        assert_eq!(page.get_tuple(1).unwrap().0, row("b", 2).0);
    }

    #[test]
    fn update_that_shrinks_stays_in_place() {
        let mut buf = [0u8; PAGE_SIZE];
        let mut page = page(&mut buf);
        page.insert_tuple(&row("long name", 1)).unwrap();
        page.update_tuple(0, IndexedList::from_vec(vec![(0, Value::Text("x".into())), (1, Value::Int(9))])).unwrap();
        assert_eq!(page.get_tuple(0).unwrap().0, row("x", 9).0);
    }

    #[test]
    fn update_that_cannot_fit_is_rejected() {
        let mut buf = [0u8; PAGE_SIZE];
        let mut page = page(&mut buf);
        page.insert_tuple(&row("a", 1)).unwrap();
        let huge = IndexedList::from_vec(vec![(0, Value::Text("x".repeat(PAGE_SIZE)))]);
        assert!(page.update_tuple(0, huge).is_err());
        assert_eq!(page.get_tuple(0).unwrap().0, row("a", 1).0);
    }
}
