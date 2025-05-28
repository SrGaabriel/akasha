pub mod file;
pub mod io;
pub mod pool;
pub mod tuple;
pub mod err;

use crate::page::tuple::Tuple;
use std::mem::size_of;

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

    pub fn get_tuple(&self, idx: usize) -> Option<Tuple> {
        let d = &self.data;
        let slot_count = u16::from_le_bytes([d[0], d[1]]) as usize;

        if idx >= slot_count {
            return None;
        }

        let slot_pos = HEADER_SIZE + idx * SLOT_META_SIZE;
        let offset = u16::from_le_bytes([d[slot_pos], d[slot_pos + 1]]) as usize;
        let length = u16::from_le_bytes([d[slot_pos + 2], d[slot_pos + 3]]) as usize;

        let slice = &d[offset..offset + length];
        Some(Tuple::from_bytes(slice))
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
