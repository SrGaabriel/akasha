pub mod file;
pub mod pool;
pub mod tuple;
pub mod io;

use crate::page::tuple::Tuple;
use std::mem::size_of;

pub const PAGE_SIZE: usize = 4096;

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
        let data = &mut *(ptr as *mut [u8; PAGE_SIZE]);
        Page { index, data }
    }

    pub fn init_new(&mut self) {
        self.data.fill(0);
        self.data[0..2].copy_from_slice(&0u16.to_le_bytes());
        self.data[2..4].copy_from_slice(&(PAGE_SIZE as u16).to_le_bytes());
    }

    pub fn insert_tuple(&mut self, tuple: &Tuple) -> Result<usize, String> {
        let bytes = tuple.to_bytes();
        let len = bytes.len() as u16;
        let d = &mut self.data;
        let sc = u16::from_le_bytes([d[0], d[1]]);
        let fsp = u16::from_le_bytes([d[2], d[3]]);
        let slot_dir_end = 4 + (sc as usize + 1) * size_of::<SlotMeta>();
        if (fsp as usize) < slot_dir_end + len as usize {
            return Err("full".into());
        }
        let start = fsp as usize - len as usize;
        d[start..start + len as usize].copy_from_slice(&bytes);
        let meta = SlotMeta { offset: start as u16, length: len };
        let pos = 4 + sc as usize * size_of::<SlotMeta>();
        d[pos..pos + 2].copy_from_slice(&meta.offset.to_le_bytes());
        d[pos + 2..pos + 4].copy_from_slice(&meta.length.to_le_bytes());
        let sc1 = sc + 1;
        d[0..2].copy_from_slice(&sc1.to_le_bytes());
        d[2..4].copy_from_slice(&meta.offset.to_le_bytes());
        Ok(sc as usize)
    }

    pub fn get_tuple(&self, idx: usize) -> Option<Tuple> {
        let d = &self.data;
        let sc = u16::from_le_bytes([d[0], d[1]]) as usize;
        if idx >= sc {
            return None;
        }
        let pos = 4 + idx * size_of::<SlotMeta>();
        let offset = u16::from_le_bytes([d[pos], d[pos + 1]]) as usize;
        let length = u16::from_le_bytes([d[pos + 2], d[pos + 3]]) as usize;
        let slice = &d[offset..offset + length];
        Some(Tuple::from_bytes(slice))
    }

    pub fn from_bytes(index: u32, data: &'a mut [u8; PAGE_SIZE]) -> Self {
        Page { index, data }
    }

    pub fn to_bytes(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = [0; PAGE_SIZE];
        bytes.copy_from_slice(self.data);
        bytes
    }
}