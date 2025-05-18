use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::{Acquire, Release, Relaxed, AcqRel}};
use std::sync::Arc;
use crate::page::PAGE_SIZE;
use std::cell::UnsafeCell;
use crate::page::io::IoManager;

const SHARD_COUNT: usize = 4;
const SLOTS_PER_SHARD: usize = 1024;

struct Slot {
    key: AtomicU64,
    ref_bit: AtomicBool,
    pin: AtomicUsize,
    dirty: AtomicBool,
    buf: UnsafeCell<[u8; PAGE_SIZE]>,
}

unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

impl Slot {
    fn new() -> Self {
        Slot {
            key: AtomicU64::new(u64::MAX),
            ref_bit: AtomicBool::new(false),
            pin: AtomicUsize::new(0),
            dirty: AtomicBool::new(false),
            buf: UnsafeCell::new([0u8; PAGE_SIZE]),
        }
    }
}

struct Shard {
    slots: Box<[Slot]>,
    hand: AtomicUsize,
    io: Arc<IoManager>,
}

impl Shard {
    fn new(io: Arc<IoManager>) -> Self {
        let mut v = Vec::with_capacity(SLOTS_PER_SHARD);
        for _ in 0..SLOTS_PER_SHARD { v.push(Slot::new()); }
        Shard { slots: v.into_boxed_slice(), hand: AtomicUsize::new(0), io }
    }

    async fn get_page(&self, file_id: u32, page_id: u32) -> *mut u8 {
        let key = make_key(file_id, page_id);

        for slot in self.slots.iter() {
            if slot.key.load(Acquire) == key {
                loop {
                    let current_pin = slot.pin.load(Acquire);
                    if current_pin == usize::MAX {
                        break;
                    }
                    if slot.pin.compare_exchange(current_pin, current_pin + 1, AcqRel, Relaxed).is_ok() {
                        slot.ref_bit.store(true, Release);
                        return slot.buf.get().cast();
                    }
                }
            }
        }

        loop {
            let idx = self.hand.fetch_add(1, Relaxed) % SLOTS_PER_SHARD;
            let s = &self.slots[idx];

            if s.pin.compare_exchange(0, usize::MAX, AcqRel, Relaxed).is_ok() {
                if s.ref_bit.swap(false, AcqRel) {
                    s.pin.store(0, Release);
                    continue;
                }
                let old_key = s.key.load(Acquire);
                s.key.store(key, Release);
                if s.dirty.swap(false, Acquire) {
                    let fid = (old_key >> 32) as u32;
                    let pid = old_key as u32;
                    let bytes = unsafe {
                        let ptr = s.buf.get();
                        (&*ptr)[..].to_vec()
                    };
                    unsafe { self.io.schedule_write(fid, pid, bytes) };
                }
                unsafe { self.io.read_into_buf(file_id, page_id, &mut *s.buf.get()).await; }
                s.pin.store(1, Release);
                s.ref_bit.store(true, Release);
                return s.buf.get().cast();
            }
        }
    }

    fn unpin(&self, file_id: u32, page_id: u32, is_dirty: bool) {
        let key = make_key(file_id, page_id);
        for slot in self.slots.iter() {
            if slot.key.load(Acquire) == key {
                let prev = slot.pin.fetch_sub(1, Release);
                if prev == 1 && is_dirty {
                    slot.dirty.store(true, Release);
                }
                return;
            }
        }
    }

    pub async fn flush_page(&self, file_id: u32, page_id: u32) {
        let key = make_key(file_id, page_id);
        for slot in self.slots.iter() {
            if slot.key.load(Acquire) == key {
                if slot.dirty.swap(false, AcqRel) {
                    let bytes = unsafe {
                        let ptr = slot.buf.get();
                        (*ptr)[..].to_vec()
                    };
                    self.io.schedule_write(file_id, page_id, bytes);
                }
                return;
            }
        }
    }

    pub async fn flush_all_dirty_pages_in_shard(&self) {
        for slot_idx in 0..self.slots.len() {
            let s = &self.slots[slot_idx];
            if s.dirty.load(Acquire) {
                let key = s.key.load(Acquire);
                if key == 0 {
                    continue;
                }
                let fid = (key >> 32) as u32;
                let pid = key as u32;

                if s.dirty.swap(false, AcqRel) {
                    let bytes = unsafe {
                        let ptr = s.buf.get();
                        (*ptr)[..].to_vec()
                    };
                    self.io.schedule_write(fid, pid, bytes);
                }
            }
        }
    }
}

pub struct BufferPool {
    shards: Vec<Arc<Shard>>,
}

impl BufferPool {
    pub fn new(io: Arc<IoManager>) -> Arc<Self> {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT { shards.push(Arc::new(Shard::new(io.clone()))); }
        Arc::new(BufferPool { shards })
    }

    fn pick_shard(&self, file_id: u32, page_id: u32) -> usize {
        (file_id as usize ^ page_id as usize) & (SHARD_COUNT - 1)
    }

    pub async fn get_page(&self, file_id: u32, page_id: u32) -> *mut u8 {
        let s = self.pick_shard(file_id, page_id);
        self.shards[s].get_page(file_id, page_id).await
    }

    pub fn unpin(&self, file_id: u32, page_id: u32, is_dirty: bool) {
        let s = self.pick_shard(file_id, page_id);
        self.shards[s].unpin(file_id, page_id, is_dirty);
    }

    pub async fn unpin_and_flush(&self, file_id: u32, page_id: u32, is_dirty: bool) {
        let s = self.pick_shard(file_id, page_id);
        let shard = &self.shards[s];
        shard.unpin(file_id, page_id, is_dirty);
        shard.flush_page(file_id, page_id).await;
    }

    pub async fn flush(&self) -> std::io::Result<()> {
        for shard in &self.shards {
            for slot in shard.slots.iter() {
                if slot.dirty.load(Acquire) {
                    let key = slot.key.load(Acquire);
                    let file_id = (key >> 32) as u32;
                    let page_id = key as u32;
                    let bytes = unsafe {
                        let ptr = slot.buf.get();
                        (&*ptr)[..].to_vec()
                    };
                    self.shards[self.pick_shard(file_id, page_id)]
                        .io
                        .schedule_write(file_id, page_id, bytes);
                    slot.dirty.store(false, Release);
                }
            }
        }
        Ok(())
    }
}

fn make_key(file_id: u32, page_id: u32) -> u64 {
    ((file_id as u64) << 32) | page_id as u64
}
