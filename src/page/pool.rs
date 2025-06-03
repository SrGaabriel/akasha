use crate::page::io::IoManager;
use crate::page::{PAGE_SIZE, Page};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool, AtomicU64, AtomicUsize,
    Ordering::{AcqRel, Acquire, Relaxed, Release},
};

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
        for _ in 0..SLOTS_PER_SHARD {
            v.push(Slot::new());
        }
        Shard {
            slots: v.into_boxed_slice(),
            hand: AtomicUsize::new(0),
            io,
        }
    }

    async fn get_page(&self, file_id: u32, page_id: u32) -> *mut u8 {
        let key_to_find = make_key(file_id, page_id);

        'search_loop: for slot_idx in 0..SLOTS_PER_SHARD {
            let current_slot = &self.slots[slot_idx];
            if current_slot.key.load(Acquire) == key_to_find {
                loop {
                    let pin_val = current_slot.pin.load(Acquire);
                    if pin_val == usize::MAX {
                        break 'search_loop;
                    }
                    match current_slot
                        .pin
                        .compare_exchange(pin_val, pin_val + 1, AcqRel, Relaxed)
                    {
                        Ok(_) => {
                            if current_slot.key.load(Acquire) == key_to_find {
                                current_slot.ref_bit.store(true, Release);
                                return current_slot.buf.get().cast();
                            } else {
                                current_slot.pin.fetch_sub(1, Release);
                                break 'search_loop;
                            }
                        }
                        Err(_) => {
                            // CAS failed, pin_val changed, loop will reload and retry
                        }
                    }
                }
            }
        }

        loop {
            let victim_idx = self.hand.fetch_add(1, Relaxed) % SLOTS_PER_SHARD;
            let victim_slot = &self.slots[victim_idx];

            if victim_slot.pin.load(Acquire) == 0 {
                if victim_slot
                    .pin
                    .compare_exchange(0, usize::MAX, AcqRel, Relaxed)
                    .is_ok()
                {
                    if victim_slot.ref_bit.swap(false, AcqRel) {
                        victim_slot.pin.store(0, Release);
                        tokio::task::yield_now().await;
                        continue;
                    }

                    let old_key = victim_slot.key.load(Acquire);
                    if old_key != u64::MAX && victim_slot.dirty.swap(false, AcqRel) {
                        let old_file_id = (old_key >> 32) as u32;
                        let old_page_id = old_key as u32;
                        let page_data_to_write = unsafe { (*victim_slot.buf.get()).to_vec() };
                        self.io
                            .schedule_write(old_file_id, old_page_id, page_data_to_write);
                    }

                    victim_slot.key.store(key_to_find, Release);

                    let page_buffer_for_io = unsafe { &mut *victim_slot.buf.get() };
                    let _res = self
                        .io
                        .read_into_buf(file_id, page_id, page_buffer_for_io)
                        .await;

                    let final_raw_ptr = victim_slot.buf.get();

                    victim_slot.pin.store(1, Release);
                    victim_slot.ref_bit.store(true, Release);
                    return final_raw_ptr.cast();
                }
            }
            tokio::task::yield_now().await;
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
                    let bytes = unsafe { (*slot.buf.get())[..].to_vec() };
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
                if key == u64::MAX {
                    continue;
                }
                if s.dirty.swap(false, AcqRel) {
                    let fid = (key >> 32) as u32;
                    let pid = key as u32;
                    let bytes = unsafe { (*s.buf.get())[..].to_vec() };
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
        for _ in 0..SHARD_COUNT {
            shards.push(Arc::new(Shard::new(Arc::clone(&io))));
        }
        Arc::new(BufferPool { shards })
    }

    fn pick_shard(&self, file_id: u32, page_id: u32) -> usize {
        (file_id as usize ^ page_id as usize) & (SHARD_COUNT - 1)
    }

    pub async fn get_page_ptr(&self, file_id: u32, page_id: u32) -> *mut u8 {
        let s = self.pick_shard(file_id, page_id);
        self.shards[s].get_page(file_id, page_id).await
    }

    pub async fn get_page_raw(&self, file_id: u32, page_id: u32) -> Page {
        let ptr = self.get_page_ptr(file_id, page_id).await;
        unsafe { Page::from_raw(page_id, ptr) }
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

    pub async fn flush(&self) {
        let mut futures = Vec::new();
        for shard_arc in &self.shards {
            futures.push(shard_arc.flush_all_dirty_pages_in_shard());
        }
        futures::future::join_all(futures).await;
    }
}

fn make_key(file_id: u32, page_id: u32) -> u64 {
    ((file_id as u64) << 32) | page_id as u64
}
