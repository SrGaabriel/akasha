use crate::page::Page;
use crate::page::pool::BufferPool;
use crate::page::tuple::Tuple;
use tokio::sync::Mutex;
use std::sync::Arc;
use futures::{Stream, Future, task::{Context, Poll}};
use std::pin::Pin;
use crate::page::io::IoManager;

pub struct TableHeap {
    pub file_id: u32,
    pub buffer_pool: Arc<BufferPool>,
    pub page_ids: Mutex<Vec<u32>>,
}

impl TableHeap {
    pub fn new(file_id: u32, buffer_pool: Arc<BufferPool>) -> Arc<Self> {
        Arc::new(TableHeap {
            file_id,
            buffer_pool,
            page_ids: Mutex::new(vec![0]),
        })
    }

    pub async fn from_existing(
        file_id: u32,
        buffer_pool: Arc<BufferPool>,
        io: Arc<IoManager>,
    ) -> Result<Arc<Self>, String> {
        let page_count = io.get_page_count(file_id).await.map_err(|e| e.to_string())?;
        let page_ids = (0..page_count).collect::<Vec<u32>>();
        Ok(Arc::new(TableHeap {
            file_id,
            buffer_pool,
            page_ids: Mutex::new(page_ids),
        }))
    }

    pub async fn insert_tuple(&self, tuple: &Tuple) -> Result<(), String> {
        let mut pages_guard = self.page_ids.lock().await;

        for &pid in pages_guard.iter() {
            let ptr = self.buffer_pool.get_page(self.file_id, pid).await;
            let mut page = unsafe { Page::from_raw(pid, ptr) };

            if page.insert_tuple(tuple).is_ok() {
                self.buffer_pool.unpin_and_flush(self.file_id, pid, true).await;
                return Ok(());
            } else {
                self.buffer_pool.unpin(self.file_id, pid, false);
            }
        }

        let new_pid = pages_guard.len() as u32;
        let ptr = self.buffer_pool.get_page(self.file_id, new_pid).await;
        let mut page = unsafe { Page::from_raw(new_pid, ptr) };

        page.init_new();
        page.insert_tuple(tuple)?;
        pages_guard.push(new_pid);

        self.buffer_pool.unpin_and_flush(self.file_id, new_pid, true).await;
        Ok(())
    }
}


enum OptimizedTableIteratorState {
    ReadyToFetchNextPage,
    FetchingPage {
        future: Pin<Box<dyn Future<Output = Option<(*mut u8, u32)>> + Send>>,
    },
    IteratingPage {
        page_id: u32,
        page_ptr: *mut u8,
        current_slot_idx: usize,
    },
    Finished,
}

pub struct OptimizedTableIterator {
    heap: Arc<TableHeap>,
    page_ids_snapshot: Arc<Vec<u32>>,
    current_page_idx_in_snapshot: usize,
    state: OptimizedTableIteratorState,
}

impl OptimizedTableIterator {
    fn new(heap: Arc<TableHeap>, page_ids_snapshot: Arc<Vec<u32>>) -> Self {
        OptimizedTableIterator {
            heap,
            page_ids_snapshot,
            current_page_idx_in_snapshot: 0,
            state: OptimizedTableIteratorState::ReadyToFetchNextPage,
        }
    }
}

impl Stream for OptimizedTableIterator {
    type Item = Tuple;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            match &mut this.state {
                OptimizedTableIteratorState::IteratingPage { page_id, page_ptr, current_slot_idx } => {
                    let page = unsafe { Page::from_raw(*page_id, *page_ptr) };

                    if let Some(tuple) = page.get_tuple(*current_slot_idx) {
                        *current_slot_idx += 1;
                        return Poll::Ready(Some(tuple));
                    } else {
                        this.heap.buffer_pool.unpin(this.heap.file_id, *page_id, false);

                        this.state = OptimizedTableIteratorState::ReadyToFetchNextPage;
                        continue;
                    }
                }

                OptimizedTableIteratorState::ReadyToFetchNextPage => {
                    if this.current_page_idx_in_snapshot >= this.page_ids_snapshot.len() {
                        this.state = OptimizedTableIteratorState::Finished;
                        return Poll::Ready(None);
                    }

                    let pid_to_fetch = this.page_ids_snapshot[this.current_page_idx_in_snapshot];
                    let heap_clone = this.heap.clone();

                    let fetch_future = async move {
                        let page_ptr = heap_clone.buffer_pool.get_page(heap_clone.file_id, pid_to_fetch).await;
                        if page_ptr.is_null() {
                            None
                        } else {
                            Some((page_ptr, pid_to_fetch))
                        }
                    };

                    this.state = OptimizedTableIteratorState::FetchingPage { future: Box::pin(fetch_future) };
                    continue;
                }

                OptimizedTableIteratorState::FetchingPage { future } => {
                    match future.as_mut().poll(cx) {
                        Poll::Ready(Some((ptr, pid))) => {
                            this.state = OptimizedTableIteratorState::IteratingPage {
                                page_id: pid,
                                page_ptr: ptr,
                                current_slot_idx: 0,
                            };
                            this.current_page_idx_in_snapshot += 1;
                            continue;
                        }
                        Poll::Ready(None) => {
                            this.current_page_idx_in_snapshot += 1;
                            this.state = OptimizedTableIteratorState::ReadyToFetchNextPage;
                            continue;
                        }
                        Poll::Pending => {
                            return Poll::Pending;
                        }
                    }
                }

                OptimizedTableIteratorState::Finished => {
                    return Poll::Ready(None);
                }
            }
        }
    }
}

pub async fn scan_table(table_ref: Arc<TableHeap>) -> OptimizedTableIterator {
    let page_ids_guard = table_ref.page_ids.lock().await;
    let snapshot = Arc::new(page_ids_guard.clone());
    OptimizedTableIterator::new(table_ref.clone(), snapshot)
}

unsafe impl Send for OptimizedTableIterator {}