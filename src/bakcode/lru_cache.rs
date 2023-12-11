use std::borrow::Borrow;
use std::fmt::{self, Debug, Formatter};
use std::hash::{BuildHasher, Hash};
use std::ptr::null;
use std::sync::{Arc, Once, Mutex, Condvar};
use std::thread;
use std::time::{Instant, Duration};
use once_cell::sync::OnceCell;
use cached::Cached;
use cached::SizedCache;

pub struct DualQueueCache<K, V>
where
    K: Hash + Eq, // 添加 Eq 约束
    V: Send + Sync,
{
    capacity: usize,
    active_queue: Mutex<u8>,  // 1 表示 queue_one 活跃、2 表示 queue_two 活跃
    is_deleting: Mutex<bool>,
    queue_one: SizedCache<K, OnceCell<V>>,
    queue_two: SizedCache<K, OnceCell<V>>,
    cleaner_stop: Arc<(Mutex<bool>, Condvar)>, // 用来停止清理线程
}
// 针对 DualQueueCache<K, V> 的变体 DualQueueCache<K, Arc<V>> 实现
impl<K, V> DualQueueCache<K, Arc<V>>
where
    K: 'static + Hash + Ord + Clone + Send + Sync + Eq,
    V: 'static + Send + Sync
{
    #[must_use]
    pub fn new(capacity: usize) -> Arc<Self> {
        let cleaner_stop = Arc::new((Mutex::new(false), Condvar::new()));

        let cache = Arc::new(Self {
            capacity,
            active_queue: Mutex::new(1),
            is_deleting: Mutex::new(false),
            queue_one: SizedCache::with_size(capacity),
            queue_two: SizedCache::with_size(capacity),
            cleaner_stop: cleaner_stop.clone(),
        });
        let cleaner_cache = cache.clone();
        let cleaner_stop_thread = cleaner_stop.clone();
        thread::spawn(move || {
            let (lock, cvar) = &*cleaner_stop_thread;
            let mut stop_cleaner = lock.lock().unwrap();
            while !*stop_cleaner {
                stop_cleaner = cvar.wait_timeout(stop_cleaner, Duration::from_secs(10)).unwrap().0;
                // 清理并重置 active 队列
                let mut active_queue = cleaner_cache.active_queue.lock().unwrap();
                let current_queue = if *active_queue == 1 {
                    &cleaner_cache.queue_one
                } else {
                    &cleaner_cache.queue_two
                };

                if current_queue.cache_size() >= cleaner_cache.capacity {
                    // 重置 active 队列标志
                    *active_queue = if *active_queue == 1 { 2 } else { 1 };
                    drop(active_queue); // 释放锁，以允许其他线程访问

                    // let mut is_deleting = cleaner_cache.is_deleting.lock().unwrap();
                    // *is_deleting = true;
                    // drop(is_deleting);
                    current_queue.cache_clear(); // 清空队列

                    // let mut is_deleting = cleaner_cache.is_deleting.lock().unwrap();
                    // *is_deleting = false;
                } else {
                    drop(active_queue);
                }
            }
        });

        cache
        // Self { capacity, active_queue: Mutex::new(1), is_deleting: Mutex::new(false), queue_one: SizedCache::with_size(capacity), queue_two: SizedCache::with_size(capacity) }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Arc<Self> {
        Self::new(capacity)
    }

    #[must_use]
    pub fn is_empty(&self, queue_id: u8) -> bool {
        if queue_id==1 {
            self.queue_one.cache_size()==0
        } else {
            self.queue_two.cache_size()==0
        }
    }

    #[must_use]
    pub fn len(&self, queue_id: u8) -> usize {
        if queue_id==1 {
            self.queue_one.cache_size()
        } else {
            self.queue_two.cache_size()
        }    
    }

    #[must_use]
    pub fn clear(&self, queue_id: u8) {
        if queue_id==1 {
            self.queue_one.cache_clear()
        } else {
            self.queue_two.cache_clear()
        } 
    }


    pub fn resolve<F: FnOnce() -> V>(&self, key: K, init: F) -> Arc<V> {
        // Attempt to get the value from the active cache

        let active_queue = self.active_queue.lock().unwrap();
        let mut current_queue = if *active_queue == 1 {
            self.queue_one
        } else {
            self.queue_two
        };
        drop(active_queue); // 释放锁以避免在调用 init 时持有它

        if let Some(value) = current_queue.cache_get(&key) {
            let re = value.get_or_init(|| Arc::new(init()));
            return Arc::clone(&re);
        } else {
            // let value = OnceCell::new
            current_queue.cache_set(key.clone(), OnceCell::new());
            if let Some(value) = current_queue.cache_get(&key) {
                let re = value.get_or_init(|| Arc::new(init()));
                return Arc::clone(&re);
            } else {
                panic!("Failed to insert and retrieve new value from cache");
            }
        }
        

    }
}


impl<K, V> Drop for DualQueueCache<K, V>
where
    K: Hash + Eq,
    V: Send + Sync
{
    fn drop(&mut self) {
        let (lock, cvar) = &*self.cleaner_stop;
        let mut stop_cleaner = lock.lock().unwrap();
        *stop_cleaner = true;
        cvar.notify_one(); // 通知清理线程立即检查标志
    }
}