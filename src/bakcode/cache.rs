use std::borrow::Borrow;
use std::fmt::{self, Debug, Formatter};
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use std::time::Instant;
use once_cell::sync::OnceCell;
use flurry::{HashMap, DefaultHashBuilder};

pub struct ConcurrentCache<K, V, S = DefaultHashBuilder> {
    size: usize,
    seconds: u64,
    items: HashMap<K, (Instant, OnceCell<V>), S>
}

// 对于任何类型 K 和 V 都有一个 ConcurrentCache 的实现, 其中 S 被指定为 DefaultHashBuilder
// where 指定了 K 和 V 必须实现的 trait: 'static 表示类型中不包含任何非静态引用
// Hash：类型可以被哈希，这是 HashMap 的键类型所必需的。
// Ord：类型可以被排序，这通常是为了在哈希冲突时能够以一致的顺序存储和检索键。
// Clone：类型可以被克隆，这意味着可以创建其值的副本。
// Send：类型的所有权可以跨线程传递。
// Sync：类型可以安全地被多个线程引用。
impl<K, V> ConcurrentCache <K, Arc<V>, DefaultHashBuilder>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Send + Sync
{
    /// Constructs a new `ConcurrentCache` with the default hashing algorithm and an
    /// initial capacity of 0.
    #[must_use]
    pub fn new(size: usize, seconds: u64) -> Self {
        Self::with_capacity(size, seconds, 0)
    }

    /// Constructs a new `ConcurrentCache` with the default hashing algorithm and the
    /// specified initial capacity.
    #[must_use]
    pub fn with_capacity(size: usize, seconds: u64, capacity: usize) -> Self {
        Self::with_capacity_and_hasher(size, seconds, capacity, DefaultHashBuilder::default())
    }
}


impl<K, V, S> ConcurrentCache <K, Arc<V>, S>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Send + Sync,
    S: BuildHasher + Clone
{
    /// Constructs a new `ConcurrentCache` with the specified hasher and an initial
    /// capacity of 0.
    #[must_use]
    pub fn with_hasher(size: usize, seconds: u64, hasher: S) -> Self {
        Self::with_capacity_and_hasher(size, seconds, 0, hasher)
    }

    /// Constructs a new `ConcurrentCache` with the specified hasher and initial
    /// capacity.
    #[must_use]
    pub fn with_capacity_and_hasher(size: usize, seconds: u64, capacity: usize, hasher: S) -> Self {
        Self { size, seconds, items: HashMap::with_capacity_and_hasher(capacity, hasher) }
    }

    /// Returns `true` if the cache currently contains no items and `false`
    /// otherwise.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.pin().is_empty()
    }

    /// Returns the number of items currently in the cache.
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.pin().len()
    }

    /// Empties the cache of all items.
    pub fn clear(&self) {
        self.items.pin().clear()
    }

    /// Retrieves the value with the specified key, or initializes it if it is
    /// not present.
    ///
    /// If the key is present but the value is not fully resolved, the current
    /// thread will block until resolution completes. If the key is not present,
    /// `init` is executed to produce a value. In either case, an immutable
    /// reference to the value is returned.
    ///
    /// # Notes
    /// The resolution closure, `init`, does not provide access to the key being
    /// resolved. You may need to provide a copy of this value to the closure.
    /// This is done to allow for maximum concurrency, as it permits the key
    /// to be accessed by other threads during the resolution process.
    pub fn resolve<F: FnOnce() -> V>(&self, key: K, init: F) -> Arc<V> {
        let pinned = self.items.pin();

        if let Some(val_ref) = pinned.get(&key) {
            if val_ref.0.elapsed().as_secs() <= self.seconds {
                let result_ref = val_ref.1.get_or_init(|| Arc::new(init()));
                return Arc::clone(result_ref);
            }
        }

        match pinned.try_insert(
            key.clone(),
            (Instant::now(), OnceCell::new())
        ) {
            // key 不存在并已经成功插入新的 k-v inserted_ref
            Ok(inserted_ref) => {
                let result = inserted_ref.1.get_or_init(|| Arc::new(init()));
                if pinned.len() > self.size {
                    // let mut count = 0;
                    // Max size reached, try to evict expired items or random valid item
                    // 遍历缓存中所有项, 移除过期的项, 直到缓存的大小不超过最大值 self.size
                    // TODO 后续可以使用更加公平的 LRU 进行缓存, 增加过期时间
                    // pinned.retain(|_k, v| {
                    //     let valid = v.0.elapsed().as_secs() <= self.seconds;
                    //     if valid {
                    //         count += 1;
                    //         count <= self.size
                    //     } else {
                    //         false
                    //     }
                    // });
                    pinned.clear();
                }
                Arc::clone(result)
            }
            // key 已经存在或者已经过期
            Err(e) => {
                let val_ref = e.current;
                if val_ref.0.elapsed().as_secs() <= self.seconds {
                    Arc::clone(val_ref.1.get_or_init(|| Arc::new(init())))
                } else {
                    pinned.insert(key.clone(), e.not_inserted); // e.not_inserted 包含了新的 (Instant, OnceCell<V>), 表示尝试插入但未成功的值
                    let re = pinned.get(&key).unwrap().1.get_or_init(|| Arc::new(init()));
                    Arc::clone(re)
                }
            }
        }
    }
}