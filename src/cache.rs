use std::borrow::Borrow;
use std::fmt::{self, Debug, Formatter};
use std::hash::{BuildHasher, Hash};
use std::time::Instant;

use once_cell::sync::OnceCell;
use flurry::{HashMap, DefaultHashBuilder};

pub struct ConcurrentCache<K, V, S = DefaultHashBuilder> {
    size: usize,
    seconds: u64,
    items: HashMap<K, (Instant, OnceCell<V>), S>
}

impl<K, V> ConcurrentCache <K, V, DefaultHashBuilder>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Clone + Send + Sync
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


impl<K, V, S> ConcurrentCache <K, V, S>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Clone + Send + Sync,
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
    pub fn resolve<F: FnOnce() -> V>(&self, key: K, init: F) -> V {
        let pinned = self.items.pin();

        if let Some(val_ref) = pinned.get(&key) {
            if val_ref.0.elapsed().as_secs() <= self.seconds {
                let result_ref = val_ref.1.get_or_init(init);
                let result = result_ref.clone();
                return result;
            }
        }

        match pinned.try_insert(key.clone(), (Instant::now(), OnceCell::new())) {
            Ok(val_ref) => {
                let result = val_ref.1.get_or_init(init).clone();
                if pinned.len() > self.size {
                    let mut count = 0;
                    // Max size reached, try to evict expired items or random valid item
                    pinned.retain(|k, v| {
                        let valid = v.0.elapsed().as_secs() <= self.seconds;
                        if valid {
                            count += 1;
                            count <= self.size
                        } else {
                            false
                        }
                    });
                }
                result
            }
            Err(e) => {
                let val_ref = e.current;
                if val_ref.0.elapsed().as_secs() <= self.seconds {
                    val_ref.1.get_or_init(init).clone()
                } else {
                    pinned.insert(key.clone(), e.not_inserted);
                    pinned.get(&key).expect("this should not happen").1.get_or_init(init).clone()
                }
            }
        }
    }
}