use flurry::{DefaultHashBuilder, HashMap};
use once_cell::sync::OnceCell;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;

/// TODO This is a temporary solution for the "skip-index" issue,
/// and this part of the code will need to be removed when rewriting the processor in the future.
/// A cache structure that stores key-value pairs.
/// The values are wrapped in `Arc<V>` for safe cross-thread sharing.
/// `K` is the type of the keys, `V` is the type of the values, and `S` is the hash builder.
pub struct FurryCache<K, V, S = DefaultHashBuilder> {
    capacity: usize,
    hash_map: HashMap<K, OnceCell<V>, S>,
}

impl<K, V> FurryCache<K, Arc<V>, DefaultHashBuilder>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Send + Sync,
{
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            hash_map: HashMap::with_capacity_and_hasher(
                capacity + 100,
                DefaultHashBuilder::default(),
            ),
        }
    }
}

impl<K, V, S> FurryCache<K, Arc<V>, S>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Send + Sync,
    S: BuildHasher + Clone,
{
    /// Resolves the value for a given key.
    /// If the key is not present in the cache, it uses the provided function `init` to create the value.
    /// This method is lock-free.
    pub fn resolve<F: FnOnce() -> Arc<V>>(&self, key: K, init: F) -> Arc<V> {
        // lock-free coding
        let pinned = self.hash_map.pin();

        if let Some(val_ref) = pinned.get(&key) {
            let result_ref = val_ref.get_or_init(|| init());
            return Arc::clone(result_ref);
        }

        // Consider implementing a more sophisticated cache clearing strategy
        if pinned.len() >= self.capacity {
            pinned.clear();
        }

        match pinned.try_insert(key.clone(), OnceCell::new()) {
            // key not exist, and success insert new key-value.
            Ok(inserted_ref) => {
                let result = inserted_ref.get_or_init(|| init());
                Arc::clone(result)
            }
            // Key already exist.
            Err(e) => {
                let val_ref = e.current;
                Arc::clone(val_ref.get_or_init(|| init()))
            }
        }
    }
}
