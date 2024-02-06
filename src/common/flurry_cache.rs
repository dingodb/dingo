use crate::logger::ffi_logger::callback_with_thread_info;
use flurry::{DefaultHashBuilder, HashMap};
use once_cell::sync::OnceCell;
use std::hash::{BuildHasher, Hash};

use crate::{common::constants::LOG_CALLBACK, INFO};

/// and this part of the code will need to be removed when rewriting the processor in the future.
/// A cache structure that stores key-value pairs.
/// The values are wrapped in `V` for safe cross-thread sharing.
/// `K` is the type of the keys, `V` is the type of the values, and `S` is the hash builder.
pub struct FlurryCache<K, V, S = DefaultHashBuilder> {
    capacity: usize,
    hash_map: HashMap<K, OnceCell<V>, S>,
}

impl<K, V> FlurryCache<K, V, DefaultHashBuilder>
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

impl<K, V, S> FlurryCache<K, V, S>
where
    K: 'static + Hash + Ord + Clone + Send + Sync,
    V: 'static + Send + Sync + Clone,
    S: BuildHasher + Clone,
{
    /// Resolves the value for a given key.
    /// If the key is not present in the cache, it uses the provided function `init` to create the value.
    /// This method is lock-free.
    pub fn resolve<F: FnOnce() -> V>(&self, key: K, init: F) -> V {
        // lock-free coding
        let pinned = self.hash_map.pin();

        if let Some(val_ref) = pinned.get(&key) {
            let result_ref = val_ref.get_or_init(|| init());
            return result_ref.clone();
        }

        // Consider implementing a more sophisticated cache clearing strategy
        if pinned.len() >= self.capacity {
            INFO!("furry cache trigger pinned clear.");
            pinned.clear();
        }

        match pinned.try_insert(key.clone(), OnceCell::new()) {
            // key not exist, and success insert new key-value.
            Ok(inserted_ref) => {
                let result = inserted_ref.get_or_init(|| init());
                result.clone()
            }
            // Key already exist.
            Err(e) => {
                let val_ref = e.current;
                val_ref.get_or_init(|| init()).clone()
            }
        }
    }

    /// Returns a vec of all keys currently in the hashmap.
    #[allow(dead_code)]
    pub fn all_keys(&self) -> Vec<K> {
        let pinned = self.hash_map.pin();
        pinned.iter().map(|entry| entry.0.clone()).collect()
    }

    /// Returns current hashmap size.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let pinned = self.hash_map.pin();
        return pinned.len();
    }

    /// Removes a list of keys from the cache.
    #[allow(dead_code)]
    pub fn remove_keys(&self, keys: Vec<K>) {
        let pinned = self.hash_map.pin();
        for key in keys {
            let _ = pinned.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use roaring::RoaringBitmap;
    use std::sync::Arc;

    use crate::common::flurry_cache::FlurryCache;

    #[derive(Debug, PartialEq)]
    struct TestData {
        value: String,
    }
    impl TestData {
        fn new(value: impl Into<String>) -> Self {
            TestData {
                value: value.into(),
            }
        }
    }

    impl Drop for TestData {
        fn drop(&mut self) {
            println!("Data(value:'{}') is being dropped!", self.value);
        }
    }

    impl Clone for TestData {
        fn clone(&self) -> Self {
            TestData::new(self.value.clone())
        }
    }

    #[test]
    fn test_store_roaring_bitmap() {
        let test_hashmap: FlurryCache<(usize, String, String), RoaringBitmap> =
            FlurryCache::with_capacity(1);
        let mut origin_bitmap = RoaringBitmap::new();
        origin_bitmap.insert_range(2..4);
        assert_eq!(
            test_hashmap.resolve((0, "abc".to_string(), "def".to_string()), || origin_bitmap
                .clone()),
            origin_bitmap.clone()
        );

        for _ in 1..10 {
            let _: RoaringBitmap = test_hashmap
                .resolve((0, "def".to_string(), "hij".to_string()), || {
                    RoaringBitmap::new()
                });
        }
        assert_eq!(
            test_hashmap.resolve((0, "abc".to_string(), "def".to_string()), || {
                RoaringBitmap::new()
            }),
            RoaringBitmap::new()
        );
    }

    #[test]
    fn test_drop_obj() {
        let test_hashmap: FlurryCache<(usize, String, String), TestData> =
            FlurryCache::with_capacity(1);
        let origin_data = TestData::new("");
        assert_eq!(
            test_hashmap.resolve((0, "abc".to_string(), "def".to_string()), || origin_data
                .clone()),
            origin_data.clone()
        );

        for _ in 1..5 {
            let _ = test_hashmap.resolve((0, "def".to_string(), "hij".to_string()), || {
                TestData::new("b")
            });
        }
        assert_eq!(
            test_hashmap.resolve((0, "abc".to_string(), "def".to_string()), || origin_data
                .clone()),
            origin_data.clone()
        );
    }

    #[test]
    fn test_remove_keys() {
        let test_hashmap: FlurryCache<(usize, String), Arc<TestData>> =
            FlurryCache::with_capacity(4);
        let _ = test_hashmap.resolve((0, "000".to_string()), || Arc::new(TestData::new("data-0")));
        let _ = test_hashmap.resolve((1, "001".to_string()), || Arc::new(TestData::new("data-1")));
        assert_eq!(test_hashmap.len(), 2);
        let _ = test_hashmap.resolve((2, "002".to_string()), || Arc::new(TestData::new("data-2")));
        let _ = test_hashmap.resolve((3, "003".to_string()), || Arc::new(TestData::new("data-3")));
        assert_eq!(test_hashmap.len(), 4);
        let _ = test_hashmap.resolve((4, "004".to_string()), || Arc::new(TestData::new("data-4")));
        let _ = test_hashmap.resolve((5, "005".to_string()), || Arc::new(TestData::new("data-5")));
        assert_eq!(test_hashmap.len(), 2);
        let _ = test_hashmap.resolve((6, "000".to_string()), || Arc::new(TestData::new("data-6")));
        let _ = test_hashmap.resolve((7, "000".to_string()), || Arc::new(TestData::new("data-7")));
        assert_eq!(test_hashmap.len(), 4);

        let all_keys = test_hashmap.all_keys();
        let specific_key = "000";
        let keys_need_remove: Vec<_> = all_keys
            .into_iter()
            .filter(|(_, ref element)| element == specific_key)
            .collect();
        test_hashmap.remove_keys(keys_need_remove);
        assert_eq!(test_hashmap.len(), 2);
    }
}
