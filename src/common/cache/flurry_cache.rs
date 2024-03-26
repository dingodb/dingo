use crate::logger::logger_bridge::TantivySearchLogger;
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
            hash_map: HashMap::with_capacity_and_hasher(capacity, DefaultHashBuilder::default()),
        }
    }
}

#[allow(dead_code)]
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
            // Key already exist, return original data.
            Err(e) => {
                let val_ref = e.current;
                let result = val_ref.get_or_init(|| init());
                result.clone()
            }
        }
    }

    /// Returns a vec of all keys currently in the hashmap.
    #[allow(dead_code)]
    pub fn all_keys(&self) -> Vec<K> {
        let pinned = self.hash_map.pin();
        pinned.iter().map(|entry| entry.0.clone()).collect()
    }

    /// Manually clean the hashmap.
    #[allow(dead_code)]
    pub fn clear(&self) {
        let pinned = self.hash_map.pin();
        pinned.clear();
    }

    /// Returns current hashmap size.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        let pinned = self.hash_map.pin();
        pinned.len()
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

    use crate::common::cache::flurry_cache::FlurryCache;

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
            // println!("Data(value:'{}') is being dropped!", self.value);
        }
    }

    impl Clone for TestData {
        fn clone(&self) -> Self {
            TestData::new(self.value.clone())
        }
    }

    #[test]
    fn test_resolve_store() {
        let test_hashmap: FlurryCache<(usize, String, String), RoaringBitmap> =
            FlurryCache::with_capacity(100);
        let test_key = (0, "a".to_string(), "b".to_string());

        // Assert that the value returned by the resolve method matches the original RoaringBitmap inserted.
        let mut origin_bitmap = RoaringBitmap::new();
        origin_bitmap.insert_range(2..4);
        let resolved_bitmap = test_hashmap.resolve(test_key.clone(), || origin_bitmap.clone());
        assert_eq!(resolved_bitmap, origin_bitmap);

        // Assert that after inserting a new value with the same key, the resolve method returns the old inserted value
        let mut newer_bitmap = RoaringBitmap::new();
        newer_bitmap.insert_range(1..10);
        let newer_resolved_bitmap = test_hashmap.resolve(test_key.clone(), || newer_bitmap.clone());
        assert_ne!(newer_resolved_bitmap, newer_bitmap);
        assert_eq!(newer_resolved_bitmap, origin_bitmap);
    }

    #[test]
    fn test_resolve_clear() {
        let capacity = 4;
        let test_hashmap: FlurryCache<(usize, String, String), Arc<RoaringBitmap>> =
            FlurryCache::with_capacity(capacity);

        // When the hashmap reaches its size limit, it will trigger a cleanup process.
        for address in 0..100 {
            let key = (address, "a".to_string(), "b".to_string());
            let value = Arc::new(RoaringBitmap::new());
            let _ = test_hashmap.resolve(key, || value);
            assert_eq!(
                test_hashmap.all_keys().len() % capacity,
                (address + 1) % capacity
            );
        }
    }

    #[test]
    fn test_flurry_cache_drop_value() {
        // These two hashmaps with different capacities are used to
        // test whether the `Arc` data stored in them is properly cleaned up.
        let test_hashmap_capacity_1: FlurryCache<(usize, String, String), Arc<TestData>> =
            FlurryCache::with_capacity(1);
        let test_hashmap_capacity_100: FlurryCache<(usize, String, String), Arc<TestData>> =
            FlurryCache::with_capacity(100);
        let arc_data: Arc<TestData> = Arc::new(TestData::new("test_data"));

        // `arc_data` pointer count should smaller than 20.
        for address in 0..20 {
            let _ = test_hashmap_capacity_1
                .resolve((address, "a".to_string(), "b".to_string()), || {
                    Arc::clone(&arc_data)
                });
        }
        assert!(Arc::strong_count(&arc_data) < 20);

        // `arc_data` pointer count should equal 21 (1+20=21).
        test_hashmap_capacity_1.clear();
        for address in 0..20 {
            let _ = test_hashmap_capacity_100
                .resolve((address, "a".to_string(), "b".to_string()), || {
                    Arc::clone(&arc_data)
                });
        }
        assert_eq!(Arc::strong_count(&arc_data), 21);
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
