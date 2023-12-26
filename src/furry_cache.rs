use crate::logger::ffi_logger::callback_with_thread_info;
use flurry::{DefaultHashBuilder, HashMap};
use once_cell::sync::OnceCell;
use std::hash::{BuildHasher, Hash};

use crate::INFO;
use crate::commons::*;

/// TODO This is a temporary solution for the "skip-index" issue,
/// and this part of the code will need to be removed when rewriting the processor in the future.
/// A cache structure that stores key-value pairs.
/// The values are wrapped in `V` for safe cross-thread sharing.
/// `K` is the type of the keys, `V` is the type of the values, and `S` is the hash builder.
pub struct FurryCache<K, V, S = DefaultHashBuilder> {
    capacity: usize,
    hash_map: HashMap<K, OnceCell<V>, S>,
}

impl<K, V> FurryCache<K, V, DefaultHashBuilder>
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

impl<K, V, S> FurryCache<K, V, S>
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
            INFO!(target: LOGGER_TARGET, "furry cache trigger pinned clear.");
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
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use once_cell::sync::Lazy;
    use roaring::RoaringBitmap;

    use crate::furry_cache::FurryCache;
    
    struct CustomStruct {
        data: String, // 或者是您需要的任何数据类型
    }
    impl CustomStruct {
        fn new(data: impl Into<String>) -> Self {
            CustomStruct { data: data.into() }
        }
    }
    
    impl Drop for CustomStruct {
        fn drop(&mut self) {
            println!("CustomStruct with data `{}` is being dropped!", self.data);
        }
    }
    impl Clone for CustomStruct {
        fn clone(&self) -> Self {
            CustomStruct::new(self.data.clone())
        }
    }


    #[test]
    fn it_works() {
        let test_furry_cache: Lazy<FurryCache<(usize, String, String), RoaringBitmap>> = Lazy::new(|| FurryCache::with_capacity(1));

        let row_id_roaring_bitmap = test_furry_cache.resolve(
            (0, "hello".to_string(), "/var/lib/xxx".to_string()),
            || {
                let fake_bitmap = RoaringBitmap::new();
                return fake_bitmap;
            },
        );

        for i_ in 1..10000 {
            let _: RoaringBitmap = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {RoaringBitmap::new()});
            let _: RoaringBitmap = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {RoaringBitmap::new()});
        }

    }

    #[test]
    fn free_value() {
        let test_furry_cache: Lazy<FurryCache<(usize, String, String), CustomStruct>> = Lazy::new(|| FurryCache::with_capacity(1));

        let _ = test_furry_cache.resolve(
            (0, "hello".to_string(), "/var/lib/xxx".to_string()),
            || {
                let fake_data = CustomStruct::new("test");
                return fake_data;
            },
        );

        for i_ in 1..10000 {
            let _: CustomStruct = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {CustomStruct::new("test-2")});
            let _: CustomStruct = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {CustomStruct::new("test-3")});
        }
    }

    #[test]
    fn free_value2() {
        let test_furry_cache: Lazy<FurryCache<(usize, String, String), Arc<CustomStruct>>> = Lazy::new(|| FurryCache::with_capacity(1));

        let _ = test_furry_cache.resolve(
            (0, "hello".to_string(), "/var/lib/xxx".to_string()),
            || {
                let fake_data = CustomStruct::new("test");
                return Arc::new(fake_data);
            },
        );

        for i_ in 1..10000 {
            let _: Arc<CustomStruct> = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {Arc::new(CustomStruct::new("test-2"))});
            let _: Arc<CustomStruct> = test_furry_cache.resolve((i_, "hello".to_string(), "/var/lib/xxx".to_string()),|| {Arc::new(CustomStruct::new("test-3"))});
        }
    }
}
