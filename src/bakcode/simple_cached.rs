use std::hash::{Hash};
use std::sync::{Arc};
use std::time::{Duration};
use once_cell::sync::OnceCell;
use stretto::Cache;

// TODO 性能不够好的话，再考虑修改配置
pub struct SimpleCached<K, V>
{
    capacity: usize,
    cache: Cache<K, OnceCell<V>>,

}
// 针对 SimpleCached<K, V> 的变体 SimpleCached<K, Arc<V>> 实现
impl<K, V> SimpleCached<K, Arc<V>>
where
    K: 'static + Hash + Ord + Clone + Send + Sync + Eq,
    V: 'static + Send + Sync
{
    #[must_use]
    pub fn new(capacity: usize, ttl: u64) -> Self {
        Self {
            capacity,
            ttl,
            cache: Cache::new(capacity+10, (capacity+1024) as i64).unwrap(),
        }
    }

    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity, 600)
    }

    #[must_use]
    pub fn with_capacity_and_ttl(capacity: usize, ttl: u64) -> Self {
        Self::new(capacity, ttl)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()

    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.cache.len()
    }

    #[must_use]
    pub fn clear(&self) {
        self.cache.clear();
    }

    #[must_use]
    pub fn resolve<F: FnOnce() -> V>(&self, key: K, init: F) -> Arc<V> {
        if self.len()>=self.capacity {
            let _ = self.clear();
        }
        match self.cache.get(&key) {
            Some(value_ref) => {
                // let value = value_ref.value().clone();
                // if value.get().is_none() {
                //     let new_val = Arc::new(init());
                //     value.set(new_val);
                //     self.cache.insert_with_ttl(key.clone(), value, 1, Duration::from_secs(self.ttl));
                // }
                Arc::clone(value_ref.value().get_or_init(|| Arc::new(init())))
            }
            None => {
                let new_value = OnceCell::new();
                let arc_value = Arc::new(init());
                let _ = new_value.set(arc_value.clone());
                self.cache.insert_with_ttl(key.clone(), new_value, 1, Duration::from_secs(self.ttl));
                arc_value
            }
        }
    }
}
