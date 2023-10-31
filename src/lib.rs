use std::ffi::{CString, CStr}; // 处理外部字符串, 如 C 字符串
use std::mem; // 内存处理
use std::ptr; // 指针相关
use std::slice; // 切片相关(部分数组引用)
use std::iter::FusedIterator; // 迭代器相关
use std::cmp::Ordering; // 提供比较操作的结果枚举

use libc::*; // 导入 C 标准库的绑定

// 导入 tantivy 搜索引擎库的功能
use tantivy::collector::{TopDocs, Count};
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{Index, IndexReader, IndexWriter, SegmentReader, SegmentLocalId, DocId, Score, DocAddress, TantivyError};
use tantivy::ReloadPolicy;

use rayon::prelude::*; // 导入 rayon 库，它提供了并行迭代器的功能
use std::sync::Arc; // 提供原子引用计数的智能指针

mod cache; // 导入当前项目中定义的 cache 模块

// 定义一个静态的缓存，使用 once_cell 库来延迟初始化
static CACHE: once_cell::sync::Lazy<cache::ConcurrentCache<(usize, String, u64, bool), Arc<(Vec<u64>, Vec<u64>)>>> = once_cell::sync::Lazy::new(|| {
    cache::ConcurrentCache::with_capacity(100, 3600, 110)
});

// 是否记录时间
const TIMING: bool = true;

// 宏定义, 用于测量代码块的执行时间
macro_rules! start {
    ($val:ident) => {
        let $val = if TIMING {
            Some(std::time::Instant::now())
        } else {
            None
        };
    };
}

macro_rules! end {
    ($val:ident) => {
        if TIMING {
            let $val = $val.unwrap();
            dbg!($val.elapsed());
        }
    };
    ($val:ident, $ex:expr) => {
        if TIMING {
            let $val = $val.unwrap();
            dbg!($val.elapsed(), $ex);
        }
    };
}






/******* 定义文档、以及文档结果的搜集过程、处理过程 *******/


#[derive(Default)]
pub struct Docs {
    limit: usize
}

impl Docs {
    // 构造函数, 用来创建一个具有特定 limit 值的 Docs 实例
    pub fn with_limit(limit: usize) -> Docs {
        Docs { limit }
    }
}

// 实现 tantivy 库中的 Collector trait （trait 可以理解为 C++ 的纯虚函数或者 java 的 interface）
// 定义如何收集搜索结果的行为
impl Collector for Docs {
    // 定义收集器结果类型, 元组向量, 每个元组包含一个 Score 和一个 DocAddress
    type Fruit = Vec<(Score, DocAddress)>;
    // 定义在每个 Segment 上运行的收集器的类型
    type Child = SegmentDocsCollector;


    // 为每个 segment 索引段创建一个 SegmentDocsCollector 实例
    // 输入参数为 segment id 和 SegmentReader
    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentDocsCollector> {
        Ok(SegmentDocsCollector { docs: vec!(), segment_local_id, limit: self.limit })
    }

    // 是否需要对文档进行评分
    fn requires_scoring(&self) -> bool {
        false
    }

    // 合并不同 segment 索引段的结果
    // 可选是否计算 merge 的时间
    fn merge_fruits(&self, segment_docs: Vec<Vec<(Score, DocAddress)>>) -> tantivy::Result<Vec<(Score, DocAddress)>> {
        // merge 阶段
        start!(merge);
        let lens: Vec<_> = segment_docs.iter().map(|v| v.len()).collect();
        let full_len = lens.iter().sum();

        let mut all = Vec::with_capacity(full_len);
        unsafe { all.set_len(full_len) };

        let mut mut_slice = &mut all[..];
        let mut mut_slices = vec!();
        for len in lens {
            let (slice, rest) = mut_slice.split_at_mut(len);
            mut_slices.push(slice);
            mut_slice = rest;
        }

        segment_docs.into_par_iter().zip(mut_slices.into_par_iter()).for_each(|(vec, slice)| {
            slice.copy_from_slice(&vec[..]);
        });
        end!(merge);

        // 结果过多, 进行 resize
        start!(resize);
        if all.len() > self.limit {
            all.resize(self.limit, (0.0f32, DocAddress(0, 0)));
        }
        end!(resize);

        Ok(all)
    }
}







/******* 关于可排序文档定义 *******/

// 属性宏, 自动为 OrdDoc 结构体派生以下 trait(Clone、Copy 允许结构体实例在赋值时按位复制、Debug)
#[derive(Clone, Copy, Debug)]
pub struct OrdDoc(Score, DocAddress);

// 实现 OrdDoc 的标准库的 trait Ord
impl Ord for OrdDoc {
    // 尝试比较 score, 若相同/无法比较则继续使用 DocAddress 进行比较
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(self.1.cmp(&other.1))
    }
}

// 为 OrdDoc 实现 PartialOrd trait
impl PartialOrd for OrdDoc {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// 为 OrdDoc 实现 PartialEq trait
// 用来比较 OrdDoc 是否相等
impl PartialEq for OrdDoc {
    fn eq(&self, other: &Self) -> bool {
        self.1 == other.1
    }
}

impl Eq for OrdDoc {}



/******* 定义可排序文档结果的搜集过程、处理过程 *******/

// 与之前的 Docs 结构体相比，RankedDocs 结构体有以下不同之处：
// RankedDocs 使用 OrdDoc 而不是 (Score, DocAddress) 来存储和操作搜索结果。
// OrdDoc 是一个结构体，它实现了排序相关的 trait，这意味着 RankedDocs 可以直接对结果进行排序。
// requires_scoring 默认是 true
#[derive(Default)]
pub struct RankedDocs {
    limit: usize
}

impl RankedDocs {
    pub fn with_limit(limit: usize) -> RankedDocs {
        RankedDocs { limit }
    }
}

impl Collector for RankedDocs {
    type Fruit = Vec<OrdDoc>;

    type Child = SegmentOrdDocsCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentLocalId,
        _: &SegmentReader,
    ) -> tantivy::Result<SegmentOrdDocsCollector> {
        Ok(SegmentOrdDocsCollector { docs: vec!(), segment_local_id, limit: self.limit })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_docs: Vec<Vec<OrdDoc>>) -> tantivy::Result<Vec<OrdDoc>> {
        start!(merge);
        let lens: Vec<_> = segment_docs.iter().map(|v| v.len()).collect();
        let full_len = lens.iter().sum();

        let mut all = Vec::with_capacity(full_len);
        unsafe { all.set_len(full_len) };

        let mut mut_slice = &mut all[..];
        let mut mut_slices = vec!();
        for len in lens {
            let (slice, rest) = mut_slice.split_at_mut(len);
            mut_slices.push(slice);
            mut_slice = rest;
        }

        segment_docs.into_par_iter().zip(mut_slices.into_par_iter()).for_each(|(vec, slice)| {
            slice.copy_from_slice(&vec[..]);
        });
        end!(merge);

        start!(sort);
        all.par_sort();
        end!(sort);

        start!(resize);
        if all.len() > self.limit {
            all.resize(self.limit, OrdDoc(0.0f32, DocAddress(0, 0)));
        }
        end!(resize);

        Ok(all)
    }
}





/******* 实现用于收集单个 segment 索引段文件的接口 *******/


#[derive(Default)]
pub struct SegmentOrdDocsCollector {
    docs: Vec<OrdDoc>,  // 存储收集到的文档和它们的评分
    segment_local_id: SegmentLocalId,  // 索引段的本地 id
    limit: usize  // 可收集最大文档的数量
}
// SegmentCollector trait 是 Tantivy 搜索库中用于收集单个索引段（segment）中的文档的接口
impl SegmentCollector for SegmentOrdDocsCollector {
    type Fruit = Vec<OrdDoc>;

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push(OrdDoc(score, DocAddress(self.segment_local_id, doc_id)));
        }
    }

    // 返回收集到的所有文档
    fn harvest(self) -> Vec<OrdDoc> {
        self.docs
    }
}






/******* 实现用于收集单个 segment 索引段带有评分文档的接口 *******/

#[derive(Default)]
pub struct SegmentDocsCollector {
    docs: Vec<(Score, DocAddress)>,
    segment_local_id: SegmentLocalId,
    limit: usize
}

impl SegmentCollector for SegmentDocsCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    // 达到 limit 时会舍弃
    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push((score, DocAddress(self.segment_local_id, doc_id)));
        }
    }

    // 返回所有收集到的文档
    fn harvest(self) -> Vec<(Score, DocAddress)> {
        self.docs
    }
}


// 将一个字节向量转换成为一个原始指针
fn leak_buf(v: Vec<u8>, vallen: *mut size_t) -> *mut c_char {
    // 解引用 vallen 指针, 并将它指向的值设置为 v 的长度
    unsafe {
        *vallen = v.len();
    }
    // 将 v 转换成为装箱的切片，即一个堆分配的数组（类似于向量），有固定的大小
    let mut bsv = v.into_boxed_slice();
    // 获得 bsv 可变原始指针
    let val = bsv.as_mut_ptr() as *mut _;
    // 阻止 rust 在离开函数作用域的时候释放 bsv 的内存，因为 bsv 接下来会被 c 接管
    mem::forget(bsv);
    val
}

// #[no_mangle]
// pub unsafe extern "C" fn tantivy_free_buf(buf: *mut c_char, sz: size_t) {
//     drop(Vec::from_raw_parts(buf, sz, sz));
// }

// IterWrapper 可以被克隆
#[derive(Clone)]
pub struct IterWrapper {
    inner: Arc<(Vec<u64>, Vec<u64>)>, // Arc 是智能指针, 允许多个所有者共享不可变的数据, 线程安全
    offset: usize // 用于跟踪迭代当前位置
}

// From trait 的实现
// 允许 <Arc<(Vec<u64>, Vec<u64>)>> 类型转换为 IterWrapper 类型
impl From<Arc<(Vec<u64>, Vec<u64>)>> for IterWrapper {
    fn from(inner: Arc<(Vec<u64>, Vec<u64>)>) -> IterWrapper {
        IterWrapper { inner, offset: 0 }
    }
}
// Iterator trait 的实现
// IterWrapper 可以作为迭代器使用
impl Iterator for IterWrapper {
    type Item = (u64, u64);

    // 返回迭代器中下一个元素
    #[inline]
    fn next(&mut self) -> Option<(u64, u64)> {
        if self.offset >= self.inner.0.len() {
            None
        } else {
            let result = Some((self.inner.0[self.offset], self.inner.1[self.offset]));
            self.offset += 1;
            result
        }
    }

    // 返回一个包含迭代器剩余元素数量的估计值
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.inner.0.len() - self.offset;
        (size, Some(size))
    }

    // 计算迭代器内剩余元素的数量
    #[inline]
    fn count(self) -> usize {
        self.inner.0.len() - self.offset
    }
}

// 表示迭代器在返回 None 之后将永远返回 None
impl FusedIterator for IterWrapper {}


#[derive(Clone)]
pub struct VecIterWrapper {
    iter: std::vec::IntoIter<(u64, u64)>
}

impl From<std::vec::IntoIter<(u64, u64)>> for VecIterWrapper {
    fn from(iter: std::vec::IntoIter<(u64, u64)>) -> VecIterWrapper {
        VecIterWrapper { iter }
    }
}

impl Iterator for VecIterWrapper {
    type Item = (u64, u64);

    #[inline]
    fn next(&mut self) -> Option<(u64, u64)> {
        self.iter.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }

    #[inline]
    fn count(self) -> usize {
        self.iter.count()
    }
}

impl DoubleEndedIterator for VecIterWrapper {
    #[inline]
    fn next_back(&mut self) -> Option<(u64, u64)> {
        self.iter.next_back()
    }
}

impl FusedIterator for VecIterWrapper {}

pub struct IndexRW {
    pub path: String,
    pub index: Index,
    pub reader: IndexReader,
    pub writer: IndexWriter
}

#[no_mangle]
pub extern "C" fn tantivysearch_open_or_create_index(dir_ptr: *const c_char) -> *mut IndexRW {
    let dir_c_str = unsafe {
        assert!(!dir_ptr.is_null());

        CStr::from_ptr(dir_ptr)
    };

    let dir_str = dir_c_str.to_str().expect("failed to get &str from cstr");

    println!("Opening index on {}", dir_str);
    let mut index = match Index::open_in_dir(dir_str) {
        Ok(index) => index,
        Err(e) => {
            match e {
                TantivyError::PathDoesNotExist(_) => {
                    println!("Creating index on {}", dir_str);
                    std::fs::create_dir_all(dir_str).expect("failed to create index dir");
                    let mut schema_builder = Schema::builder();
                    schema_builder.add_u64_field("primary_id", FAST);
                    schema_builder.add_u64_field("secondary_id", FAST);
                    schema_builder.add_text_field("body", TEXT);
                    let schema = schema_builder.build();
                    Index::create_in_dir(dir_str, schema).expect("failed to create index")
                }
                _ => {
                    panic!("this should not happen");
                }
            }
        }
    };

    index.set_default_multithread_executor().expect("failed to create thread pool");
    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into().expect("failed to create reader");
    let writer = index
        .writer(1024 * 1024 * 1024)
        .expect("failed to create writer");

    // let mut policy = tantivy::merge_policy::LogMergePolicy::default();
    // policy.set_max_merge_size(3_000_000);

    // writer.set_merge_policy(Box::new(policy));

    Box::into_raw(Box::new(IndexRW { index, reader, writer, path: dir_str.to_string() }))
}

pub fn tantivysearch_search_impl(irw: *mut IndexRW, query_str: &str, limit: u64) -> Arc<(Vec<u64>, Vec<u64>)> {
    CACHE.resolve((irw as usize, query_str.to_string(), limit, false), move || {
        println!("Searching index for {} with limit {}", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*irw).index.schema() };

        let body = schema.get_field("body").expect("missing field body");
        let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
        let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

        let searcher = unsafe { (*irw).reader.searcher() };
        let segment_readers = searcher.segment_readers();
        let ff_readers_primary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(primary_id).unwrap()
        }).collect();
        let ff_readers_secondary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(secondary_id).unwrap()
        }).collect();


        let query_parser = QueryParser::for_index(unsafe { &(*irw).index }, vec![body]);

        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        let docs = searcher.search(&query, &Docs::with_limit(limit as usize)).expect("failed to search");
        let mut results: (Vec<_>, Vec<_>) = docs.into_par_iter().map(|(_score, doc_address)| {
            let ff_reader_primary = &ff_readers_primary[doc_address.segment_ord() as usize];
            let ff_reader_secondary = &ff_readers_secondary[doc_address.segment_ord() as usize];
            let primary_id: u64 = ff_reader_primary.get(doc_address.doc());
            let secondary_id: u64 = ff_reader_secondary.get(doc_address.doc());
            (primary_id, secondary_id)
        }).unzip();

        dbg!(search.elapsed());
        Arc::new(results)
    })
}

pub fn tantivysearch_ranked_search_impl(irw: *mut IndexRW, query_str: &str, limit: u64) -> Arc<(Vec<u64>, Vec<u64>)> {
    CACHE.resolve((irw as usize, query_str.to_string(), limit, true), move || {
        println!("Searching index for {} with limit {} and ranking", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*irw).index.schema() };

        let body = schema.get_field("body").expect("missing field body");
        let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
        let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

        let searcher = unsafe { (*irw).reader.searcher() };
        let segment_readers = searcher.segment_readers();
        let ff_readers_primary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(primary_id).unwrap()
        }).collect();
        let ff_readers_secondary: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64(secondary_id).unwrap()
        }).collect();


        let query_parser = QueryParser::for_index(unsafe { &(*irw).index }, vec![body]);

        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        let docs = searcher.search(&query, &RankedDocs::with_limit(limit as usize)).expect("failed to search");
        let mut results: (Vec<_>, Vec<_>) = docs.into_par_iter().map(|OrdDoc(_score, doc_address)| {
            let ff_reader_primary = &ff_readers_primary[doc_address.segment_ord() as usize];
            let ff_reader_secondary = &ff_readers_secondary[doc_address.segment_ord() as usize];
            let primary_id: u64 = ff_reader_primary.get(doc_address.doc());
            let secondary_id: u64 = ff_reader_secondary.get(doc_address.doc());
            (primary_id, secondary_id)
        }).unzip();

        dbg!(search.elapsed());
        Arc::new(results)
    })
}

#[no_mangle]
pub extern "C" fn tantivysearch_search(irw: *mut IndexRW, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!irw.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_search_impl(irw, query_str, limit);

    println!("Search results: {}", results.0.len());

    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_ranked_search(irw: *mut IndexRW, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!irw.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_ranked_search_impl(irw, query_str, limit);

    println!("Search results: {}", results.0.len());

    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_index(irw: *mut IndexRW, primary_ids: *const u64, secondary_ids: *const u64, chars: *const c_char, offsets: *const u64, size: size_t) -> c_uchar {
    assert!(!irw.is_null());
    assert!(!primary_ids.is_null());
    assert!(!secondary_ids.is_null());
    assert!(!offsets.is_null());
    assert!(!chars.is_null());
    if size == 0 {
        return 1;
    }
    let primary_slice = unsafe { slice::from_raw_parts(primary_ids, size) };
    let secondary_slice = unsafe { slice::from_raw_parts(secondary_ids, size) };
    let offsets_slice = unsafe { slice::from_raw_parts(offsets, size) };
    let chars_len: usize = (*offsets_slice.iter().last().unwrap()) as usize;
    let chars_slice = unsafe { slice::from_raw_parts(chars as *const u8, chars_len) };
    let mut strs = Vec::with_capacity(size);
    let mut current_start = 0;
    for i in 0..size {
        let end: usize = (offsets_slice[i] as usize - 1);
        strs.push(unsafe { std::str::from_utf8_unchecked(&chars_slice[current_start..end]) });
        current_start = end + 1;
    }

    let schema = unsafe { (*irw).index.schema() };

    let body = schema.get_field("body").expect("missing field body");
    let primary_id = schema.get_field("primary_id").expect("missing field primary_id");
    let secondary_id = schema.get_field("secondary_id").expect("missing field secondary_id");

    for i in 0..size {
        let mut doc = Document::default();
        doc.add_u64(primary_id, primary_slice[i]);
        doc.add_u64(secondary_id, secondary_slice[i]);
        doc.add_text(body, strs[i]);
        unsafe { (*irw).writer.add_document(doc) };
    }

    1
}

#[no_mangle]
pub extern "C" fn tantivysearch_writer_commit(irw: *mut IndexRW) -> c_uchar {
    assert!(!irw.is_null());
    match unsafe { (*irw).writer.commit() } {
        Ok(_) => 1,
        Err(e) => {
            eprintln!("Failed to commit writer: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_truncate(irw: *mut IndexRW) -> c_uchar {
    assert!(!irw.is_null());
    match unsafe { (*irw).writer.delete_all_documents() } {
        Ok(_) =>  {
            match unsafe { (*irw).writer.commit() } {
                Ok(_) => 1,
                Err(e) => {
                    eprintln!("Failed to commit writer: {}", e);
                    0
                }
            }
        },
        Err(e) => {
            eprintln!("Failed to delete all documents: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_next(iter_ptr: *mut IterWrapper, primary_id_ptr: *mut u64, secondary_id_ptr: *mut u64) -> c_uchar {
    assert!(!iter_ptr.is_null());
    match unsafe { (*iter_ptr).next() } {
        Some((primary_id, secondary_id)) => {
            unsafe {
                *primary_id_ptr = primary_id;
                *secondary_id_ptr = secondary_id;
            }
            1
        }
        None => 0
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_batch(iter_ptr: *mut IterWrapper, count: u64, primary_ids_ptr: *mut u64, secondary_ids_ptr: *mut u64) -> size_t {
    assert!(!iter_ptr.is_null());
    if primary_ids_ptr.is_null() {
        return 0;
    }

    let iter_size = unsafe { (*iter_ptr).inner.0.len() - (*iter_ptr).offset };
    let n_to_write = std::cmp::min(count as usize, iter_size);

    unsafe {
        let src_ptr = (*iter_ptr).inner.0.as_ptr().offset((*iter_ptr).offset as isize);
        std::ptr::copy_nonoverlapping(src_ptr, primary_ids_ptr, n_to_write);
    }

    if !secondary_ids_ptr.is_null() {
        unsafe {
            let src_ptr = (*iter_ptr).inner.1.as_ptr().offset((*iter_ptr).offset as isize);
            std::ptr::copy_nonoverlapping(src_ptr, secondary_ids_ptr, n_to_write);
        }
    }

    unsafe { (*iter_ptr).offset += n_to_write };

    n_to_write
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_count(iter_ptr: *mut IterWrapper) -> size_t {
    assert!(!iter_ptr.is_null());
    unsafe { (*iter_ptr).inner.0.len() - (*iter_ptr).offset }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_free(iter_ptr: *mut IterWrapper) {
    assert!(!iter_ptr.is_null());
    drop(unsafe { Box::from_raw(iter_ptr) });
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_free(irw: *mut IndexRW) {
    assert!(!irw.is_null());
    drop(unsafe { Box::from_raw(irw) });
}

#[no_mangle]
pub extern "C" fn tantivysearch_index_delete(irw: *mut IndexRW) {
    assert!(!irw.is_null());
    let path = unsafe { (*irw).path.clone() };
    std::fs::remove_dir_all(path).expect("failed to delete index");
    println!("removed dir");
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}