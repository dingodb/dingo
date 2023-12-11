
use std::ffi::{CString, CStr}; // 处理外部字符串, 如 C 字符串
use std::mem; // 内存处理
use std::ptr; // 指针相关
use std::slice; // 切片相关(部分数组引用)
use std::iter::FusedIterator; // 迭代器相关
use std::cmp::Ordering; // 提供比较操作的结果枚举
use serde::{Serialize, Deserialize};

use libc::*; // 导入 C 标准库的绑定

use once_cell::sync::Lazy;
use roaring::RoaringBitmap;
// 导入 tantivy 搜索引擎库的功能
use tantivy::collector::{TopDocs, Count};
use tantivy::directory::error::OpenDirectoryError;
use tantivy::query::QueryParser;
use tantivy::schema::*;
use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{Index, IndexReader, IndexWriter, SegmentReader, SegmentOrdinal, DocId, Score, DocAddress, TantivyError};
use tantivy::ReloadPolicy;

use rayon::prelude::*; // 导入 rayon 库，它提供了并行迭代器的功能
use std::sync::Arc; // 提供原子引用计数的智能指针

use crate::furry_cache::FurryCache;
use crate::{start};
use crate::end;
use crate::macros::TIMING;

// use furry_cache;

// pub use create::macros::*;

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
        segment_local_id: SegmentOrdinal,
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
        // TODO 关掉了关于时间的记录
        // start!(merge);
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
        // end!(merge);

        // 结果过多, 进行 resize
        // start!(resize);
        if all.len() > self.limit {
            all.resize(self.limit, (0.0f32, DocAddress::new(0, 0)));
        }
        // end!(resize);

        Ok(all)
    }
}







/******* 关于可排序文档定义 *******/

// 属性宏, 自动为 OrdDoc 结构体派生以下 trait(Clone、Copy 允许结构体实例在赋值时按位复制、Debug)
#[derive(Clone, Copy, Debug)]
pub struct OrdDoc(pub Score, pub DocAddress);

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
        segment_local_id: SegmentOrdinal,
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
            all.resize(self.limit, OrdDoc(0.0f32, DocAddress::new(0, 0)));
        }
        end!(resize);

        Ok(all)
    }
}





/******* 实现用于收集单个 segment 索引段文件的接口 *******/


#[derive(Default)]
pub struct SegmentOrdDocsCollector {
    docs: Vec<OrdDoc>,  // 存储收集到的文档和它们的评分
    segment_local_id: SegmentOrdinal,  // 索引段的本地 id
    limit: usize  // 可收集最大文档的数量
}
// SegmentCollector trait 是 Tantivy 搜索库中用于收集单个索引段（segment）中的文档的接口
impl SegmentCollector for SegmentOrdDocsCollector {
    type Fruit = Vec<OrdDoc>;

    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push(OrdDoc(score, DocAddress::new(self.segment_local_id, doc_id)));
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
    segment_local_id: SegmentOrdinal,
    limit: usize
}

impl SegmentCollector for SegmentDocsCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    // 达到 limit 时会舍弃
    #[inline]
    fn collect(&mut self, doc_id: DocId, score: Score) {
        if self.docs.len() < self.limit {
            self.docs.push((score, DocAddress::new(self.segment_local_id, doc_id)));
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


// IterWrapper 可以被克隆
#[derive(Clone)]
pub struct IterWrapper {
    pub inner: Arc<Vec<u64>>, // Arc 是智能指针, 允许多个所有者共享不可变的数据, 线程安全
    pub offset: usize // 用于跟踪迭代当前位置
}

// From trait 的实现
// 允许 <Arc<(Vec<u64>, Vec<u64>)>> 类型转换为 IterWrapper 类型
impl From<Arc<Vec<u64>>> for IterWrapper {
    fn from(inner: Arc<Vec<u64>>) -> IterWrapper {
        IterWrapper { inner, offset: 0 }
    }
}
// Iterator trait 的实现
// IterWrapper 可以作为迭代器使用
impl Iterator for IterWrapper {
    type Item = u64;

    // 返回迭代器中下一个元素
    #[inline]
    fn next(&mut self) -> Option<u64> {
        if self.offset >= self.inner.len() {
            None
        } else {
            let result = Some(self.inner[self.offset]);
            self.offset += 1;
            result
        }
    }

    // 返回一个包含迭代器剩余元素数量的估计值
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.inner.len() - self.offset;
        (size, Some(size))
    }

    // 计算迭代器内剩余元素的数量
    #[inline]
    fn count(self) -> usize {
        self.inner.len() - self.offset
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