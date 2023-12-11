#[no_mangle]
pub extern "C" fn tantivy_load_or_create_index(dir_ptr: *const c_char) -> *mut IndexRW {
    let dir_c_str = unsafe {
        assert!(!dir_ptr.is_null());
        CStr::from_ptr(dir_ptr)
    };

    let dir_str = dir_c_str.to_str().expect("failed to get &str from cstr");

    println!("Loading tantivy index on {}", dir_str);
    let mut index = match Index::open_in_dir(dir_str) {
        Ok(index) => index,
        Err(e) => {
            match e {
                // 匹配 OpenDirectoryError 枚举变体
                TantivyError::OpenDirectoryError(OpenDirectoryError::DoesNotExist(_)) => {
                    // TantivyError::OpenDirectoryError(OpenDirectoryError::DoesNotExist(_)) |
                    // TantivyError::OpenReadError(ref err) if err.to_string().contains("meta.json") => {
                    println!("Creating tantivy index on path {}", dir_str);
                    std::fs::create_dir_all(dir_str).expect("failed to create index dir");
                    let mut schema_builder = Schema::builder();
                    schema_builder.add_u64_field("row_id", INDEXED);
                    schema_builder.add_text_field("text", TEXT);
                    let schema = schema_builder.build();
                    Index::create_in_dir(dir_str, schema).expect("Failed to create tantivy index")
                }
                // 处理 IO 错误
                TantivyError::IoError(io_error) => {
                    panic!("IO error occurred: {:?}", io_error);
                }
                // 处理其它所有类型的错误
                _ => {
                    panic!("An error occurred: {:?}", e);
                }
            }
        }
    };

    // 设置 search 的线程数
    index
        .set_default_multithread_executor()
        .expect("Failed to create thread pool");

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()
        .expect("Failed to create tantivy reader");

    let writer = index
        .writer(1024 * 1024 * 1024)
        .expect("Failed to create tantivy writer");

    // TODO 测试不同 LogMergePolicy 参数下的性能
    let policy = tantivy::merge_policy::LogMergePolicy::default();
    // policy.set_max_merge_size(3_000_000);

    writer.set_merge_policy(Box::new(policy));

    // 将 Box 类型的智能指针转换成为裸指针, 不会对它指向的内存进行析构
    Box::into_raw(Box::new(IndexRW {
        index,
        reader,
        writer,
        path: dir_str.to_string(),
    }))
}


// 获得搜索到的 row_id
pub fn tantivysearch_search_impl(ir: *mut IndexR, query_str: &str, limit: u64) -> Arc<Vec<u64>> {
    // 参数 init 是一个闭包 closure, 作用是提供一个值的初始化逻辑
    CACHE.resolve((ir as usize, query_str.to_string(), limit, false), move || {
        println!("Searching index for {} with limit {}", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*ir).index.schema() };

        let text = schema.get_field("text").expect("missing field text");

        let searcher = unsafe { (*ir).reader.searcher() };
        let segment_readers = searcher.segment_readers();

        // 获得所有 SegmentReader 对应的 Column
        let row_id_columns: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64("row_id").unwrap()
        }).collect();

        // 创建 QueryParser, text 列被用来进行 search
        let query_parser = QueryParser::for_index(unsafe { &(*ir).index }, vec![text]);
        // 解析 query
        let query = query_parser.parse_query(query_str).expect("tantivy failed to parse query");
        // 搜索 TopK 的 docs
        let top_docs = searcher.search(&query, &Docs::with_limit(limit as usize)).expect("failed to search");

        // print!("top docs size is {}", top_docs.len());

        let mut results: Vec<u64> = top_docs.into_par_iter().map(|(_score, doc_address)| {
            // 确定 primary 和 secondary 所在的 Column
            let row_id_column = &row_id_columns[doc_address.segment_ord as usize];

            // 从 Column 内拿到存储的数据
            // TODO 后续需要验证这里使用的 unwrap_or 的正确性, 是否在 Column 内无法找到的时候应该直接报错?
            let row_id: u64 = row_id_column.values_for_doc(doc_address.doc_id).next().unwrap_or(0);
            row_id
        }).collect();

        dbg!(search.elapsed());
        // 返回值
        results
    })
}

// 返回值包含两个 u64 向量, 可能代表主键和次键
// TODO  refactor
pub fn tantivysearch_ranked_search_impl(ir: *mut IndexR, query_str: &str, limit: u64) -> Arc<Vec<u64>> {
    CACHE.resolve((ir as usize, query_str.to_string(), limit, true), move || {
        println!("Searching index for {} with limit {} and ranking", query_str, limit);
        let search = std::time::Instant::now();

        let schema = unsafe { (*ir).index.schema() };

        let text = schema.get_field("text").expect("missing field text");
        let row_id = schema.get_field("row_id").expect("missing field primary_id");

        let searcher = unsafe { (*ir).reader.searcher() };
        let segment_readers = searcher.segment_readers();

        // 获得所有 SegmentReader 对应的 Column
        let row_id_columns: Vec<_> = segment_readers.iter().map(|seg_r| {
            let ffs = seg_r.fast_fields();
            ffs.u64("row_id").unwrap()
        }).collect();
        // 创建 QueryParser, text 列被用来进行 search
        let query_parser = QueryParser::for_index(unsafe { &(*ir).index }, vec![text]);
        // 解析 query
        let query = query_parser.parse_query(query_str).expect("failed to parse query");
        // 搜索 TopK 的 order docs
        let docs = searcher.search(&query, &RankedDocs::with_limit(limit as usize)).expect("failed to search");
        let mut results: Vec<u64> = docs.into_par_iter().map(|OrdDoc(_score, doc_address)| {
            let row_id_column = &row_id_columns[doc_address.segment_ord as usize];
            
            // 从 Column 内拿到存储的数据
            // TODO 后续需要验证这里使用的 unwrap_or 的正确性, 是否在 Column 内无法找到的时候应该直接报错?
            let row_id: u64 = row_id_column.values_for_doc(doc_address.doc_id).next().unwrap_or(0);
            row_id
        }).collect();

        dbg!(search.elapsed());
        results
    })
}

#[no_mangle]
pub extern "C" fn tantivysearch_search(ir: *mut IndexR, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!ir.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_search_impl(ir, query_str, limit);

    println!("Search results: {}", results.len());

    Box::into_raw(Box::new(results.into()))
}

#[no_mangle]
pub extern "C" fn tantivysearch_ranked_search(ir: *mut IndexR, query_ptr: *const c_char, limit: u64) -> *mut IterWrapper {
    assert!(!ir.is_null());

    let query_c_str = unsafe {
        assert!(!query_ptr.is_null());

        CStr::from_ptr(query_ptr)
    };

    let query_str = query_c_str.to_str().expect("failed to get &str from cstr");

    let results = tantivysearch_ranked_search_impl(ir, query_str, limit);

    println!("Search results: {}", results.len());

    Box::into_raw(Box::new(results.into()))
}

// 该属性告诉 Rust 编译器不要改变这个函数的名称
// C 的 char 类型只是一个普通整数, 所以 Rust 的 c_char 实际上是 i8 / u8
#[no_mangle]
pub extern "C" fn tantivysearch_index(irw: *mut IndexRW, row_ids: *const u64, chars: *const c_char, offsets: *const u64, size: size_t) -> c_uchar {
    assert!(!irw.is_null());
    assert!(!row_ids.is_null());
    assert!(!offsets.is_null());
    assert!(!chars.is_null());
    // size 用来表示数组长度
    if size == 0 {
        return 1;
    }

    // 获得 primary_ids 数组引用
    let row_id_slice = unsafe { slice::from_raw_parts(row_ids, size) };
    println!("row_ids: {:?}", row_id_slice);

    // 获得 offsets_slice 数组引用
    let offsets_slice = unsafe { slice::from_raw_parts(offsets, size) };
    println!("offsets_slice: {:?}", offsets_slice);

    // 字符数组中最后一个字符的结束位置
    let chars_len: usize = (*offsets_slice.iter().last().unwrap()) as usize;
    println!("chars_len: {:?}", chars_len);

    // 创建字符数组
    let chars_slice = unsafe { slice::from_raw_parts(chars as *const u8, chars_len) };
    println!("chars_slice: {:?}", chars_slice);

    let mut strs = Vec::with_capacity(size);
    let mut current_start = 0;
    for i in 0..size {
        // usize - 1 表示 offsets 是半开放的, 左闭右开
        let end: usize = (offsets_slice[i] as usize - 1);
        let temp_str = unsafe { std::str::from_utf8_unchecked(&chars_slice[current_start..end]) };
        strs.push(temp_str);
    
        println!("push temp_str: {}", temp_str);

        current_start = end + 1;
    }

    let schema = unsafe { (*irw).index.schema() };

    let text = schema.get_field("text").expect("missing field text");
    let row_id = schema.get_field("row_id").expect("missing field row_id");

    for i in 0..size {
        let mut doc = Document::default();
        doc.add_u64(row_id, row_id_slice[i]);
        doc.add_text(text, strs[i]);
        unsafe { (*irw).writer.add_document(doc) };
    }

    1
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
pub extern "C" fn tantivysearch_iter_next(iter_ptr: *mut IterWrapper, row_id_ptr: *mut u64) -> c_uchar {
    assert!(!iter_ptr.is_null());
    match unsafe { (*iter_ptr).next() } {
        Some(row_id) => {
            unsafe {
                *row_id_ptr = row_id;
            }
            1
        }
        None => 0
    }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_batch(iter_ptr: *mut IterWrapper, count: u64, row_ids_ptr: *mut u64) -> size_t {
    assert!(!iter_ptr.is_null());
    if row_ids_ptr.is_null() {
        return 0;
    }

    let iter_size = unsafe { (*iter_ptr).inner.len() - (*iter_ptr).offset };
    let n_to_write = std::cmp::min(count as usize, iter_size);

    unsafe {
        let src_ptr = (*iter_ptr).inner.as_ptr().offset((*iter_ptr).offset as isize);
        std::ptr::copy_nonoverlapping(src_ptr, row_ids_ptr, n_to_write);
    }

    unsafe { (*iter_ptr).offset += n_to_write };

    n_to_write
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_count(iter_ptr: *mut IterWrapper) -> size_t {
    assert!(!iter_ptr.is_null());
    unsafe { (*iter_ptr).inner.len() - (*iter_ptr).offset }
}

#[no_mangle]
pub extern "C" fn tantivysearch_iter_free(iter_ptr: *mut IterWrapper) {
    assert!(!iter_ptr.is_null());
    drop(unsafe { Box::from_raw(iter_ptr) });
}


    // let raw_text_options = TextOptions::default()
    //     .set_indexing_options(
    //         TextFieldIndexing::default()
    //             .set_index_option(IndexRecordOption::Basic)
    //             .set_tokenizer("raw"),
    //     );

#[no_mangle]
pub extern "C" fn tantivy_nums_docs(ir: *mut IndexR) -> c_uint {
    let searcher = unsafe { (*ir).reader.searcher() };
    let count = searcher.num_docs();
    // TODO 文档的数量非常多的时候，不建议使用 u32 进行转换
    count as c_uint
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