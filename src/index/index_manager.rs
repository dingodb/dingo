use std::sync::Mutex;
use std::{path::Path, sync::Arc};

use cxx::CxxString;
use cxx::{let_cxx_string, CxxVector};
use tantivy::schema::Schema;
use tantivy::schema::TextFieldIndexing;
use tantivy::schema::TextOptions;
use tantivy::schema::FAST;
use tantivy::schema::INDEXED;
use tantivy::schema::{IndexRecordOption, STORED};

use crate::commons::LOG_CALLBACK;
use crate::logger::ffi_logger::callback_with_thread_info;
use crate::search::index_r::*;
use crate::search::index_searcher::tantivy_reader_free;
use crate::tokenizer::parse_and_register::get_custom_tokenizer;
use crate::tokenizer::parse_and_register::register_tokenizer_to_index;
use crate::{ERROR, INFO, WARNING};

use super::index_w::*;
use crate::common::index_utils::*;

use tantivy::{Document, Index, Term};

/// Creates an index using a specified tokenizer (e.g., Chinese, English, Japanese, etc.).
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `tokenizer_with_parameter`: A str contains tokenizer name and parameters.
/// - `doc_store`: Whether store origin document.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_create_index_with_tokenizer(
    index_path: &CxxString,
    tokenizer_with_parameter: &CxxString,
    doc_store: bool,
) -> Result<bool, String> {
    // parse parameter
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter index_path: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };
    let tokenizer_with_parameter_str = match tokenizer_with_parameter.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter tokenizer_with_parameter: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };

    // Try free index reader before prepare index directory.
    if let Err(e) = tantivy_reader_free(index_path) {
        let error_msg = format!(
            "Can't pre free index reader before prepare index directory, due to: {}",
            e
        );
        ERROR!("{}", error_msg);
        return Err(error_msg);
    }

    // Try free index writer before prepare index directory.
    if let Err(e) = tantivy_writer_free(index_path) {
        let error_msg = format!(
            "Can't pre free index reader before prepare index directory, due to: {}",
            e
        );
        ERROR!("{}", error_msg);
        return Err(error_msg);
    }

    // Prepare the index directory for use.
    let index_files_directory: &Path = Path::new(&index_path_str);

    prepare_index_directory(index_files_directory)?;

    // Save custom index settings.
    let custom_index_setting = CustomIndexSetting {
        tokenizer: tokenizer_with_parameter.to_string(),
    };

    save_custom_index_setting(index_files_directory, &custom_index_setting)?;

    // Get and register the tokenizer for the specified tokenizer.
    let (tokenizer_type, text_analyzer) = match get_custom_tokenizer(&tokenizer_with_parameter_str)
    {
        Ok((tokenizer_type, text_analyzer)) => (tokenizer_type, text_analyzer),
        Err(e) => {
            let error_info = format!("Can't initialize tokenizer: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Initialize TextOptions for indexing documents.
    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer(tokenizer_type.name())
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );

    // Construct the schema for the index.
    let mut schema_builder = Schema::builder();
    schema_builder.add_u64_field("row_id", FAST | INDEXED);
    if doc_store {
        schema_builder.add_text_field("text", text_options | STORED);
    } else {
        schema_builder.add_text_field("text", text_options);
    }
    let schema = schema_builder.build();

    INFO!(
        "create index, index_path:{}, tokenizer:{}",
        index_path_str,
        tokenizer_with_parameter_str
    );
    // Create the index in the specified directory.
    let mut index = match Index::create_in_dir(index_files_directory, schema) {
        Ok(index) => index,
        Err(e) => {
            let error_info = format!(
                "Failed to create index in directory:{}; exception:{}",
                index_path_str,
                e.to_string()
            );
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Register the tokenizer with the index.
    if let Err(e) = register_tokenizer_to_index(&mut index, tokenizer_type.clone(), text_analyzer) {
        let error_info = format!(
            "Failed to register tokenizer: {}, exception: {}",
            tokenizer_type.name(),
            e
        );
        ERROR!("{}", error_info);
        return Err(error_info);
    }

    // Create the writer with a specified buffer size (e.g., 64 MB).
    let writer = match index.writer_with_num_threads(2, 1024 * 1024 * 64) {
        // 1 GB
        Ok(w) => w,
        Err(e) => {
            let error_info = format!("Failed to create tantivy writer: {}", e);
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    };

    // Configure and set the merge policy.
    let mut merge_policy = tantivy::merge_policy::LogMergePolicy::default();
    merge_policy.set_min_num_segments(5);
    merge_policy.set_max_docs_before_merge(500000);
    merge_policy.set_min_layer_size(10000);
    merge_policy.set_level_log_size(0.75);
    writer.set_merge_policy(Box::new(merge_policy));

    // Save IndexW to cache.
    let indexw = IndexW {
        index,
        path: index_path_str.trim_end_matches('/').to_string(),
        writer: Mutex::new(Some(writer)),
    };

    if let Err(e) = set_index_w(index_path_str.clone(), Arc::new(indexw)) {
        ERROR!("{}", e);
        return Err(e);
    }

    Ok(true)
}

/// Creates an index using the default tokenizer.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `doc_store`: Whether store origin document.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_create_index(index_path: &CxxString, doc_store: bool) -> Result<bool, String> {
    // use `default` as tokenizer.
    let_cxx_string!(tokenizer_with_parameter = "default");
    // get immutable ref from pin.
    let tokenizer_with_parameter_ref: &CxxString = tokenizer_with_parameter.as_ref().get_ref();
    // Delegate to `tantivy_create_index_with_tokenizer` using the default tokenizer.
    tantivy_create_index_with_tokenizer(index_path, tokenizer_with_parameter_ref, doc_store)
}

/// Indexes a document.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `row_id`: Row ID associated with the document.
/// - `doc`: The text data of the document.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_index_doc(
    index_path: &CxxString,
    row_id: u64,
    doc: &CxxString,
) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter index_path: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };
    let doc_str = match doc.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter doc: {}, exception: {}",
                doc,
                e.to_string()
            ));
        }
    };

    // get index writer from CACHE
    let index_w = match get_index_w(index_path_str) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    // get schema from index writer.
    let schema = index_w.index.schema();
    let text_field = match schema.get_field("text") {
        Ok(text_field_) => text_field_,
        Err(e) => {
            ERROR!("Failed to get text field: {}", e.to_string());
            return Err(e.to_string());
        }
    };
    let row_id_field = match schema.get_field("row_id") {
        Ok(row_id_field_) => row_id_field_,
        Err(e) => {
            ERROR!("Failed to get row_id field: {}", e.to_string());
            return Err(e.to_string());
        }
    };

    // create document
    let mut doc = Document::default();
    doc.add_u64(row_id_field, row_id);
    doc.add_text(text_field, doc_str);

    // index document
    match index_w.add_document(doc) {
        Ok(_) => Ok(true),
        Err(e) => {
            let error_info = format!("Failed to index doc:{}", e);
            ERROR!("{}", error_info);
            Err(error_info.to_string())
        }
    }
}

/// Delete a group of row_ids.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
/// - `row_ids`: a group of row_ids that needs to be deleted.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_delete_row_ids(
    index_path: &CxxString,
    row_ids: &CxxVector<u32>,
) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter index_path: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };
    let row_ids: Vec<u32> = row_ids.iter().map(|s| *s as u32).collect();

    // get index writer from CACHE
    let index_w = match get_index_w(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    // get schema from index writer.
    let schema = index_w.index.schema();
    let row_id_field = match schema.get_field("row_id") {
        Ok(row_id_field_) => row_id_field_,
        Err(e) => {
            ERROR!("Failed to get row_id field: {}", e.to_string());
            return Err(e.to_string());
        }
    };
    let terms = row_ids
        .iter()
        .map(|row_id| Term::from_field_u64(row_id_field, *row_id as u64))
        .collect();
    match index_w.delete_terms(terms) {
        Ok(_opstamp) => {
            // something need to do.
        }
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    }
    // after delete_term, need commit.
    match index_w.commit() {
        Ok(_) => {
            //
        }
        Err(e) => {
            let error_info = format!("Failed to commit index writer: {}", e.to_string());
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    }
    // try reload index reader from CACHE
    let reload_status = match get_index_r(index_path_str.clone()) {
        Ok(current_index_reader) => match current_index_reader.reload() {
            Ok(_) => true,
            Err(e) => {
                ERROR!("Can't reload reader after delete operation: {}", e);
                return Err(e);
            }
        },
        Err(e) => {
            WARNING!("{}, skip reload it. ", e);
            true
        }
    };
    Ok(reload_status)
}

/// Commits the changes to the index, writing it to the file system.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_writer_commit(index_path: &CxxString) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter index_path: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };

    // get index writer from CACHE
    let index_w = match get_index_w(index_path_str) {
        Ok(content) => content,
        Err(e) => {
            ERROR!("{}", e);
            return Err(e);
        }
    };

    match index_w.commit() {
        Ok(_) => Ok(true),
        Err(e) => {
            let error_info = format!("Failed to commit index writer: {}", e.to_string());
            ERROR!("{}", error_info);
            return Err(error_info);
        }
    }
}

/// Frees the index writer and waits for all merging threads to complete.
///
/// Arguments:
/// - `index_path`: The directory path for building the index.
///
/// Returns:
/// - A bool value represent operation success.
pub fn tantivy_writer_free(index_path: &CxxString) -> Result<bool, String> {
    // Parse parameter.
    let index_path_str = match index_path.to_str() {
        Ok(content) => content.to_string(),
        Err(e) => {
            return Err(format!(
                "Can't parse parameter index_path: {}, exception: {}",
                index_path,
                e.to_string()
            ));
        }
    };

    // get index writer from CACHE
    let index_w = match get_index_w(index_path_str.clone()) {
        Ok(content) => content,
        Err(e) => {
            WARNING!("Index writer already been removed: {}", e);
            return Ok(false);
        }
    };
    if let Err(e) = index_w.wait_merging_threads() {
        // TODO: time sleep?
        ERROR!("{}", e);
        return Err(e);
    }

    // remove index writer from CACHE
    if let Err(e) = remove_index_w(index_path_str.clone()) {
        ERROR!("{}", e);
        return Err(e);
    };

    INFO!("Index writer:[{}] has been freed", index_path_str);
    Ok(true)
}
