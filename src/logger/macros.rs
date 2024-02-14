#[macro_export]
macro_rules! FATAL {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-2, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-2, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::error!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-2, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::error!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-2, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! ERROR {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-1, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::error!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-1, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::error!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-1, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::error!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(-1, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! WARNING {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::warn!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(0, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::warn!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(0, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::warn!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(0, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::warn!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(0, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! INFO {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::info!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(1, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::info!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(1, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::info!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(1, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::info!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(1, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! DEBUG {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::debug!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(2, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::debug!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(2, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::debug!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(2, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::debug!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(2, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[macro_export]
macro_rules! TRACE {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {{
        log::trace!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(3, format!("[{}: {}] - {}", $target, $function, format!($($arg)+)), *callback);
        }
    }};
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {{
        log::trace!(target: "tantivy_search", "[{}] - {}", $function, format_args!($($arg)+));
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(3, format!("[tantivy_search: {}] - {}", $function, format!($($arg)+)), *callback);
        }
    }};
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {{
        log::trace!(target: $target, $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(3, format!("[{}] - {}", $target, format!($($arg)+)), *callback);
        }
    }};
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {{
        log::trace!(target: "tantivy_search", $($arg)+);
        if let Some(callback) = LOG_CALLBACK.get() {
            TantivySearchLogger::trigger_logger_callback(3, format!("[tantivy_search] - {}", format!($($arg)+)), *callback);
        }
    }};
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;

    use crate::logger::logger_bridge::TantivySearchLogger;
    use crate::{empty_log_callback, update_logger_for_test, LOG_CALLBACK, TEST_MUTEX};

    fn prepare_callback() {
        let _ = LOG_CALLBACK.get_or_init(|| empty_log_callback);
    }

    #[test]
    fn test_fatal_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Error);
        FATAL!(target: "other", function: "test_fatal_logger", "Here is a fatal log, number:{}", 20);
        FATAL!(target: "tantivy_search", function: "test_fatal_logger", "Here is a fatal log, number:{}", 20);
        FATAL!(target: "tantivy_search", "Here is a fatal log, number:{}", 20);
        FATAL!(function: "test_fatal_logger", "Here is a fatal log, number:{}", 20);
        FATAL!("Here is a fatal log, number:{}", 20);
    }

    #[test]
    fn test_error_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Debug);
        ERROR!(target: "other", function: "test_error_logger", "Here is a error log, number:{}", 20);
        ERROR!(target: "tantivy_search", function: "test_error_logger", "Here is a error log, number:{}", 20);
        ERROR!(target: "tantivy_search", "Here is a error log, number:{}", 20);
        ERROR!(function: "test_error_logger", "Here is a error log, number:{}", 20);
        ERROR!("Here is a error log, number:{}", 20);
    }

    #[test]
    fn test_warning_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Warn);
        WARNING!(target: "other", function: "test_warning_logger", "Here is a warning log, number:{}", 20);
        WARNING!(target: "tantivy_search", function: "test_warning_logger", "Here is a warning log, number:{}", 20);
        WARNING!(target: "tantivy_search", "Here is a warning log, number:{}", 20);
        WARNING!(function: "test_warning_logger", "Here is a warning log, number:{}", 20);
        WARNING!("Here is a warning log, number:{}", 20);
    }

    #[test]
    fn test_info_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Info);
        INFO!(target: "other", function: "test_info_logger", "Here is a info log, number:{}", 20);
        INFO!(target: "tantivy_search", function: "test_info_logger", "Here is a info log, number:{}", 20);
        INFO!(target: "tantivy_search", "Here is a info log, number:{}", 20);
        INFO!(function: "test_info_logger", "Here is a info log, number:{}", 20);
        INFO!("Here is a info log, number:{}", 20);
    }

    #[test]
    fn test_debug_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Debug);
        DEBUG!(target: "other", function: "test_debug_logger", "Here is a debug log, number:{}", 20);
        DEBUG!(target: "tantivy_search", function: "test_debug_logger", "Here is a debug log, number:{}", 20);
        DEBUG!(target: "tantivy_search", "Here is a debug log, number:{}", 20);
        DEBUG!(function: "test_debug_logger", "Here is a debug log, number:{}", 20);
        DEBUG!("Here is a debug log, number:{}", 20);
    }

    #[test]
    fn test_trace_logger() {
        let _guard = TEST_MUTEX.lock().unwrap();
        prepare_callback();
        update_logger_for_test(LevelFilter::Trace);
        TRACE!(target: "other", function: "test_trace_logger", "Here is a log, number:{}", 20);
        TRACE!(target: "tantivy_search", function: "test_trace_logger", "Here is a log, number:{}", 20);
        TRACE!(target: "tantivy_search", "Here is a log, number:{}", 20);
        TRACE!(function: "test_trace_logger", "Here is a log, number:{}", 20);
        TRACE!("Here is a log, number:{}", 20);
    }
}
