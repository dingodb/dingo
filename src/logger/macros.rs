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
