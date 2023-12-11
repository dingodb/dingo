#[macro_export]
macro_rules! FATAL {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::error!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-2, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::error!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-2, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::error!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-2, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::error!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-2, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}

#[macro_export]
macro_rules! ERROR {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::error!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-1, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::error!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-1, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::error!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-1, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::error!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(-1, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}

#[macro_export]
macro_rules! WARNING {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::warn!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(0, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::warn!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(0, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::warn!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(0, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::warn!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(0, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}

#[macro_export]
macro_rules! INFO {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::info!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(1, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::info!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(1, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::info!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(1, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::info!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(1, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}

#[macro_export]
macro_rules! DEBUG {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::debug!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(2, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::debug!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(2, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::debug!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(2, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::debug!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(2, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}

#[macro_export]
macro_rules! TRACE {
    // provide target、function、message
    (target: $target:expr, function: $function:expr, $($arg:tt)+) => {
        {
            log::trace!(target: $target, "[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(3, format!("{} - [{}] - {}",$target, $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide function、message
    (function: $function:expr, $($arg:tt)+) => {
        {
            log::trace!("[{}] - {}", $function, format_args!($($arg)+));
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(3, format!("[{}] - {}", $function, format!($($arg)+)), *callback);
            }
        }
    };
    // provide target、message
    (target: $target:expr, $($arg:tt)+) => {
        {
            log::trace!(target:$target, $($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(3, format!("{} - [unknown] - {}",$target, format!($($arg)+)), *callback);
            }
        }
    };
    // provide message, log will use default target, such as `tantivy_search`.
    ($($arg:tt)+) => {
        {
            log::trace!($($arg)+);
            if let Some(callback) = LOG_CALLBACK.get() {
                callback_with_thread_info(3, format!("tantivy_search - [unknown] - {}", format!($($arg)+)), *callback);
            }
        }
    };
}
