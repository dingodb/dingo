use crate::common::constants::{LogCallback, LOG_CALLBACK};
use crate::INFO;
use once_cell::sync::OnceCell;
use std::ffi::{c_int, CString};
use std::thread;

pub struct TantivySearchLogger;

impl TantivySearchLogger {
    pub fn update_log_callback(
        cell: &OnceCell<LogCallback>,
        callback: LogCallback,
    ) -> Result<(), String> {
        let _ = cell.get_or_init(|| callback);
        Ok(())
    }

    pub fn update_log4rs_handler(
        cell: &OnceCell<log4rs::Handle>,
        log_config: log4rs::Config,
    ) -> Result<(), String> {
        match cell.get() {
            Some(handle) => {
                handle.set_config(log_config);
                INFO!("Successfully updated log handler.");
                Ok(())
            }
            None => {
                let handle = log4rs::init_config(log_config)
                    .map_err(|e| format!("Failed to initialize log4rs: {}", e))?;
                INFO!("Successfully initialize log4rs handler.");
                cell.set(handle)
                    .map_err(|_| "Failed to save log4rs handler to cell".to_string())
            }
        }
    }

    fn get_thread_id() -> String {
        let thread_id: String = format!("{:?}", thread::current().id());
        thread_id
            .chars()
            .filter(|c| c.is_digit(10))
            .collect::<String>()
    }

    pub fn trigger_logger_callback(level: i8, message: String, callback: LogCallback) {
        let thread_id: String = Self::get_thread_id();
        let thread_name: String = thread::current().name().unwrap_or("none").to_string();

        let thread_info: String = if thread_name == "none" {
            format!("[{}]", thread_id)
        } else {
            format!("[{}] {}", thread_id, thread_name)
        };

        let thread_info_c = match CString::new(thread_info) {
            Ok(cstr) => cstr,
            Err(_) => CString::new("none").expect("Failed to create CString from thread_info."),
        };

        let c_message = match CString::new(message) {
            Ok(cstr) => cstr,
            Err(_) => {
                CString::new("unknown_error").expect("Failed to create CString from message.")
            }
        };

        callback(level as c_int, thread_info_c.as_ptr(), c_message.as_ptr());
    }
}

#[cfg(test)]
mod tests {
    use crate::{LOG4RS_HANDLE, TEST_MUTEX};

    use super::*;
    use libc::*;
    use log::LevelFilter;
    use log4rs::{
        append::console::ConsoleAppender,
        config::{Appender, Config, Root},
        encode::pattern::PatternEncoder,
    };
    use once_cell::sync::OnceCell;

    extern "C" fn log_callback_for_test(level: i32, _info: *const c_char, _message: *const c_char) {
        assert_eq!(level, 1);
    }

    #[test]
    fn test_update_log_callback() {
        let callback_cell: OnceCell<LogCallback> = OnceCell::new();
        assert!(callback_cell.get().is_none());
        let result =
            TantivySearchLogger::update_log_callback(&callback_cell, log_callback_for_test);
        assert!(callback_cell.get().is_some());
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_log4rs_handler() {
        let _guard = TEST_MUTEX.lock().unwrap();

        let stdout_appender = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}\n")))
            .build();

        let log_config_info = Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout_appender)))
            .build(Root::builder().appender("stdout").build(LevelFilter::Debug))
            .expect("Failed to build log config with stdout appender");

        // assert!(LOG4RS_HANDLE.get().is_none()); // Need set RUST_TEST_THREADS=1
        let result = TantivySearchLogger::update_log4rs_handler(&LOG4RS_HANDLE, log_config_info);
        assert!(result.is_ok());
        assert!(format!("{:?}", LOG4RS_HANDLE.get().unwrap()).contains("Debug"));
        assert!(!format!("{:?}", LOG4RS_HANDLE.get().unwrap()).contains("Info"));

        // ConsoleAppender doesn't impl Clone trait.
        let stdout_appender = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {m}\n")))
            .build();

        let log_config_debug = Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout_appender)))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .expect("Failed to build log config with stdout appender");

        assert!(LOG4RS_HANDLE.get().is_some());
        let result = TantivySearchLogger::update_log4rs_handler(&LOG4RS_HANDLE, log_config_debug);
        assert!(result.is_ok());

        assert!(!format!("{:?}", LOG4RS_HANDLE.get().unwrap()).contains("Debug"));
        assert!(format!("{:?}", LOG4RS_HANDLE.get().unwrap()).contains("Info"));
    }

    #[test]
    fn test_get_thread_id() {
        let thread_id = TantivySearchLogger::get_thread_id();
        assert!(!thread_id.is_empty());
        assert!(thread_id.chars().all(char::is_numeric));
    }

    #[test]
    fn test_trigger_logger_callback() {
        let callback: LogCallback = log_callback_for_test;
        TantivySearchLogger::trigger_logger_callback(1, "Test message".to_string(), callback);
    }
}
