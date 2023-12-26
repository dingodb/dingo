use crate::commons::LogCallback;
use crate::commons::LOG_CALLBACK;
use log::LevelFilter;
use log4rs::{
    append::console::ConsoleAppender,
    append::rolling_file::{
        policy::compound::{
            roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger, CompoundPolicy,
        },
        RollingFileAppender,
    },
    config::{Appender, Config, Root},
    encode::pattern::PatternEncoder,
    filter::{threshold::ThresholdFilter, Filter, Response},
};
use once_cell::sync::OnceCell;
use std::ffi::{c_int, CString};
use std::thread;


// Store log4rs handle for runtime update.
static LOG4RS_HANDLE: OnceCell<log4rs::Handle> = OnceCell::new();


#[derive(Debug)]
struct TantivyFilter {
    only_tantivy_search: bool,
}

impl Filter for TantivyFilter {
    fn filter(&self, record: &log::Record) -> Response {
        if self.only_tantivy_search && record.target() == "tantivy_search" {
            Response::Neutral
        } else if self.only_tantivy_search {
            Response::Reject
        } else {
            Response::Neutral
        }
    }
}



fn build_log_config(
    log_path: String,
    log_level: String,
    console_logging: bool,
    only_tantivy_search: bool
) -> Result<Config, String> {
    let log_path_trimmed = log_path.trim_end_matches('/').to_string();
    // process Log Path
    let log_file_path = format!("{}/tantivy-search.log", log_path_trimmed);
    let log_rolling_pattern = format!("{}/tantivy-search.{{}}.log", log_path_trimmed);

    // process Log LevelFilter
    let log_level = match log_level.to_lowercase().as_str() {
        "trace" | "tracing" | "traces" | "tracings" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" | "information" => LevelFilter::Info,
        "warn" | "warning" | "warnings" | "warns" => LevelFilter::Warn,
        "error" | "errors" | "fatal" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    // support log roller.
    let roller = FixedWindowRoller::builder()
        .build(&log_rolling_pattern, 3)
        .map_err(|e| e.to_string())?;

    // log file trigger size: 20MB
    let size_trigger = SizeTrigger::new(20 * 1024 * 1024);
    let policy = CompoundPolicy::new(Box::new(size_trigger), Box::new(roller));

    // log file config
    let file = RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
        // .append(true)
        .build(log_file_path, Box::new(policy))
        .map_err(|e| e.to_string())?;

    // log console config
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
        .build();

    let file_appender = Appender::builder()
        // log level filter
        .filter(Box::new(ThresholdFilter::new(log_level)))
        // tantivy_search target filter
        .filter(Box::new(TantivyFilter { only_tantivy_search }))
        .build("file", Box::new(file));

    let mut config_builder = Config::builder().appender(file_appender);
    let mut root_builder = Root::builder().appender("file");

    // enable console logging
    if console_logging {
        let stdout_appender = Appender::builder()
            // log level filter
            .filter(Box::new(ThresholdFilter::new(log_level)))
            // tantivy_search target filter
            .filter(Box::new(TantivyFilter { only_tantivy_search }))
            .build("stdout", Box::new(stdout));
        // add console Appender to config_builder
        config_builder = config_builder.appender(stdout_appender);
        // add console Appender to root_builder
        root_builder = root_builder.appender("stdout");
    }

    // construct config
    let config = config_builder
        .build(root_builder.build(log_level))
        .map_err(|e| e.to_string())?;

        crate::INFO!(
        "build log4rs config: [log_path:{}, log_level:{}, console_logging:{}, only_tantivy_search:{}, log_file_path:{}]",
        log_path,
        log_level,
        console_logging,
        only_tantivy_search,
        format!("{}/tantivy-search.log", log_path_trimmed)
    );
    Ok(config)
}

fn initialize_logger(
    log_path: String,
    log_level: String,
    console_logging: bool,
    only_tantivy_search: bool
) -> Result<(), String> {
    // construct logger config
    let config = build_log_config(log_path, log_level, console_logging, only_tantivy_search)
        .map_err(|e| format!("Failed to build log config: {}", e))?;

    match LOG4RS_HANDLE.get() {
        // update logger config during running time
        Some(handle) => {
            crate::INFO!("updating log4rs handle....");
            handle.set_config(config);
            Ok(())
        }
        // first time init handle
        None => {
            crate::INFO!("initialize log4rs handle....");
            let handle = log4rs::init_config(config)
                .map_err(|e| format!("Failed to initialize log4rs: {}", e))?;

            LOG4RS_HANDLE
                .set(handle)
                .map_err(|_| "Failed to store log4rs handle".to_string())
        }
    }
}

// Initialize callback from caller.
fn initialize_log_callback(callback: LogCallback) {
    let _ = LOG_CALLBACK.get_or_init(|| callback);
}


/**
 * Initializes the tantivy_search logger.
 * log_path: Directory where logs will be saved.
 * log_level: Log level (requires a restart of CK to update the log level for tantivy-search).
 * console_logging: Whether to output tantivy-search logs to the console.
 * callback: Log callback function provided by CK to output tantivy-search logs to the CK log system.
 * enable_callback: Whether to enable the callback function.
 * only_tantivy_search: Whether the log file should only retain logs from tantivy_search.
 */
pub fn initialize_tantivy_search_logger(
    log_path: String,
    log_level: String,
    console_logging: bool,
    callback: LogCallback,
    enable_callback: bool,
    only_tantivy_search: bool,
) -> Result<(), String> {
    if enable_callback {
        initialize_log_callback(callback);
    }
    initialize_logger(log_path, log_level, console_logging, only_tantivy_search)
}

// This function is used by self LOG macros.
pub fn callback_with_thread_info(level: i8, message: String, callback: LogCallback) {
    let c_thread_id = match CString::new(format!("{:?}", thread::current().id())) {
        Ok(cstr) => cstr,
        Err(_) => CString::new("unknown_error").expect("Failed to create CString from thread id."),
    };

    let c_thread_name =
        match CString::new(thread::current().name().unwrap_or("unknown").to_string()) {
            Ok(cstr) => cstr,
            Err(_) => {
                CString::new("unknown_error").expect("Failed to create CString from thread name.")
            }
        };

    let c_message = match CString::new(message) {
        Ok(cstr) => cstr,
        Err(_) => CString::new("unknown_error").expect("Failed to create CString from message."),
    };

    callback(
        level as c_int,
        c_thread_id.as_ptr(),
        c_thread_name.as_ptr(),
        c_message.as_ptr(),
    );
}
