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
    only_record_tantivy_search: bool,
}

impl Filter for TantivyFilter {
    fn filter(&self, record: &log::Record) -> Response {
        if self.only_record_tantivy_search && record.target() == "tantivy_search" {
            Response::Neutral
        } else if self.only_record_tantivy_search {
            Response::Reject
        } else {
            Response::Neutral
        }
    }
}

fn build_log_config(
    log_directory: String,
    log_level: String,
    log_in_file: bool,
    console_dispaly: bool,
    only_record_tantivy_search: bool,
) -> Result<Config, String> {
    /********************     Config for log-file path     ********************/
    let log_path_trimmed = log_directory.trim_end_matches('/').to_string();
    let log_file_path = format!("{}/tantivy-search.log", log_path_trimmed);
    let log_rolling_pattern = format!("{}/tantivy-search.{{}}.log", log_path_trimmed);

    /********************     Config for log-level     ********************/
    let log_level = match log_level.to_lowercase().as_str() {
        "trace" | "tracing" | "traces" | "tracings" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" | "information" => LevelFilter::Info,
        "warn" | "warning" | "warnings" | "warns" => LevelFilter::Warn,
        "error" | "errors" | "fatal" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    let mut config_builder = Config::builder();
    let mut root_builder = Root::builder().appender("file");

    /********************     Config for log-file rolling     ********************/
    // TODO: refine this hard code.
    if log_in_file {
        let roller = FixedWindowRoller::builder()
            .build(&log_rolling_pattern, 3)
            .map_err(|e| e.to_string())?;

        let size_trigger = SizeTrigger::new(20 * 1024 * 1024); // log file trigger size: 20MB
        let policy = CompoundPolicy::new(Box::new(size_trigger), Box::new(roller));

        let file = RollingFileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
            // .append(true)
            .build(log_file_path, Box::new(policy))
            .map_err(|e| e.to_string())?;

        let file_appender = Appender::builder()
            .filter(Box::new(ThresholdFilter::new(log_level))) // log level filter
            .filter(Box::new(TantivyFilter {
                // `target=tantivy_search` filter
                only_record_tantivy_search,
            }))
            .build("file", Box::new(file));
        config_builder = config_builder.appender(file_appender)
    }

    /********************     Config for log-console dispaly     ********************/
    if console_dispaly {
        let stdout = ConsoleAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
            .build();

        let stdout_appender = Appender::builder()
            .filter(Box::new(ThresholdFilter::new(log_level))) // log level filter
            .filter(Box::new(TantivyFilter {
                // `target=tantivy_search` filter
                only_record_tantivy_search,
            }))
            .build("stdout", Box::new(stdout));
        config_builder = config_builder.appender(stdout_appender); // add console Appender to config_builder
        root_builder = root_builder.appender("stdout"); // add console Appender to root_builder
    }

    /********************     Build log config     ********************/
    let config = config_builder
        .build(root_builder.build(log_level))
        .map_err(|e| e.to_string())?;

    crate::INFO!(
        "build log4rs config: [log_path:{}, log_level:{}, console_dispaly:{}, only_record_tantivy_search:{}, log_file_path:{}]",
        log_directory,
        log_level,
        console_dispaly,
        only_record_tantivy_search,
        format!("{}/tantivy-search.log", log_path_trimmed)
    );
    Ok(config)
}

fn apply_log_config(config: Config) -> Result<(), String> {
    match LOG4RS_HANDLE.get() {
        // Update logger config during running time
        Some(handle) => {
            crate::INFO!("updating log4rs handle....");
            handle.set_config(config);
            Ok(())
        }
        // The first time for initializing the handle.
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

/**
 * Initializes the tantivy_search log4rs.
 * log_directory: Directory where logs will be saved.
 * log_level: Log level.
 * console_dispaly: Whether to output tantivy-search logs to the console.
 * only_record_tantivy_search: If set true, log content generated by `tantivy_search` dependency won't be record.
 * callback: Callback will be called in macors.
 */
pub fn initialize_log4rs(
    log_directory: String,
    log_level: String,
    log_in_file: bool,
    console_dispaly: bool,
    only_record_tantivy_search: bool,
    callback: LogCallback,
) -> Result<(), String> {
    // Insert callback to OnceCell, it can't be update during lib running time.
    let _ = LOG_CALLBACK.get_or_init(|| callback);

    // Build log config (log level, filter, stdout...)
    let config: Config = build_log_config(
        log_directory,
        log_level,
        log_in_file,
        console_dispaly,
        only_record_tantivy_search,
    )
    .map_err(|e| format!("Failed to build log config: {}", e))?;

    // Apply log config to Log4RS
    apply_log_config(config)
}

fn get_thread_id() -> String {
    let thread_id: String = format!("{:?}", thread::current().id());
    thread_id
        .chars()
        .filter(|c| c.is_digit(10))
        .collect::<String>()
}

// This function is used by self LOG macros.
pub fn callback_with_thread_info(level: i8, message: String, callback: LogCallback) {
    let thread_id: String = get_thread_id();
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
        Err(_) => CString::new("unknown_error").expect("Failed to create CString from message."),
    };

    callback(level as c_int, thread_info_c.as_ptr(), c_message.as_ptr());
}
