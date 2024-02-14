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

#[derive(Debug)]
struct LoggerFilter {
    only_record_tantivy_search: bool,
}

impl Filter for LoggerFilter {
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

pub struct LoggerConfig {
    log_directory: String,            // log file stored path.
    log_level: LevelFilter,           // log level.
    log_in_file: bool,                // store log content to file.
    console_display: bool,            // display log content in console.
    only_record_tantivy_search: bool, // only record `tantivy_search` log content.
}

impl LoggerConfig {
    pub fn new(
        log_directory: String,
        log_level: String,
        log_in_file: bool,
        console_display: bool,
        only_record_tantivy_search: bool,
    ) -> Self {
        let log_level = match log_level.to_lowercase().as_str() {
            "trace" | "tracing" | "traces" | "tracings" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" | "information" => LevelFilter::Info,
            "warn" | "warning" | "warnings" | "warns" => LevelFilter::Warn,
            "error" | "errors" | "fatal" => LevelFilter::Error,
            _ => LevelFilter::Info,
        };

        LoggerConfig {
            log_directory,
            log_level,
            log_in_file,
            console_display,
            only_record_tantivy_search,
        }
    }

    pub fn build_logger_config(&self) -> Result<Config, String> {
        let mut config_builder = Config::builder();
        let mut root_builder = Root::builder();

        // Config for logger file rolling update.
        if self.log_in_file {
            let log_path_trimmed = self.log_directory.trim_end_matches('/').to_string();
            let log_file_path = format!("{}/tantivy-search.log", log_path_trimmed);
            let log_rolling_pattern = format!("{}/tantivy-search.{{}}.log", log_path_trimmed);

            let roller = FixedWindowRoller::builder()
                .build(&log_rolling_pattern, 15)
                .map_err(|e| e.to_string())?;

            let size_trigger = SizeTrigger::new(200 * 1024 * 1024); // log file trigger size: 100MB
            let policy = CompoundPolicy::new(Box::new(size_trigger), Box::new(roller));

            let file = RollingFileAppender::builder()
                .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
                .build(log_file_path, Box::new(policy))
                .map_err(|e| e.to_string())?;

            let file_appender = Appender::builder()
                .filter(Box::new(ThresholdFilter::new(self.log_level)))
                .filter(Box::new(LoggerFilter {
                    only_record_tantivy_search: self.only_record_tantivy_search,
                }))
                .build("file", Box::new(file));
            config_builder = config_builder.appender(file_appender);
            root_builder = root_builder.appender("file");
        }

        // Config for console dispaly.
        if self.console_display {
            let stdout = ConsoleAppender::builder()
                .encoder(Box::new(PatternEncoder::new("{d} - {l} - {t} - {m}{n}")))
                .build();

            let stdout_appender = Appender::builder()
                .filter(Box::new(ThresholdFilter::new(self.log_level)))
                .filter(Box::new(LoggerFilter {
                    only_record_tantivy_search: self.only_record_tantivy_search,
                }))
                .build("stdout", Box::new(stdout));
            config_builder = config_builder.appender(stdout_appender);
            root_builder = root_builder.appender("stdout");
        }

        // Build and apply log config.
        let config = config_builder
            .build(root_builder.build(self.log_level))
            .map_err(|e| e.to_string())?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_build_logger_config() {
        let logger_config = LoggerConfig::new(
            String::from("/tmp//"),
            String::from("debug"),
            true,
            true,
            false,
        );
        let config_result = logger_config.build_logger_config();

        assert!(config_result.is_ok());
    }

    #[test]
    fn test_not_exist_log_directory() {
        // If path not exist, log4rs will create it.
        let not_exist = TempDir::new().unwrap();
        fs::remove_dir_all(not_exist.path()).unwrap();
        assert_eq!(not_exist.path().exists(), false);

        let logger_config = LoggerConfig::new(
            String::from(not_exist.path().to_str().unwrap()),
            String::from("info"),
            true,
            false,
            false,
        );
        let config_result = logger_config.build_logger_config();
        assert!(config_result.is_ok());
    }
}
