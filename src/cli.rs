use clap::{Parser, ValueEnum};

#[derive(Copy, Clone, ValueEnum)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl std::fmt::Display for LogLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Trace => "trace",
            Self::Debug => "debug",
            Self::Info => "info",
            Self::Warn => "warn",
            Self::Error => "error",
        };
        write!(f, "{value}")
    }
}

#[derive(Parser)]
#[command(name = "edfs")]
pub struct Args {
    pub secret: String,

    #[arg(short, long, default_value = "mnt")]
    pub mountpoint: String,

    #[arg(short, long, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,

    #[arg(long, default_value = "128")]
    pub max_storage: u64,
}
