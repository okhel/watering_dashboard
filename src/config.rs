pub const BIND_ADDR: &str = "0.0.0.0:5000";
pub const HTTP_ADDR: &str = "0.0.0.0:8080";
pub const LIVENESS_SECS: u64 = 2 * 60 * 60;   // 2 h  — beyond this the node is "offline"
pub const RAW_WINDOW_SECS: i64 = 30 * 60;       // 30 min of raw readings kept verbatim
pub const RETAIN_SECS: i64 = 14 * 24 * 3600;    // 14 days total in-memory retention
pub const CSV_PATH: &str = "moisture_log.csv";   // append-only persistent store
pub const AUTO_WATER_INTERVAL_SECS: i64 = 14 * 24 * 3600; // 2 weeks
pub const AUTO_WATER_DURATION_S: u16    = 22;
