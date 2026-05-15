pub const BIND_ADDR: &str = "0.0.0.0:5000";
pub const HTTP_ADDR: &str = "0.0.0.0:8080";
pub const LIVENESS_SECS: u64 = 2 * 60 * 60; // 2 hours — beyond this the node is "offline"
pub const MAX_EVENTS: usize = 1000;
