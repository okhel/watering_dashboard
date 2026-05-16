use serde::{Deserialize, Serialize};

/// Which sensor the moisture reading came from.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum Depth { Shallow, Deep }

/// Messages sent from the ESP32 to the server.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Moisture { adc_raw: u16, depth: Depth },
    Pump { duration_s: u16 },
    /// Host-issued command — echoed back by the ESP32 after execution.
    Water { duration_s: u16 },
}

/// A timestamped entry in the server's in-memory log.
#[derive(Serialize, Clone)]
pub struct LogEntry {
    pub timestamp: String, // RFC 3339
    pub message: Message,
}

/// Response body for GET /api/data.
#[derive(Serialize)]
pub struct ApiData {
    pub log: Vec<LogEntry>,
    pub online: bool,
}

/// Request body for POST /api/water.
#[derive(Deserialize)]
pub struct WaterRequest {
    pub duration_s: u16,
}
