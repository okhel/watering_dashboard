use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    Moisture { adc_raw: u16 },
    /// Pump fired for this long.
    Pump { duration_s: u16 },
    /// Host-issued command: run pump for this many seconds.
    Water { duration_s: u16 },
}
