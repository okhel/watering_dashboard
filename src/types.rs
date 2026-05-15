use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Message {
    /// Moisture in centi-percent: 4250 = 42.50 %.
    Moisture { value_cpct: u16 },
    /// Pump fired for this long.
    Pump { duration_ms: u32 },
}
