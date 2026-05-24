// Per-plant settings selected at startup by CLI flag.
//
// Plant images live in `pics/{name}_{level}.png`, where level ∈
// {drenched, wet, happy, dry, parched, original}. The dashboard picks
// the level at render time from the latest moisture reading.
pub struct PlantConfig {
    pub name:                     &'static str,  // lowercase identifier (logs, CLI, image filenames)
    pub display_name:             &'static str,  // shown in the dashboard UI
    pub bind_addr:                &'static str,  // TCP port the ESP32 connects to
    pub http_addr:                &'static str,  // HTTP port for the dashboard UI
    pub csv_path:                 &'static str,
    pub auto_water_interval_secs: i64,
    pub auto_water_duration_s:    u16,
}

pub const YUCCA: PlantConfig = PlantConfig {
    name:                     "yucca",
    display_name:             "Yucca palm",
    bind_addr:                "0.0.0.0:5000",
    http_addr:                "0.0.0.0:8080",
    csv_path:                 "yucca_log.csv",
    auto_water_interval_secs: 14 * 24 * 3600, // 2 weeks
    auto_water_duration_s:    22,
};

pub const MONSTERA: PlantConfig = PlantConfig {
    name:                     "monstera",
    display_name:             "Monstera",
    bind_addr:                "0.0.0.0:5001",
    http_addr:                "0.0.0.0:8081",
    csv_path:                 "monstera_log.csv",
    auto_water_interval_secs: 10 * 24 * 3600, // 10 days — slightly more frequent than yucca
    auto_water_duration_s:    22,             // same pot, same fill
};

/// Selects the plant config based on a single CLI flag: `--yucca` or `--monstera`.
pub fn from_args() -> &'static PlantConfig {
    match std::env::args().nth(1).as_deref() {
        Some("--yucca")    => &YUCCA,
        Some("--monstera") => &MONSTERA,
        _ => {
            eprintln!("usage: dashboard --yucca | --monstera");
            std::process::exit(2);
        }
    }
}

// Shared constants — same for every plant.
// Liveness threshold: also used as the TCP read_timeout on the server socket so a
// stalled session is torn down at the same moment the UI flips to "offline".
pub const LIVENESS_SECS: u64 = 10 * 60;         // 10 min — node sends every ~60 s
pub const RAW_WINDOW_SECS: i64 = 30 * 60;       // 30 min of raw readings kept verbatim
pub const RETAIN_SECS: i64 = 14 * 24 * 3600;    // 14 days total in-memory retention
