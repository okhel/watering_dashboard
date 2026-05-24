use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use std::thread;
use chrono::{Local, NaiveDateTime, TimeZone};
use axum::{Router, routing::{get, post}, extract::State, Json};
use axum::http::StatusCode;
use axum::response::Html;
mod config;
mod types;
use config::{LIVENESS_SECS, PlantConfig, RAW_WINDOW_SECS, RETAIN_SECS};
use types::{ApiData, Depth, LogEntry, Message, WaterRequest};

struct SharedState {
    log:             Mutex<Vec<LogEntry>>,
    node_tx:         Mutex<Option<mpsc::Sender<u16>>>,
    written_buckets: Mutex<std::collections::HashSet<(i64, bool)>>,
    plant:           &'static PlantConfig,
    start_time:      i64,  // server boot, used as fallback "last watered" for auto-water
}

// --- Node communication (TCP) ---

fn node_listener(state: Arc<SharedState>) {
    let bind_addr = state.plant.bind_addr;
    let listener = TcpListener::bind(bind_addr).expect("TCP bind failed");
    println!("TCP  listening on {bind_addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = node_session(s, &state) {
                    eprintln!("node disconnected: {e}");
                }
                *state.node_tx.lock().unwrap() = None;
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

fn node_session(stream: TcpStream, state: &Arc<SharedState>) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("[{}] connected: {peer}", Local::now().format("%H:%M:%S"));

    // No message within LIVENESS_SECS => read times out => session torn down.
    // Same threshold also flips the UI pill — single source of truth.
    stream.set_read_timeout(Some(Duration::from_secs(LIVENESS_SECS)))?;

    // Per-session channel for sending water commands to the node
    let (tx, rx) = mpsc::channel::<u16>();
    *state.node_tx.lock().unwrap() = Some(tx);

    // Writer thread: sends water commands whenever they arrive
    let mut write_stream = stream.try_clone()?;
    thread::spawn(move || {
        loop {
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(duration_s) => {
                    let json = format!(r#"{{"Water":{{"duration_s":{duration_s}}}}}"#);
                    let len = json.len() as u16;
                    let hdr = [(len >> 8) as u8, len as u8];
                    if write_stream.write_all(&hdr).is_err()
                        || write_stream.write_all(json.as_bytes()).is_err()
                    {
                        break; // Connection dropped, exit writer
                    }
                    println!("[{}] → water {duration_s}s", Local::now().format("%H:%M:%S"));
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}       // Poll again
                Err(mpsc::RecvTimeoutError::Disconnected) => break, // Reader exited
            }
        }
    });

    // Reader loop: blocking, no timeout needed
    let mut read_stream = stream;
    let mut len_buf = [0u8; 2];
    loop {
        read_stream.read_exact(&mut len_buf)?;
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        read_stream.read_exact(&mut payload)?;

        match serde_json::from_slice::<Message>(&payload) {
            Ok(msg) => {
                println!("[{}] {msg:?}", Local::now().format("%H:%M:%S"));
                let entry = LogEntry { timestamp: Local::now().to_rfc3339(), message: msg };
                // Pump events are sparse — flush immediately to CSV
                if matches!(entry.message, Message::Pump { .. }) {
                    append_csv_rows(state.plant.csv_path, &[entry.clone()]);
                }
                let sealed = {
                    let mut log     = state.log.lock().unwrap();
                    let mut written = state.written_buckets.lock().unwrap();
                    log.push(entry);
                    compact_log(&mut log, &mut written)
                };
                if !sealed.is_empty() { append_csv_rows(state.plant.csv_path, &sealed); }
            }
            Err(e) => eprintln!("decode error ({len} bytes): {e}"),
        }
    }
}

// --- Log compaction ---
//
// Called after every new entry. Keeps:
//   • all events (pump/water) verbatim — they're sparse and always useful
//   • moisture readings from the last RAW_WINDOW_SECS verbatim
//   • moisture readings older than that collapsed to one average per hour
//   • nothing older than RETAIN_SECS

// Returns newly sealed hourly entries that should be appended to CSV.
// A bucket is "sealed" once its end (bucket_ts + 3600) is older than the raw window,
// meaning no further raw readings can still belong to it.
fn compact_log(
    log: &mut Vec<LogEntry>,
    written: &mut std::collections::HashSet<(i64, bool)>,
) -> Vec<LogEntry> {
    let now_secs  = Local::now().timestamp();
    let cutoff    = now_secs - RETAIN_SECS;
    let raw_edge  = now_secs - RAW_WINDOW_SECS;
    let seal_edge = raw_edge - 3600; // bucket fully outside raw window

    // Drop everything beyond the retention window
    log.retain(|e| {
        chrono::DateTime::parse_from_rfc3339(&e.timestamp)
            .map(|t| t.timestamp() > cutoff)
            .unwrap_or(false)
    });

    // Split moisture entries into raw (recent) vs. to-be-averaged (older)
    let mut recent:       Vec<LogEntry> = Vec::new();
    let mut to_average:   Vec<LogEntry> = Vec::new();
    let mut non_moisture: Vec<LogEntry> = Vec::new();

    for entry in log.drain(..) {
        let ts_secs = chrono::DateTime::parse_from_rfc3339(&entry.timestamp)
            .map(|t| t.timestamp())
            .unwrap_or(0);
        match &entry.message {
            Message::Moisture { .. } if ts_secs >= raw_edge => recent.push(entry),
            Message::Moisture { .. }                        => to_average.push(entry),
            _                                               => non_moisture.push(entry),
        }
    }

    // Accumulate hourly buckets: (bucket_epoch, is_deep) -> (sum, count)
    let mut hourly: std::collections::BTreeMap<(i64, bool), (u64, u32)> = std::collections::BTreeMap::new();
    for entry in &to_average {
        if let Message::Moisture { adc_raw, depth } = &entry.message {
            let ts = chrono::DateTime::parse_from_rfc3339(&entry.timestamp)
                .map(|t| t.timestamp())
                .unwrap_or(0);
            let bucket = ts / 3600 * 3600; // floor to hour
            let acc = hourly.entry((bucket, *depth == Depth::Deep)).or_insert((0, 0));
            acc.0 += *adc_raw as u64;
            acc.1 += 1;
        }
    }

    // One averaged LogEntry per (bucket, depth)
    let mut averaged: Vec<LogEntry> = hourly
        .into_iter()
        .filter_map(|((bucket_ts, is_deep), (sum, count))| {
            let avg   = (sum / count as u64) as u16;
            let depth = if is_deep { Depth::Deep } else { Depth::Shallow };
            let ts    = NaiveDateTime::from_timestamp_opt(bucket_ts, 0)
                .map(|ndt| Local.from_utc_datetime(&ndt).to_rfc3339())?;
            Some(LogEntry { timestamp: ts, message: Message::Moisture { adc_raw: avg, depth } })
        })
        .collect();

    // Collect newly sealed buckets to hand back for CSV writing
    let mut to_csv: Vec<LogEntry> = Vec::new();
    for entry in &averaged {
        if let Message::Moisture { depth, .. } = &entry.message {
            if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&entry.timestamp) {
                let key = (t.timestamp(), *depth == Depth::Deep);
                if t.timestamp() < seal_edge && written.insert(key) {
                    to_csv.push(entry.clone());
                }
            }
        }
    }

    log.append(&mut averaged);
    log.append(&mut non_moisture);
    log.append(&mut recent);
    log.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    to_csv
}

// --- Auto-watering ---
//
// Checks every minute. Reference time = last Pump event in the log,
// or server start time if no watering has ever been recorded.
// Sends a water command via node_tx when the interval elapses.

/// Returns the unix-timestamp (seconds) of the last pump event, or server
/// start time as fallback. Shared by auto-water and the dashboard API so
/// the displayed countdown matches the actual trigger.
fn last_water_reference(state: &SharedState) -> i64 {
    let log = state.log.lock().unwrap();
    log.iter()
        .filter(|e| matches!(e.message, Message::Pump { .. }))
        .last()
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e.timestamp).ok())
        .map(|t| t.timestamp())
        .unwrap_or(state.start_time)
}

fn auto_water_checker(state: Arc<SharedState>) {
    loop {
        thread::sleep(Duration::from_secs(60));

        let reference = last_water_reference(&state);

        let elapsed = Local::now().timestamp() - reference;
        if elapsed >= state.plant.auto_water_interval_secs {
            let tx = state.node_tx.lock().unwrap();
            if let Some(tx) = tx.as_ref() {
                tx.send(state.plant.auto_water_duration_s).ok();
                println!("[{}] Auto-water sent ({} days since last)",
                    Local::now().format("%H:%M:%S"), elapsed / 86400);
            }
        }
    }
}

// --- CSV persistence ---

fn load_csv(path: &str) -> (Vec<LogEntry>, std::collections::HashSet<(i64, bool)>) {
    use std::io::BufRead;
    let mut entries: Vec<LogEntry> = Vec::new();
    let mut written: std::collections::HashSet<(i64, bool)> = std::collections::HashSet::new();

    let Ok(file) = std::fs::File::open(path) else {
        return (entries, written); // first run — no file yet
    };

    for line in std::io::BufReader::new(file).lines().skip(1) {
        let Ok(line) = line else { continue };
        let parts: Vec<&str> = line.splitn(5, ',').collect();
        if parts.len() < 5 { continue }

        let timestamp = parts[0].to_string();
        let msg = match parts[1] {
            "moisture" => {
                let Ok(adc_raw) = parts[2].parse::<u16>() else { continue };
                let depth = if parts[3] == "Deep" { Depth::Deep } else { Depth::Shallow };
                if let Ok(t) = chrono::DateTime::parse_from_rfc3339(&timestamp) {
                    written.insert((t.timestamp(), depth == Depth::Deep));
                }
                Message::Moisture { adc_raw, depth }
            }
            "pump" => {
                let Ok(dur) = parts[4].parse::<u16>() else { continue };
                Message::Pump { duration_s: dur }
            }
            _ => continue,
        };
        entries.push(LogEntry { timestamp, message: msg });
    }

    println!("CSV: loaded {} total entries", entries.len());

    // Only keep the retention window in memory; the CSV holds everything
    let cutoff = Local::now().timestamp() - RETAIN_SECS;
    entries.retain(|e| {
        chrono::DateTime::parse_from_rfc3339(&e.timestamp)
            .map(|t| t.timestamp() > cutoff)
            .unwrap_or(false)
    });

    (entries, written)
}

fn append_csv_rows(path: &str, entries: &[LogEntry]) {
    use std::io::Write as _;
    let needs_header = !std::path::Path::new(path).exists();
    let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true).append(true).open(path)
    else {
        eprintln!("CSV: could not open {path} for writing");
        return;
    };
    if needs_header {
        let _ = writeln!(file, "timestamp,kind,adc_raw,depth,duration_s");
    }
    for entry in entries {
        let line = match &entry.message {
            Message::Moisture { adc_raw, depth } =>
                format!("{},moisture,{},{},",
                    entry.timestamp, adc_raw,
                    if *depth == Depth::Deep { "Deep" } else { "Shallow" }),
            Message::Pump { duration_s } =>
                format!("{},pump,,,{}", entry.timestamp, duration_s),
            Message::Water { .. } => continue, // echo — not worth storing
        };
        let _ = writeln!(file, "{line}");
    }
}

// --- Dashboard API (HTTP) ---

async fn serve_dashboard(State(state): State<Arc<SharedState>>) -> Html<String> {
    Html(DASHBOARD_HTML.replace("{{PLANT}}", state.plant.display_name))
}

async fn serve_dashboard_css() -> impl axum::response::IntoResponse {
    ([(axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8")], DASHBOARD_CSS)
}

async fn serve_plant_image(State(state): State<Arc<SharedState>>) -> impl axum::response::IntoResponse {
    match tokio::fs::read(state.plant.image_path).await {
        Ok(bytes) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "image/png")],
            bytes,
        ),
        Err(e) => {
            eprintln!("could not read {}: {e}", state.plant.image_path);
            (StatusCode::NOT_FOUND, [(axum::http::header::CONTENT_TYPE, "image/png")], Vec::new())
        }
    }
}

async fn get_event_log(State(state): State<Arc<SharedState>>) -> Json<ApiData> {
    let next_ts = last_water_reference(&state) + state.plant.auto_water_interval_secs;
    let next_water_at = NaiveDateTime::from_timestamp_opt(next_ts, 0)
        .map(|ndt| Local.from_utc_datetime(&ndt).to_rfc3339())
        .unwrap_or_default();

    let log = state.log.lock().unwrap().clone();
    let online = log.iter()
        .filter(|e| matches!(e.message, Message::Moisture { .. }))
        .last()
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e.timestamp).ok())
        .map(|t| (chrono::Local::now() - t.with_timezone(&chrono::Local)).num_seconds().unsigned_abs() < LIVENESS_SECS)
        .unwrap_or(false);
    Json(ApiData { log, online, next_water_at })
}

async fn post_water_command(
    State(state): State<Arc<SharedState>>,
    Json(body): Json<WaterRequest>,
) -> StatusCode {
    match state.node_tx.lock().unwrap().as_ref() {
        Some(tx) => { tx.send(body.duration_s).ok(); StatusCode::OK }
        None     => StatusCode::SERVICE_UNAVAILABLE,
    }
}

// --- main ---

#[tokio::main]
async fn main() {
    let plant = config::from_args();
    let (initial_log, initial_written) = load_csv(plant.csv_path);
    let state = Arc::new(SharedState {
        log:             Mutex::new(initial_log),
        node_tx:         Mutex::new(None),
        written_buckets: Mutex::new(initial_written),
        plant,
        start_time:      Local::now().timestamp(),
    });

    let state_node  = Arc::clone(&state);
    let state_water = Arc::clone(&state);
    thread::spawn(move || node_listener(state_node));
    thread::spawn(move || auto_water_checker(state_water));

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/dashboard.css", get(serve_dashboard_css))
        .route("/plant.png", get(serve_plant_image))
        .route("/api/data", get(get_event_log))
        .route("/api/water", post(post_water_command))
        .with_state(state);

    let http_addr = plant.http_addr;
    let listener = tokio::net::TcpListener::bind(http_addr).await.unwrap();
    println!("HTTP dashboard for '{}' on http://{http_addr}", plant.name);
    axum::serve(listener, app).await.unwrap();
}


const DASHBOARD_HTML: &str = include_str!("dashboard.html");
const DASHBOARD_CSS:  &str = include_str!("dashboard.css");
