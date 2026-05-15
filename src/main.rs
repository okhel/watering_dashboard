use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use std::thread;
use chrono::Local;
use axum::{Router, routing::{get, post}, extract::State, Json};
use axum::http::StatusCode;
use axum::response::Html;
use serde::{Serialize, Deserialize};

mod config;
mod types;
use config::{BIND_ADDR, HTTP_ADDR, LIVENESS_SECS, MAX_EVENTS};
use types::Message;

#[derive(Serialize, Clone)]
struct Event {
    timestamp: String, // RFC 3339 — JS can parse this directly
    kind: String,
    data: serde_json::Value,
}

#[derive(Serialize)]
struct ApiData {
    events: Vec<Event>,
    online: bool,
}

#[derive(Deserialize)]
struct WaterRequest {
    duration_s: u16,
}

struct AppState {
    events:   Mutex<Vec<Event>>,
    water_tx: Mutex<Option<mpsc::Sender<u16>>>, // None when no ESP32 connected
}

// --- TCP server ---

fn tcp_server(state: Arc<AppState>) {
    let listener = TcpListener::bind(BIND_ADDR).expect("TCP bind failed");
    println!("TCP  listening on {BIND_ADDR}");

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = handle_connection(s, &state) {
                    eprintln!("connection closed: {e}");
                }
                // Clear the water channel when the connection drops
                *state.water_tx.lock().unwrap() = None;
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
}

fn handle_connection(stream: TcpStream, state: &Arc<AppState>) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("[{}] connected: {peer}", Local::now().format("%H:%M:%S"));

    // Create a per-connection water command channel
    let (tx, rx) = mpsc::channel::<u16>();
    *state.water_tx.lock().unwrap() = Some(tx);

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
                let ts = Local::now().to_rfc3339();
                println!("[{}] {msg:?}", Local::now().format("%H:%M:%S"));
                let event = to_event(ts, &msg);
                let mut events = state.events.lock().unwrap();
                events.push(event);
                if events.len() > MAX_EVENTS {
                    events.remove(0);
                }
            }
            Err(e) => eprintln!("decode error ({len} bytes): {e}"),
        }
    }
}

fn to_event(ts: String, msg: &Message) -> Event {
    match msg {
        Message::Moisture { adc_raw } => Event {
            timestamp: ts, kind: "moisture".into(),
            data: serde_json::json!({ "adc_raw": adc_raw }),
        },
        Message::Pump { duration_s } => Event {
            timestamp: ts, kind: "pump".into(),
            data: serde_json::json!({ "duration_s": duration_s }),
        },
        Message::Water { duration_s } => Event {
            timestamp: ts, kind: "water".into(),
            data: serde_json::json!({ "duration_s": duration_s }),
        },
    }
}

// --- HTTP handlers ---

async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn api_data(State(state): State<Arc<AppState>>) -> Json<ApiData> {
    let events = state.events.lock().unwrap().clone();
    let online = events.iter()
        .filter(|e| e.kind == "moisture")
        .last()
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e.timestamp).ok())
        .map(|t| (chrono::Local::now() - t.with_timezone(&chrono::Local)).num_seconds().unsigned_abs() < LIVENESS_SECS)
        .unwrap_or(false);
    Json(ApiData { events, online })
}

async fn api_water(
    State(state): State<Arc<AppState>>,
    Json(body): Json<WaterRequest>,
) -> StatusCode {
    match state.water_tx.lock().unwrap().as_ref() {
        Some(tx) => { tx.send(body.duration_s).ok(); StatusCode::OK }
        None     => StatusCode::SERVICE_UNAVAILABLE,
    }
}

// --- main ---

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState {
        events:   Mutex::new(Vec::new()),
        water_tx: Mutex::new(None),
    });

    let state_tcp = Arc::clone(&state);
    thread::spawn(move || tcp_server(state_tcp));

    let app = Router::new()
        .route("/", get(index))
        .route("/api/data", get(api_data))
        .route("/api/water", post(api_water))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(HTTP_ADDR).await.unwrap();
    println!("HTTP dashboard on http://{HTTP_ADDR}");
    axum::serve(listener, app).await.unwrap();
}

const DASHBOARD_HTML: &str = r####"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Garden dashboard</title>
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, sans-serif; background: #f5f5f2; color: #1a1a18; }
.container { max-width: 860px; margin: 0 auto; padding: 2rem 1rem; }
.topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem; }
.title { font-size: 20px; font-weight: 500; }
.pill { display: inline-flex; align-items: center; gap: 6px; font-size: 13px; padding: 4px 12px; border-radius: 999px; border: 1px solid; }
.pill.online  { color: #0f6e56; background: #e1f5ee; border-color: #9fe1cb; }
.pill.offline { color: #993c1d; background: #faece7; border-color: #f5c4b3; }
.dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.cards { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; margin-bottom: 1.5rem; }
.card { background: #ebebea; border-radius: 8px; padding: 1rem; }
.card-label { font-size: 13px; color: #5f5e5a; margin-bottom: 6px; }
.card-value { font-size: 24px; font-weight: 500; }
.card-sub { font-size: 12px; color: #888780; margin-top: 4px; }
.section { background: white; border: 1px solid #e8e8e4; border-radius: 12px; padding: 1.25rem; margin-bottom: 1.5rem; }
.section-label { font-size: 13px; color: #5f5e5a; font-weight: 500; margin-bottom: 12px; }
.log-row { display: flex; align-items: center; gap: 12px; padding: 8px 0; border-bottom: 1px solid #f1efe8; font-size: 13px; }
.log-row:last-child { border-bottom: none; }
.log-time { color: #888780; min-width: 130px; font-variant-numeric: tabular-nums; }
.log-badge { font-size: 11px; padding: 2px 8px; border-radius: 6px; font-weight: 500; min-width: 64px; text-align: center; }
.badge-moisture { background: #e6f1fb; color: #185fa5; }
.badge-pump { background: #e1f5ee; color: #0f6e56; }
.log-msg { color: #1a1a18; }
.actions { display: flex; gap: 12px; align-items: center; }
.btn { padding: 8px 20px; border-radius: 8px; font-size: 14px; cursor: pointer; background: #e6f1fb; color: #185fa5; border: 1px solid #b5d4f4; }
.btn:hover { background: #b5d4f4; }
.empty { color: #888780; font-size: 13px; }
.status-note { font-size: 13px; color: #5f5e5a; }
</style>
</head>
<body>
<div class="container">
  <div class="topbar">
    <span class="title">Yucca palm</span>
    <span class="pill offline" id="pill"><span class="dot"></span><span id="pill-text">offline</span></span>
  </div>
  <div class="cards">
    <div class="card">
      <div class="card-label">Moisture (ADC raw)</div>
      <div class="card-value" id="c-moisture">—</div>
      <div class="card-sub" id="c-moisture-sub">no data</div>
    </div>
    <div class="card">
      <div class="card-label">Last watered</div>
      <div class="card-value" id="c-watered">—</div>
      <div class="card-sub" id="c-watered-sub"></div>
    </div>
    <div class="card">
      <div class="card-label">Readings (7 d)</div>
      <div class="card-value" id="c-count">—</div>
      <div class="card-sub">moisture reports</div>
    </div>
  </div>
  <div class="section">
    <div class="section-label">Moisture — last 7 days</div>
    <canvas id="chart" height="160"></canvas>
  </div>
  <div class="section">
    <div class="section-label">Event log</div>
    <div id="log"></div>
  </div>
  <div class="actions">
    <button class="btn" onclick="sendWater()">Water now (22 s)</button>
    <span class="status-note" id="water-note"></span>
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
let chart = null;

function timeSince(iso) {
  const s = Math.floor((Date.now() - new Date(iso)) / 1000);
  if (s < 120)    return s + ' s ago';
  if (s < 7200)   return Math.floor(s / 60) + ' min ago';
  if (s < 172800) return Math.floor(s / 3600) + ' h ago';
  return Math.floor(s / 86400) + ' d ago';
}

function fmtTs(iso) {
  return new Date(iso).toLocaleString('no-NO', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function updateCards(events) {
  const moisture = events.filter(e => e.kind === 'moisture');
  const pumps    = events.filter(e => e.kind === 'pump');
  const cutoff   = Date.now() - 7 * 86400e3;

  document.getElementById('c-count').textContent = moisture.filter(e => new Date(e.timestamp) > cutoff).length;

  if (moisture.length) {
    const last = moisture[moisture.length - 1];
    document.getElementById('c-moisture').textContent = last.data.adc_raw.toLocaleString('no-NO');
    document.getElementById('c-moisture-sub').textContent = timeSince(last.timestamp);
    const online = (Date.now() - new Date(last.timestamp)) < 2 * 3600e3;
    const pill = document.getElementById('pill');
    pill.className = 'pill ' + (online ? 'online' : 'offline');
    document.getElementById('pill-text').textContent = online ? 'online' : 'offline';
  }

  if (pumps.length) {
    const last = pumps[pumps.length - 1];
    document.getElementById('c-watered').textContent = timeSince(last.timestamp);
    document.getElementById('c-watered-sub').textContent = last.data.duration_s + ' s';
  }
}

function renderChart(events) {
  const pts = events
    .filter(e => e.kind === 'moisture' && new Date(e.timestamp) > Date.now() - 7 * 86400e3)
    .slice(-48);
  const labels = pts.map(e => fmtTs(e.timestamp));
  const data   = pts.map(e => e.data.adc_raw);
  if (!chart) {
    chart = new Chart(document.getElementById('chart'), {
      type: 'line',
      data: { labels, datasets: [{ data, borderColor: '#378ADD', backgroundColor: 'rgba(55,138,221,0.08)', borderWidth: 1.5, pointRadius: 3, pointBackgroundColor: '#378ADD', fill: true, tension: 0.3 }] },
      options: { responsive: true, plugins: { legend: { display: false } }, scales: {
        x: { ticks: { color: '#888780', font: { size: 11 }, maxTicksLimit: 7 } },
        y: { min: 0, max: 4095, ticks: { color: '#888780', font: { size: 11 } } }
      }}
    });
  } else {
    chart.data.labels = labels;
    chart.data.datasets[0].data = data;
    chart.update();
  }
}

function renderLog(events) {
  const el = document.getElementById('log');
  if (!events.length) { el.innerHTML = '<span class="empty">No events yet.</span>'; return; }
  el.innerHTML = [...events].reverse().slice(0, 50).map(e => {
    const badge = e.kind === 'pump' ? 'badge-pump' : 'badge-moisture';
    const msg   = e.kind === 'moisture'
      ? 'adc_raw ' + e.data.adc_raw.toLocaleString('no-NO')
      : 'ran ' + e.data.duration_s + ' s';
    return '<div class="log-row"><span class="log-time">' + fmtTs(e.timestamp) + '</span>'
      + '<span class="log-badge ' + badge + '">' + e.kind + '</span>'
      + '<span class="log-msg">' + msg + '</span></div>';
  }).join('');
}

async function loadData() {
  try {
    const r = await fetch('/api/data');
    const d = await r.json();
    updateCards(d.events);
    renderChart(d.events);
    renderLog(d.events);
  } catch(e) { console.error(e); }
}

async function sendWater() {
  const note = document.getElementById('water-note');
  note.textContent = 'sending…';
  try {
    const r = await fetch('/api/water', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({duration_s: 22}) });
    note.textContent = r.ok ? 'command sent' : 'no ESP32 connected';
  } catch { note.textContent = 'error'; }
  setTimeout(() => note.textContent = '', 3000);
}

loadData();
setInterval(loadData, 60000);
</script>
</body>
</html>"####;
