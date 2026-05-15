use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;
use std::thread;
use chrono::Local;
use axum::{Router, routing::{get, post}, extract::State, Json};
use axum::http::StatusCode;
use axum::response::Html;
mod config;
mod types;
use config::{BIND_ADDR, HTTP_ADDR, LIVENESS_SECS, MAX_EVENTS};
use types::{ApiData, LogEntry, Message, WaterRequest};

struct SharedState {
    log:     Mutex<Vec<LogEntry>>,
    node_tx: Mutex<Option<mpsc::Sender<u16>>>, // None when no node is connected
}

// --- Node communication (TCP) ---

fn node_listener(state: Arc<SharedState>) {
    let listener = TcpListener::bind(BIND_ADDR).expect("TCP bind failed");
    println!("TCP  listening on {BIND_ADDR}");

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
                let mut log = state.log.lock().unwrap();
                log.push(LogEntry { timestamp: Local::now().to_rfc3339(), message: msg });
                if log.len() > MAX_EVENTS { log.remove(0); }
            }
            Err(e) => eprintln!("decode error ({len} bytes): {e}"),
        }
    }
}

// --- Dashboard API (HTTP) ---

async fn serve_dashboard() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

async fn get_event_log(State(state): State<Arc<SharedState>>) -> Json<ApiData> {
    let log = state.log.lock().unwrap().clone();
    let online = log.iter()
        .filter(|e| matches!(e.message, Message::Moisture { .. }))
        .last()
        .and_then(|e| chrono::DateTime::parse_from_rfc3339(&e.timestamp).ok())
        .map(|t| (chrono::Local::now() - t.with_timezone(&chrono::Local)).num_seconds().unsigned_abs() < LIVENESS_SECS)
        .unwrap_or(false);
    Json(ApiData { log, online })
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
    let state = Arc::new(SharedState {
        log:     Mutex::new(Vec::new()),
        node_tx: Mutex::new(None),
    });

    let state_node = Arc::clone(&state);
    thread::spawn(move || node_listener(state_node));

    let app = Router::new()
        .route("/", get(serve_dashboard))
        .route("/api/data", get(get_event_log))
        .route("/api/water", post(post_water_command))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(HTTP_ADDR).await.unwrap();
    println!("HTTP dashboard on http://{HTTP_ADDR}");
    axum::serve(listener, app).await.unwrap();
}

const DASHBOARD_HTML: &str = r#####"<!DOCTYPE html>
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
.cards-top    { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 12px; }
.cards-bottom { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-bottom: 1.5rem; }
.card { background: #ebebea; border-radius: 8px; padding: 1rem; }
.card-label { font-size: 13px; color: #5f5e5a; margin-bottom: 6px; }
.card-value { font-size: 24px; font-weight: 500; }
.card-sub { font-size: 12px; color: #888780; margin-top: 4px; }
.btn-tile { border-radius: 8px; overflow: hidden; }
.btn-tile button { display: block; width: 100%; min-height: 72px; border: none; border-radius: 8px; font-size: 14px; font-weight: 500; cursor: pointer; font-family: inherit; }
.btn-water button { background: #e1f5ee; color: #27500a; }
.btn-water button:hover { background: #9fe1cb; }
.btn-test button  { background: #d3d1c7; color: #444441; border: none; }
.btn-test button:hover  { background: #b4b2a9; }
.section { background: white; border: 1px solid #e8e8e4; border-radius: 12px; padding: 1.25rem; margin-bottom: 1.5rem; }
.section-label { font-size: 13px; color: #5f5e5a; font-weight: 500; margin-bottom: 12px; }
.log-row { display: flex; align-items: center; gap: 12px; padding: 8px 0; border-bottom: 1px solid #f1efe8; font-size: 13px; }
.log-row:last-child { border-bottom: none; }
.log-time { color: #888780; min-width: 130px; font-variant-numeric: tabular-nums; }
.log-badge { font-size: 11px; padding: 2px 8px; border-radius: 6px; font-weight: 500; min-width: 64px; text-align: center; }
.badge-moisture { background: #eaf3de; color: #3b6d11; }
.badge-pump { background: #e1f5ee; color: #0f6e56; }
.log-msg { color: #1a1a18; }
.empty { color: #888780; font-size: 13px; }
</style>
</head>
<body>
<div class="container">
  <div class="topbar">
    <span class="title">Yucca palm</span>
    <span class="pill offline" id="pill"><span class="dot"></span><span id="pill-text">offline</span></span>
  </div>
  <div class="cards-top">
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
  <div class="cards-bottom">
    <div class="btn-tile btn-water">
      <button onclick="sendWater(22)">Water now (22 s)</button>
    </div>
    <div class="btn-tile btn-test">
      <button onclick="sendWater(3)">Test pump (3 s)</button>
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

function fmtTs(ms) {
  return new Date(ms).toLocaleString('no-NO', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}

function updateCards(log) {
  const moisture = log.filter(e => e.message.Moisture);
  const pumps    = log.filter(e => e.message.Pump);
  const cutoff   = Date.now() - 7 * 86400e3;

  document.getElementById('c-count').textContent = moisture.filter(e => new Date(e.timestamp) > cutoff).length;

  if (moisture.length) {
    const last = moisture[moisture.length - 1];
    document.getElementById('c-moisture').textContent = last.message.Moisture.adc_raw.toLocaleString('no-NO');
    document.getElementById('c-moisture-sub').textContent = timeSince(last.timestamp);
    const online = (Date.now() - new Date(last.timestamp)) < 2 * 3600e3;
    const pill = document.getElementById('pill');
    pill.className = 'pill ' + (online ? 'online' : 'offline');
    document.getElementById('pill-text').textContent = online ? 'online' : 'offline';
  }

  if (pumps.length) {
    const last = pumps[pumps.length - 1];
    document.getElementById('c-watered').textContent = timeSince(last.timestamp);
    document.getElementById('c-watered-sub').textContent = last.message.Pump.duration_s + ' s';
  }
}

function renderChart(log) {
  const now      = Date.now();
  const sevenDaysAgo = now - 7 * 86400e3;
  const pts = log.filter(e => e.message.Moisture && new Date(e.timestamp) > sevenDaysAgo).slice(-48);
  const data = pts.map(e => ({ x: new Date(e.timestamp).getTime(), y: e.message.Moisture.adc_raw }));
  const xMin = pts.length ? Math.max(new Date(pts[0].timestamp).getTime(), sevenDaysAgo) : sevenDaysAgo;

  if (!chart) {
    chart = new Chart(document.getElementById('chart'), {
      type: 'line',
      data: { datasets: [{ data, borderColor: '#639922', backgroundColor: 'rgba(99,153,34,0.08)', borderWidth: 1.5, pointRadius: 3, pointBackgroundColor: '#639922', fill: true, tension: 0.3 }] },
      options: { responsive: true, parsing: false, plugins: { legend: { display: false } }, scales: {
        x: { type: 'linear', min: xMin, max: now, ticks: { color: '#888780', font: { size: 11 }, maxTicksLimit: 7, callback: v => fmtTs(v) } },
        y: { min: 0, max: 4095, ticks: { color: '#888780', font: { size: 11 } } }
      }}
    });
  } else {
    chart.data.datasets[0].data = data;
    chart.options.scales.x.min = xMin;
    chart.options.scales.x.max = now;
    chart.update();
  }
}

function renderLog(log) {
  const el = document.getElementById('log');
  if (!log.length) { el.innerHTML = '<span class="empty">No events yet.</span>'; return; }
  el.innerHTML = [...log].reverse().slice(0, 50).map(e => {
    const isMoisture = !!e.message.Moisture;
    const badge = isMoisture ? 'badge-moisture' : 'badge-pump';
    const kind  = isMoisture ? 'moisture' : 'pump';
    const msg   = isMoisture
      ? 'adc_raw ' + e.message.Moisture.adc_raw.toLocaleString('no-NO')
      : 'ran ' + e.message.Pump.duration_s + ' s';
    return '<div class="log-row"><span class="log-time">' + fmtTs(new Date(e.timestamp).getTime()) + '</span>'
      + '<span class="log-badge ' + badge + '">' + kind + '</span>'
      + '<span class="log-msg">' + msg + '</span></div>';
  }).join('');
}

async function loadData() {
  try {
    const r = await fetch('/api/data');
    const d = await r.json();
    updateCards(d.log);
    renderChart(d.log);
    renderLog(d.log);
  } catch(e) { console.error(e); }
}

async function sendWater(duration_s) {
  await fetch('/api/water', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({duration_s}) });
}

loadData();
setInterval(loadData, 60000);
</script>
</body>
</html>"#####;
