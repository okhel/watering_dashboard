//! Garden dashboard — TCP server that receives moisture/pump messages
//! from the ESP32 node and logs them to stdout.
//!
//! Wire framing: big-endian u16 length + JSON payload.

use std::io::Read;
use std::net::{TcpListener, TcpStream};

mod config;
mod types;
use config::BIND_ADDR;
use types::Message;

fn handle(mut stream: TcpStream) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;
    println!("[{}] connected: {peer}", chrono::Utc::now().to_rfc3339());

    let mut len_buf = [0u8; 2];
    loop {
        // Frame length. read_exact is required: TcpStream::read may return
        // a short read and would otherwise desync the framing.
        stream.read_exact(&mut len_buf)?;
        let len = u16::from_be_bytes(len_buf) as usize;

        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload)?;

        match serde_json::from_slice::<Message>(&payload) {
            Ok(msg) => {
                let ts = chrono::Utc::now().to_rfc3339();
                println!("[{ts}] {msg:?}");
            }
            Err(e) => {
                eprintln!("decode error ({} bytes): {e}", payload.len());
                // Keep the connection: a single bad frame shouldn't drop
                // the link. If framing is wrong we'll EOF on the next read.
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind(BIND_ADDR)?;
    println!("listening on {BIND_ADDR}");

    for stream in listener.incoming() {
        match stream {
            Ok(s) => {
                if let Err(e) = handle(s) {
                    eprintln!("connection closed: {e}");
                }
            }
            Err(e) => eprintln!("accept error: {e}"),
        }
    }
    Ok(())
}
