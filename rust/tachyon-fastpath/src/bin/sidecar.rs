use bytes::{Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

#[path = "../frame.rs"]
mod frame;
use frame::Frame;

const OP_ENCODE_FRAME: u8 = 1;
const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;

#[tokio::main]
async fn main() -> io::Result<()> {
    let path = "/tmp/tachyon-fastpath.sock";
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = handle(stream).await {
                eprintln!("sidecar client error: {err}");
            }
        });
    }
}

async fn handle(mut stream: UnixStream) -> io::Result<()> {
    loop {
        let mut header = [0u8; 8];
        if stream.read_exact(&mut header).await.is_err() {
            return Ok(());
        }

        let op = header[0];
        let flags = header[1];
        let topic_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let payload_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

        let body_len = topic_len + payload_len;
        if body_len > MAX_REQUEST_BODY {
            write_err(&mut stream, b"request too large").await?;
            continue;
        }

        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;

        let topic = Bytes::copy_from_slice(&body[..topic_len]);
        let payload = Bytes::copy_from_slice(&body[topic_len..]);

        match op {
            OP_ENCODE_FRAME => {
                let frame = Frame {
                    flags,
                    topic,
                    payload,
                };
                let mut out = BytesMut::new();
                frame.encode(&mut out);
                write_ok(&mut stream, &out).await?;
            }
            _ => {
                write_err(&mut stream, b"unsupported opcode").await?;
            }
        }
    }
}

async fn write_ok(stream: &mut UnixStream, body: &[u8]) -> io::Result<()> {
    let mut hdr = [0u8; 6];
    hdr[0..2].copy_from_slice(&0u16.to_be_bytes());
    hdr[2..6].copy_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(body).await
}

async fn write_err(stream: &mut UnixStream, body: &[u8]) -> io::Result<()> {
    let mut hdr = [0u8; 6];
    hdr[0..2].copy_from_slice(&1u16.to_be_bytes());
    hdr[2..6].copy_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(body).await
}
