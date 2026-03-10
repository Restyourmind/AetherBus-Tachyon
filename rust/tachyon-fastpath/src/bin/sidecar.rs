use bytes::{Bytes, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

#[path = "../frame.rs"]
mod frame;
#[path = "../protocol.rs"]
mod protocol;

use frame::Frame;

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
        let mut header = [0u8; protocol::REQUEST_HEADER_LEN];
        if stream.read_exact(&mut header).await.is_err() {
            return Ok(());
        }

        let op = header[0];
        let flags = header[1];
        let topic_len = u16::from_be_bytes([header[2], header[3]]) as usize;
        let payload_len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

        let body_len = topic_len + payload_len;
        if body_len > protocol::MAX_REQUEST_BODY {
            write_err(&mut stream, b"request too large").await?;
            continue;
        }

        let mut body = vec![0u8; body_len];
        stream.read_exact(&mut body).await?;

        let topic = Bytes::copy_from_slice(&body[..topic_len]);
        let payload = Bytes::copy_from_slice(&body[topic_len..]);

        match op {
            protocol::OP_ENCODE_FRAME => {
                let frame = Frame {
                    flags,
                    topic,
                    payload,
                };
                let mut out = BytesMut::new();
                frame.encode(&mut out);
                write_ok(&mut stream, &out).await?;
            }
            protocol::OP_COMPRESS_LZ4 | protocol::OP_DECOMPRESS_LZ4 => {
                write_err(&mut stream, b"opcode scaffolded but not implemented").await?;
            }
            _ => {
                write_err(&mut stream, b"unsupported opcode").await?;
            }
        }
    }
}

async fn write_ok(stream: &mut UnixStream, body: &[u8]) -> io::Result<()> {
    let mut hdr = [0u8; protocol::RESPONSE_HEADER_LEN];
    hdr[0..2].copy_from_slice(&protocol::STATUS_OK.to_be_bytes());
    hdr[2..6].copy_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(body).await
}

async fn write_err(stream: &mut UnixStream, body: &[u8]) -> io::Result<()> {
    let mut hdr = [0u8; protocol::RESPONSE_HEADER_LEN];
    hdr[0..2].copy_from_slice(&protocol::STATUS_ERR.to_be_bytes());
    hdr[2..6].copy_from_slice(&(body.len() as u32).to_be_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(body).await
}
