use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const MAGIC_BYTES: u16 = 0xAE7B;
pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 10;

pub const FLAG_COMPRESSED: u8 = 0b0000_0001;
#[allow(dead_code)]
pub const FLAG_CHUNKED: u8 = 0b0000_0010;

#[derive(Debug, Clone, Copy)]
pub struct HeaderView {
    pub flags: u8,
    pub topic_len: usize,
    pub payload_len: usize,
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub flags: u8,
    pub topic: Bytes,
    pub payload: Bytes,
}

impl Frame {
    pub fn encode(&self, out: &mut BytesMut) {
        let topic_len = self.topic.len();
        let payload_len = self.payload.len();

        out.reserve(HEADER_LEN + topic_len + payload_len);
        out.put_u16(MAGIC_BYTES);
        out.put_u8(VERSION);
        out.put_u8(self.flags);
        out.put_u16(topic_len as u16);
        out.put_u32(payload_len as u32);
        out.extend_from_slice(&self.topic);
        out.extend_from_slice(&self.payload);
    }

    pub fn try_decode(buf: &mut BytesMut) -> Result<Option<Self>, &'static str> {
        let Some(header_view) = Self::peek_header(buf)? else {
            return Ok(None);
        };

        let frame_len = HEADER_LEN + header_view.topic_len + header_view.payload_len;
        if buf.len() < frame_len {
            return Ok(None);
        }

        // Now consume exactly one full frame.
        let mut frame_buf = buf.split_to(frame_len);

        let _magic = frame_buf.get_u16();
        let _version = frame_buf.get_u8();
        let flags = frame_buf.get_u8();
        let topic_len = frame_buf.get_u16() as usize;
        let payload_len = frame_buf.get_u32() as usize;

        let topic = frame_buf.split_to(topic_len).freeze();
        let payload = frame_buf.split_to(payload_len).freeze();

        Ok(Some(Frame {
            flags,
            topic,
            payload,
        }))
    }

    pub fn peek_header(buf: &BytesMut) -> Result<Option<HeaderView>, &'static str> {
        if buf.len() < HEADER_LEN {
            return Ok(None);
        }

        // Peek header first without consuming the original buffer.
        let mut header = &buf[..HEADER_LEN];
        let magic = header.get_u16();
        if magic != MAGIC_BYTES {
            return Err("invalid magic bytes");
        }

        let version = header.get_u8();
        if version != VERSION {
            return Err("unsupported version");
        }

        let flags = header.get_u8();
        let topic_len = header.get_u16() as usize;
        let payload_len = header.get_u32() as usize;

        Ok(Some(HeaderView {
            flags,
            topic_len,
            payload_len,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_does_not_consume_on_partial_frame() {
        let frame = Frame {
            flags: FLAG_COMPRESSED,
            topic: Bytes::from_static(b"t"),
            payload: Bytes::from_static(b"payload"),
        };

        let mut encoded = BytesMut::new();
        frame.encode(&mut encoded);

        let partial = encoded[..HEADER_LEN + 2].to_vec();
        let mut buf = BytesMut::from(&partial[..]);

        let before = buf.len();
        let decoded = Frame::try_decode(&mut buf).expect("decode should not error");
        assert!(decoded.is_none());
        assert_eq!(buf.len(), before);

        buf.extend_from_slice(&encoded[before..]);
        let decoded = Frame::try_decode(&mut buf)
            .expect("decode should not error")
            .expect("frame should decode");

        assert_eq!(decoded.flags, FLAG_COMPRESSED);
        assert_eq!(&decoded.topic[..], b"t");
        assert_eq!(&decoded.payload[..], b"payload");
        assert!(buf.is_empty());
    }

    #[test]
    fn peek_header_reads_lengths_without_consuming_payload() {
        let frame = Frame {
            flags: FLAG_COMPRESSED | FLAG_CHUNKED,
            topic: Bytes::from_static(b"media.video.chunk"),
            payload: Bytes::from_static(b"abcdef"),
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);

        let before = buf.len();
        let header = Frame::peek_header(&buf)
            .expect("peek should not error")
            .expect("header should exist");

        assert_eq!(header.flags, FLAG_COMPRESSED | FLAG_CHUNKED);
        assert_eq!(header.topic_len, frame.topic.len());
        assert_eq!(header.payload_len, frame.payload.len());
        assert_eq!(buf.len(), before);
    }
}
