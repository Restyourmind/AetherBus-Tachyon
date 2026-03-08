use bytes::{BufMut, BytesMut};
use std::{ptr, slice};

const MAGIC_BYTES: u16 = 0xAE7B;
const VERSION: u8 = 1;

#[repr(C)]
pub struct RustBuf {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
    pub code: i32,
}

#[no_mangle]
pub extern "C" fn tachyon_encode_frame(
    topic_ptr: *const u8,
    topic_len: usize,
    payload_ptr: *const u8,
    payload_len: usize,
    flags: u8,
) -> RustBuf {
    if (topic_len > 0 && topic_ptr.is_null()) || (payload_len > 0 && payload_ptr.is_null()) {
        return RustBuf {
            ptr: ptr::null_mut(),
            len: 0,
            cap: 0,
            code: 1,
        };
    }

    let topic = if topic_len == 0 {
        &[][..]
    } else {
        // SAFETY: pointer validity is checked above; data is only read during this call.
        unsafe { slice::from_raw_parts(topic_ptr, topic_len) }
    };
    let payload = if payload_len == 0 {
        &[][..]
    } else {
        // SAFETY: pointer validity is checked above; data is only read during this call.
        unsafe { slice::from_raw_parts(payload_ptr, payload_len) }
    };

    let mut out = BytesMut::with_capacity(10 + topic.len() + payload.len());
    out.put_u16(MAGIC_BYTES);
    out.put_u8(VERSION);
    out.put_u8(flags);
    out.put_u16(topic.len() as u16);
    out.put_u32(payload.len() as u32);
    out.extend_from_slice(topic);
    out.extend_from_slice(payload);

    let mut vec = out.to_vec();
    let buf = RustBuf {
        ptr: vec.as_mut_ptr(),
        len: vec.len(),
        cap: vec.capacity(),
        code: 0,
    };
    std::mem::forget(vec);
    buf
}

#[no_mangle]
pub extern "C" fn tachyon_free_buf(ptr: *mut u8, len: usize, cap: usize) {
    if ptr.is_null() {
        return;
    }
    // SAFETY: ptr/len/cap are produced by tachyon_encode_frame and must be freed once.
    unsafe {
        let _ = Vec::from_raw_parts(ptr, len, cap);
    }
}
