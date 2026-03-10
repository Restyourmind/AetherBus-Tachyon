pub const OP_ENCODE_FRAME: u8 = 1;
pub const OP_COMPRESS_LZ4: u8 = 3;
pub const OP_DECOMPRESS_LZ4: u8 = 4;

pub const STATUS_OK: u16 = 0;
pub const STATUS_ERR: u16 = 1;

pub const REQUEST_HEADER_LEN: usize = 8;
pub const RESPONSE_HEADER_LEN: usize = 6;

pub const MAX_REQUEST_BODY: usize = 64 * 1024 * 1024;
