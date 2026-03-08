package fastpathffi

/*
#cgo LDFLAGS: -L${SRCDIR}/../../rust/tachyon-fastpath/target/release -ltachyon_fastpath
#include <stdint.h>
#include <stdlib.h>

typedef struct {
	uint8_t* ptr;
	size_t len;
	size_t cap;
	int32_t code;
} RustBuf;

RustBuf tachyon_encode_frame(
	const uint8_t* topic_ptr,
	size_t topic_len,
	const uint8_t* payload_ptr,
	size_t payload_len,
	uint8_t flags
);

void tachyon_free_buf(uint8_t* ptr, size_t len, size_t cap);
*/
import "C"
import (
	"fmt"
	"unsafe"
)

func EncodeFrame(topic string, payload []byte, flags byte) ([]byte, error) {
	topicBytes := []byte(topic)

	var topicPtr *C.uint8_t
	var payloadPtr *C.uint8_t

	if len(topicBytes) > 0 {
		topicPtr = (*C.uint8_t)(unsafe.Pointer(&topicBytes[0]))
	}
	if len(payload) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}

	out := C.tachyon_encode_frame(
		topicPtr,
		C.size_t(len(topicBytes)),
		payloadPtr,
		C.size_t(len(payload)),
		C.uint8_t(flags),
	)
	if out.code != 0 {
		return nil, fmt.Errorf("rust encode failed: code=%d", int(out.code))
	}
	defer C.tachyon_free_buf(out.ptr, out.len, out.cap)

	frame := C.GoBytes(unsafe.Pointer(out.ptr), C.int(out.len))
	return frame, nil
}
