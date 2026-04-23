# Principal Engineer Review Notes

วันที่รีวิว: 2026-04-11 (UTC)
ขอบเขต: `internal/delivery/zmq`, `pkg/client`, และเอกสาร protocol ที่เกี่ยวข้อง

## 1) ประเด็นสำคัญ (ต้องแก้ก่อน merge)

1. **Context/timeout contract ของ client ไม่ถูกบังคับใช้จริง**
   - `Publish(ctx, ...)` ไม่ใช้ `ctx` เลย และ `Options.Timeout` ก็ไม่ได้ถูกใช้กับ socket (`SNDTIMEO/RCVTIMEO`) ทำให้ API contract สื่อว่า cancel/timeout ได้ แต่ runtime จริง block ได้ไม่จำกัดเวลา.
   - ผลกระทบ: reliability และ idempotency ฝั่ง caller จัดการยาก (เกิด retry ซ้อน/duplicate publish โดยไม่รู้ผลลัพธ์ครั้งแรก).
   - จุดอ้างอิง: `pkg/client/publisher.go`, `pkg/client/options.go`, `pkg/client/client.go`.

2. **Control plane ไม่มีการยืนยันตัวตน/authorization ของ ACK/NACK/register**
   - `handleControl` รับคำสั่ง `consumer.register`, `ack`, `nack` จาก payload JSON โดยไม่มี signature/token/session binding เพิ่มเติมนอกจาก `consumerID/sessionID`.
   - ผู้ส่งที่รู้ `message_id + consumer_id` อาจ spoof ACK/NACK ได้ ถ้า network boundary ไม่ป้องกันแน่นพอ.
   - จุดอ้างอิง: `internal/delivery/zmq/router.go`.

3. **Error handling ใน Router startup มีโอกาส resource leak เมื่อ fail กลางทาง**
   - เมื่อสร้าง socket/bind ล้มเหลวบางจุด ไม่มี cleanup ครบทุก path (เช่นสร้าง ROUTER สำเร็จแต่ PUB ล้มเหลว, หรือ bind pub fail).
   - ผลกระทบ: leak FD/socket ในการ restart/retry startup loop.
   - จุดอ้างอิง: `internal/delivery/zmq/router.go` (`Start`).

## 2) ประเด็นควรปรับ (คุณภาพ/หนี้เทคนิค)

1. **Protocol/contract drift กับ spec เรื่อง version enforcement**
   - Spec ระบุว่า broker *MUST reject unsupported protocol versions* แต่เส้นทางรับ event ปัจจุบัน decode แล้ว route โดยไม่เห็นการตรวจ `spec_version` ก่อนใช้งาน.
   - ควรระบุชัดเจนว่าจะ enforce ที่ layer ไหน (codec/domain/usecase/router) เพื่อป้องกัน incompatibility เงียบ.
   - จุดอ้างอิง: `docs/PROTOCOL.md`, `internal/delivery/zmq/router.go`.

2. **Observability ยังใช้ `fmt.Printf` กระจายหลายจุด**
   - ปัจจุบันมี event exporter แล้ว แต่ error log หลักจำนวนมากยังเป็น plain text/JSON string print และไม่มีระดับ log/trace correlation ที่คงที่.
   - ควรย้ายเป็น structured logger เดียวกัน พร้อม fields มาตรฐาน (`tenant_id`, `message_id`, `attempt`, `session_id`, `reason`, `route_type`).

3. **ประสิทธิภาพเสี่ยง O(n log n) ต่อ message ใน hot path**
   - `selectSession` สร้าง+sort รายชื่อ session ทุกครั้งที่ dispatch; `sortPriorityQueueLocked` sort ทั้ง queue ทุก enqueue.
   - เมื่อจำนวน consumer/queue โต จะเพิ่ม latency tail อย่างมีนัย.

4. **Backpressure policy ยังไม่มี guardrail ด้าน fairness/tenant isolation เชิงลึก**
   - มี tenant quota พื้นฐานแล้ว แต่ policy ปรับ global inflight/queue อาจทำให้ noisy tenant กระทบกลุ่มอื่น.
   - ควรเพิ่ม per-tenant adaptive limits หรือ weighted-fair scheduling.

## 3) ข้อเสนอแนะต่อยอด (roadmap สั้น)

1. **Hardening reliability baseline (สัปดาห์นี้)**
   - บังคับ timeout/cancel path ใน client + retry policy ที่แยก retryable/non-retryable.
   - เพิ่ม startup cleanup guarantees และ explicit shutdown handshake.

2. **Security + protocol conformance (สั้น-กลาง)**
   - เพิ่ม authenticated control message (mTLS identity binding หรือ signed control envelope).
   - เพิ่ม protocol gate: validate `spec_version`, required headers, และ reject reason code ที่ชัดเจน.

3. **Performance/observability (กลาง)**
   - ทำ benchmark scenario ที่มี consumer 1k+ และ queued backlog สูง.
   - เปลี่ยน hot path เป็น heap/priority data structure, ลด full sort.
   - ติด metric histogram: dispatch latency, queue wait, ack RTT, retry age.

## 4) รายการ test ที่แนะนำ

1. `pkg/client/client_test.go`
   - `TestPublish_RespectsContextDeadline`
   - `TestPublish_AppliesSocketTimeoutFromOptions`
   - `TestSubscribe_StopsPromptlyOnContextCancel`

2. `internal/delivery/zmq/router_frames_test.go`
   - `TestHandleControl_RejectsUnauthenticatedAckNack` (หลังเพิ่ม auth contract)
   - `TestStart_CleansUpSocketsOnPubBindFailure`
   - `TestParseFrames_RejectsUnexpectedProtocolVersionEnvelope` (หรือย้ายไป suite ที่เหมาะกว่า)

3. `internal/delivery/zmq/router_test.go` (เพิ่มไฟล์ใหม่ได้)
   - `TestSelectSession_PerfBaseline_1000Consumers` (benchmark)
   - `BenchmarkDispatchDirect_WithLargeDeferredQueue`
   - `TestQueuePolicy_FairnessAcrossTenants`

4. `internal/usecase/intent_core_test.go` หรือ suite ingress
   - `TestIngress_RejectUnsupportedSpecVersion`
   - `TestIngress_RejectPastDeliverAtWithDeterministicReason`

