use super::InternalEvent;
use metrics::counter;
use prost::DecodeError;

#[derive(Debug)]
pub struct VectorEventReceived {
    pub byte_size: usize,
}

impl InternalEvent for VectorEventReceived {
    fn emit_logs(&self) {
        trace!(message = "Received one event.");
    }

    fn emit_metrics(&self) {
        counter!("processed_events_total", 1);
        counter!("processed_bytes_total", self.byte_size as u64);
    }
}

#[derive(Debug)]
pub struct VectorProtoDecodeError {
    pub error: DecodeError,
}

impl InternalEvent for VectorProtoDecodeError {
    fn emit_logs(&self) {
        error!(message = "Failed to decode protobuf message.", error = ?self.error, internal_log_rate_secs = 10);
    }

    fn emit_metrics(&self) {
        counter!("protobuf_decode_errors_total", 1);
    }
}
