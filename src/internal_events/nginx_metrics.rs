use super::InternalEvent;
use crate::sources::nginx_metrics::parser::ParseError;
use metrics::{counter, histogram};
use std::time::Instant;

#[derive(Debug)]
pub struct NginxMetricsCollectCompleted {
    pub start: Instant,
    pub end: Instant,
}

impl InternalEvent for NginxMetricsCollectCompleted {
    fn emit_logs(&self) {
        debug!(message = "Collection completed.");
    }

    fn emit_metrics(&self) {
        counter!("collect_completed_total", 1);
        histogram!("collect_duration_nanoseconds", self.end - self.start);
    }
}

pub struct NginxMetricsRequestError<'a> {
    pub error: crate::Error,
    pub endpoint: &'a str,
}

impl<'a> InternalEvent for NginxMetricsRequestError<'a> {
    fn emit_logs(&self) {
        error!(message = "Nginx request error.", endpoint = %self.endpoint, error = ?self.error)
    }

    fn emit_metrics(&self) {
        counter!("http_request_errors_total", 1);
    }
}

pub struct NginxMetricsStubStatusParseError<'a> {
    pub error: ParseError,
    pub endpoint: &'a str,
}

impl<'a> InternalEvent for NginxMetricsStubStatusParseError<'a> {
    fn emit_logs(&self) {
        error!(message = "NginxStubStatus parse error.", endpoint = %self.endpoint, error = ?self.error)
    }

    fn emit_metrics(&self) {
        counter!("parse_errors_total", 1);
    }
}
