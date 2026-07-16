//! Outcome classification (design §(e), spec: classification requirement).
//!
//! Every `do_get` outcome is exactly one of [`Outcome::Ok`],
//! [`Outcome::Unavailable`], or [`Outcome::Error`]. A gRPC `UNAVAILABLE` status is
//! attributed to the #2420 admission-shed signal (retry-safe by the server's
//! stated contract) and counted as `unavailable`; every other non-success status
//! or transport/decode failure is counted as `error`, tagged with its status code.
//!
//! Caveat (recorded in the record schema): a transport-layer `UNAVAILABLE` is
//! indistinguishable from an admission `UNAVAILABLE` on the wire — we attribute
//! `UNAVAILABLE` to admission by the server's #2420 contract and note it.

use arrow_flight::error::FlightError;
use tonic::{Code, Status};

/// The classified result of one `do_get` request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The request streamed to completion (its rows/bytes were drained).
    Ok,
    /// gRPC `UNAVAILABLE` — the #2420 admission shed (retry-safe).
    Unavailable,
    /// Any other non-success status or transport/decode failure. Carries the
    /// status-code label recorded under `error_codes`.
    Error(String),
}

/// Classify a `tonic::Status` (the error from the initial `do_get` call, or a
/// mid-stream `FlightError::Tonic`).
pub fn classify_status(status: &Status) -> Outcome {
    if status.code() == Code::Unavailable {
        Outcome::Unavailable
    } else {
        Outcome::Error(code_label(status.code()))
    }
}

/// Classify a `FlightError` surfaced while decoding the response stream: a
/// wrapped `tonic::Status` classifies as above; any other decode/transport
/// variant is an `error` tagged with a stable variant label.
pub fn classify_flight_error(err: &FlightError) -> Outcome {
    match err {
        FlightError::Tonic(status) => classify_status(status),
        FlightError::Arrow(_) => Outcome::Error("ArrowDecode".to_string()),
        FlightError::ProtocolError(_) => Outcome::Error("ProtocolError".to_string()),
        FlightError::DecodeError(_) => Outcome::Error("DecodeError".to_string()),
        FlightError::ExternalError(_) => Outcome::Error("ExternalError".to_string()),
        // `FlightError` is `#[non_exhaustive]`; any future variant is still an
        // `error` outcome (never silently `ok`).
        _ => Outcome::Error("OtherFlightError".to_string()),
    }
}

/// A stable, human-readable label for a gRPC status code, used as the
/// `error_codes` map key in the JSONL record.
fn code_label(code: Code) -> String {
    // `Code`'s Debug is the CamelCase variant name (`Internal`, `InvalidArgument`,
    // …) — a stable, template-friendly key.
    format!("{code:?}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unavailable_status_is_admission_shed() {
        let status = Status::unavailable("permit-wait timeout");
        assert_eq!(classify_status(&status), Outcome::Unavailable);
    }

    #[test]
    fn other_status_is_error_with_code() {
        assert_eq!(
            classify_status(&Status::internal("boom")),
            Outcome::Error("Internal".to_string())
        );
        assert_eq!(
            classify_status(&Status::invalid_argument("bad ticket")),
            Outcome::Error("InvalidArgument".to_string())
        );
        assert_eq!(
            classify_status(&Status::not_found("no table")),
            Outcome::Error("NotFound".to_string())
        );
    }

    #[test]
    fn flight_tonic_error_delegates_to_status_classification() {
        let shed = FlightError::Tonic(Status::unavailable("shed"));
        assert_eq!(classify_flight_error(&shed), Outcome::Unavailable);
        let other = FlightError::Tonic(Status::internal("mid-stream fault"));
        assert_eq!(
            classify_flight_error(&other),
            Outcome::Error("Internal".to_string())
        );
    }

    #[test]
    fn flight_decode_error_is_error_not_unavailable() {
        let err = FlightError::ProtocolError("truncated".to_string());
        assert_eq!(
            classify_flight_error(&err),
            Outcome::Error("ProtocolError".to_string())
        );
    }
}
