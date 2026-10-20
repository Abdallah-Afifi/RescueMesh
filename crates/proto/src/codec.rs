//! Framing and serialization.
//!
//! TCP frames are a 4-byte big-endian length followed by the body. Bodies are
//! `postcard` by default, or JSON when `run.wire_format = "json"` for
//! debugging. Every node and client in a run must use the same format.

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;
use std::io::{self, Read, Write};

/// Largest TCP frame body accepted. A larger length prefix means a corrupt
/// stream or a bug, and the connection is dropped instead of allocating.
pub const MAX_FRAME: usize = 16 * 1024 * 1024;

/// Largest heartbeat datagram, kept under a typical Ethernet MTU so it is
/// never fragmented.
pub const MAX_DATAGRAM: usize = 1200;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireFormat {
    #[default]
    Postcard,
    Json,
}

#[derive(Debug)]
pub enum CodecError {
    Io(io::Error),
    TooLarge(usize),
    Encode(String),
    Decode(String),
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CodecError::Io(e) => write!(f, "i/o error: {e}"),
            CodecError::TooLarge(n) => write!(f, "frame of {n} bytes exceeds the limit"),
            CodecError::Encode(e) => write!(f, "encode error: {e}"),
            CodecError::Decode(e) => write!(f, "decode error: {e}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<io::Error> for CodecError {
    fn from(e: io::Error) -> Self {
        CodecError::Io(e)
    }
}

pub fn encode<T: Serialize>(format: WireFormat, value: &T) -> Result<Vec<u8>, CodecError> {
    match format {
        WireFormat::Postcard => {
            postcard::to_stdvec(value).map_err(|e| CodecError::Encode(e.to_string()))
        }
        WireFormat::Json => {
            serde_json::to_vec(value).map_err(|e| CodecError::Encode(e.to_string()))
        }
    }
}

pub fn decode<T: DeserializeOwned>(format: WireFormat, bytes: &[u8]) -> Result<T, CodecError> {
    match format {
        WireFormat::Postcard => {
            postcard::from_bytes(bytes).map_err(|e| CodecError::Decode(e.to_string()))
        }
        WireFormat::Json => {
            serde_json::from_slice(bytes).map_err(|e| CodecError::Decode(e.to_string()))
        }
    }
}

/// Length-prefixed frame ready to write to a TCP stream.
pub fn encode_frame<T: Serialize>(format: WireFormat, value: &T) -> Result<Vec<u8>, CodecError> {
    let body = encode(format, value)?;
    if body.len() > MAX_FRAME {
        return Err(CodecError::TooLarge(body.len()));
    }
    let len = u32::try_from(body.len()).map_err(|_| CodecError::TooLarge(body.len()))?;
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    Ok(out)
}

/// Blocking read of one frame. Async code uses the same layout with its own
/// reader; [`frame_len`] validates the prefix.
pub fn read_frame<T: DeserializeOwned, R: Read>(
    format: WireFormat,
    r: &mut R,
) -> Result<T, CodecError> {
    let mut prefix = [0u8; 4];
    r.read_exact(&mut prefix)?;
    let len = frame_len(prefix)?;
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    decode(format, &body)
}

pub fn write_frame<T: Serialize, W: Write>(
    format: WireFormat,
    w: &mut W,
    value: &T,
) -> Result<(), CodecError> {
    w.write_all(&encode_frame(format, value)?)?;
    Ok(())
}

/// Body length from a 4-byte prefix, rejecting oversized frames.
pub fn frame_len(prefix: [u8; 4]) -> Result<usize, CodecError> {
    let len = u32::from_be_bytes(prefix) as usize;
    if len > MAX_FRAME {
        return Err(CodecError::TooLarge(len));
    }
    Ok(len)
}

/// Encode a heartbeat datagram, rejecting one that would be fragmented.
pub fn encode_datagram<T: Serialize>(format: WireFormat, value: &T) -> Result<Vec<u8>, CodecError> {
    let b = encode(format, value)?;
    if b.len() > MAX_DATAGRAM {
        return Err(CodecError::TooLarge(b.len()));
    }
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Category, Incident, IncidentStatus, OpResult, Plan, Severity, ZoneId};
    use crate::entry::{Entry, EntryKind, View};
    use crate::ids::*;
    use crate::msg::*;

    fn rid() -> ReqId {
        ReqId {
            client: ClientId {
                machine: 2,
                process: 7,
                user: 41,
            },
            incarnation: 9,
            seq: 1234,
        }
    }

    fn sample_messages() -> Vec<Envelope> {
        let incident = Incident {
            id: rid(),
            owner: rid().client,
            category: Category::Flooding,
            zone: ZoneId(12),
            severity: Severity(4),
            reported_at_ms: 1_760_000_000_000,
            status: IncidentStatus::Open,
            ttl_ms: 600_000,
            attachment: None,
        };
        let plan = Plan {
            incident: rid(),
            priority: -17,
            assignments: vec![],
            route: vec![ZoneId(12), ZoneId(3)],
        };
        let body = [
            NodeMsg::Ack,
            NodeMsg::Claim {
                req: rid(),
                view: ViewNo(3),
                key: ClaimKey {
                    score_step: 4,
                    tie: u64::MAX,
                    node: NodeId(2),
                },
            },
            NodeMsg::Propose(Proposal {
                req: rid(),
                epoch: Epoch(5),
                view: ViewNo(3),
                executor: NodeId(2),
                read_set: vec![(rid(), Version(77))],
                writes: vec![crate::domain::Write::PutIncident(incident)],
                result: OpResult::Planned(plan),
            }),
            NodeMsg::Append {
                epoch: Epoch(5),
                prev_seq: Seq(10),
                prev_epoch: Epoch(4),
                entries: vec![
                    Entry {
                        seq: Seq(11),
                        epoch: Epoch(5),
                        kind: EntryKind::View(View::new(ViewNo(4), vec![NodeId(1), NodeId(2)])),
                    },
                    Entry {
                        seq: Seq(12),
                        epoch: Epoch(5),
                        kind: EntryKind::Inject {
                            k: 3,
                            victim: NodeId(1),
                            duration_ms: 9000,
                            by_operator: false,
                        },
                    },
                ],
                commit: Seq(10),
            },
            NodeMsg::ProposeReply {
                req: rid(),
                outcome: ProposeOutcome::Stale { current: Seq(12) },
            },
            NodeMsg::Silence {
                target: SilenceTarget::CurrentRank1,
                duration_ms: 15_000,
            },
        ];
        body.into_iter()
            .enumerate()
            .map(|(i, body)| Envelope {
                version: PROTOCOL_VERSION,
                from: NodeId(1),
                msg_id: MsgId {
                    incarnation: 1,
                    n: i as u64,
                },
                ack_up_to: 0,
                body,
            })
            .collect()
    }

    #[test]
    fn frames_round_trip_in_both_formats() {
        for format in [WireFormat::Postcard, WireFormat::Json] {
            let mut buf = Vec::new();
            for m in sample_messages() {
                write_frame(format, &mut buf, &m).unwrap();
            }
            let mut r = &buf[..];
            for m in sample_messages() {
                let back: Envelope = read_frame(format, &mut r).unwrap();
                assert_eq!(back, m);
            }
            assert!(r.is_empty());
        }
    }

    #[test]
    fn oversized_prefix_is_rejected_without_allocating() {
        let mut r: &[u8] = &[0xff, 0xff, 0xff, 0xff];
        assert!(matches!(
            read_frame::<Envelope, _>(WireFormat::Postcard, &mut r),
            Err(CodecError::TooLarge(_))
        ));
    }

    #[test]
    fn heartbeat_fits_in_one_datagram() {
        let hb = Heartbeat {
            version: PROTOCOL_VERSION,
            from: NodeId(3),
            incarnation: u32::MAX,
            n: u64::MAX,
            epoch: Epoch(u64::MAX),
            coordinator: Some(NodeId(1)),
            view: ViewNo(u64::MAX),
            commit: Seq(u64::MAX),
            applied: Seq(u64::MAX),
            load: LoadSample {
                queue_len: u32::MAX,
                cpu_permille: 1000,
                service_ewma_us: u32::MAX,
            },
        };
        for format in [WireFormat::Postcard, WireFormat::Json] {
            let b = encode_datagram(format, &hb).unwrap();
            assert_eq!(decode::<Heartbeat>(format, &b).unwrap(), hb);
        }
    }
}
