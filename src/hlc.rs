use std::fmt;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, SquirrelError};

/// Hybrid Logical Clock.
///
/// String representation: `{physical_ms:013x}-{logical:04x}-{node_id_hex}`
///
/// All components are zero-padded to fixed widths, so the string representation
/// is lexicographically sortable — DynamoDB and SQLite `ORDER BY hlc` work correctly
/// without any parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hlc {
    physical_ms: u64,
    logical: u16,
    node_id: [u8; 6],
}

impl Hlc {
    /// Create an HLC at the current wall-clock time with logical = 0.
    pub fn new(node_id: [u8; 6]) -> Self {
        Self { physical_ms: wall_clock_ms(), logical: 0, node_id }
    }

    /// Returns a new HLC that is strictly greater than `self` and no less than
    /// the current wall clock. Call this before every local write.
    pub fn tick(&self) -> Self {
        let wall_ms = wall_clock_ms();
        let physical = wall_ms.max(self.physical_ms);
        let logical = if physical == self.physical_ms {
            self.logical.saturating_add(1)
        } else {
            0
        };
        Self { physical_ms: physical, logical, node_id: self.node_id }
    }

    /// Merge with a remote HLC so the result is greater than both `self` and `remote`.
    /// Call this when receiving a record from another device to keep the local clock
    /// causally ahead of all known events.
    pub fn merge(&self, remote: &Hlc) -> Self {
        let wall_ms = wall_clock_ms();
        let physical = wall_ms.max(self.physical_ms).max(remote.physical_ms);
        let logical = if physical == self.physical_ms && physical == remote.physical_ms {
            self.logical.max(remote.logical).saturating_add(1)
        } else if physical == self.physical_ms {
            self.logical.saturating_add(1)
        } else if physical == remote.physical_ms {
            remote.logical.saturating_add(1)
        } else {
            0
        };
        Self { physical_ms: physical, logical, node_id: self.node_id }
    }

    pub fn physical_ms(&self) -> u64 { self.physical_ms }
    pub fn logical(&self) -> u16 { self.logical }
    pub fn node_id(&self) -> &[u8; 6] { &self.node_id }
}

fn wall_clock_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl fmt::Display for Hlc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:013x}-{:04x}-{}", self.physical_ms, self.logical, hex_encode(&self.node_id))
    }
}

impl FromStr for Hlc {
    type Err = SquirrelError;

    fn from_str(s: &str) -> Result<Self> {
        let err = || SquirrelError::InvalidHlc(s.to_string());
        let mut parts = s.splitn(3, '-');
        let phys_str = parts.next().ok_or_else(err)?;
        let log_str  = parts.next().ok_or_else(err)?;
        let node_str = parts.next().ok_or_else(err)?;

        let physical_ms = u64::from_str_radix(phys_str, 16).map_err(|_| err())?;
        let logical     = u16::from_str_radix(log_str,  16).map_err(|_| err())?;
        let bytes       = hex_decode(node_str).map_err(|_| err())?;
        if bytes.len() != 6 { return Err(err()); }

        let mut node_id = [0u8; 6];
        node_id.copy_from_slice(&bytes);
        Ok(Self { physical_ms, logical, node_id })
    }
}

impl PartialOrd for Hlc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Hlc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Numeric comparison matches string lex-order because all fields are
        // zero-padded to fixed widths in the Display impl.
        (self.physical_ms, self.logical, self.node_id)
            .cmp(&(other.physical_ms, other.logical, other.node_id))
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub(crate) fn hex_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    if s.len() % 2 != 0 { return Err(()); }
    (0..s.len()).step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_monotonic() {
        let node = [1u8; 6];
        let h0 = Hlc::new(node);
        let h1 = h0.tick();
        let h2 = h1.tick();
        assert!(h1 > h0, "second tick must exceed first");
        assert!(h2 > h1, "third tick must exceed second");
    }

    #[test]
    fn tick_increments_logical_when_wall_is_behind() {
        // A far-future physical_ms ensures wall_clock_ms() < physical_ms,
        // forcing the logical counter to increment instead of resetting.
        let node = [2u8; 6];
        let h = Hlc { physical_ms: 9_999_999_999_999, logical: 5, node_id: node };
        let next = h.tick();
        assert_eq!(next.physical_ms, h.physical_ms, "physical should be unchanged");
        assert_eq!(next.logical, 6, "logical should increment by 1");
    }

    #[test]
    fn display_fromstr_roundtrip() {
        let node = [0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56];
        let h = Hlc { physical_ms: 0x0001_2345_6789A, logical: 42, node_id: node };
        let s = h.to_string();
        let parsed: Hlc = s.parse().unwrap();
        assert_eq!(h, parsed);
    }

    #[test]
    fn string_order_matches_semantic_order() {
        let node = [0u8; 6];
        let h1 = Hlc { physical_ms: 1000, logical: 0, node_id: node };
        let h2 = Hlc { physical_ms: 1000, logical: 1, node_id: node };
        let h3 = Hlc { physical_ms: 1001, logical: 0, node_id: node };
        assert!(h1 < h2 && h2 < h3, "semantic order");
        assert!(h1.to_string() < h2.to_string(), "string order matches for logical");
        assert!(h2.to_string() < h3.to_string(), "string order matches for physical");
    }

    #[test]
    fn merge_exceeds_both_inputs() {
        let node = [3u8; 6];
        let local  = Hlc { physical_ms: 1000, logical: 5, node_id: node };
        let remote = Hlc { physical_ms: 1000, logical: 3, node_id: [4u8; 6] };
        let merged = local.merge(&remote);
        assert!(merged > local,  "merged must exceed local");
        assert!(merged > remote, "merged must exceed remote");
    }

    #[test]
    fn invalid_hlc_string_returns_error() {
        assert!("not-an-hlc".parse::<Hlc>().is_err());
        assert!("".parse::<Hlc>().is_err());
        assert!("fffff-0001-zzzzzzzzzzzz".parse::<Hlc>().is_err());
    }
}
