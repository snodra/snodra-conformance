//! The pure parts of the matrix: which pairs talk, and how a `traceparent` reads.

/// Every ordered pair (sender, receiver) of two different languages, in a stable order.
pub fn pairs(langs: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for x in langs {
        for y in langs {
            if x != y {
                out.push((x.clone(), y.clone()));
            }
        }
    }
    out
}

/// The trace id of a W3C `traceparent` (`<version>-<trace id>-<parent id>-<flags>`), or `None`
/// when the value is not valid trace context: lowercase hex of 2, 32, 16 and 2 digits, version
/// not `ff`, trace and parent ids not all zero, and exactly four fields for version `00`.
pub fn trace_id(traceparent: &str) -> Option<&str> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    let [version, trace, parent, flags, rest @ ..] = parts.as_slice() else {
        return None;
    };
    let hex = |s: &str, n: usize| {
        s.len() == n && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    };
    let nonzero = |s: &str| s.bytes().any(|b| b != b'0');
    let valid = hex(version, 2)
        && *version != "ff"
        && (*version != "00" || rest.is_empty())
        && hex(trace, 32)
        && nonzero(trace)
        && hex(parent, 16)
        && nonzero(parent)
        && hex(flags, 2);
    valid.then_some(*trace)
}

/// A `traceparent` of `trace` with a fresh parent id (a new span of the same trace).
pub fn traceparent(trace: &str) -> String {
    let parent = &uuid::Uuid::new_v4().simple().to_string()[..16];
    format!("00-{trace}-{parent}-01")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn langs(l: &[&str]) -> Vec<String> {
        l.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn every_ordered_pair_once() {
        assert_eq!(pairs(&langs(&["go", "rust", "kotlin"])).len(), 6);
    }

    #[test]
    fn a_pair_never_joins_a_language_to_itself() {
        assert!(
            pairs(&langs(&["go", "rust", "kotlin", "python"]))
                .iter()
                .all(|(x, y)| x != y)
        );
    }

    #[test]
    fn trace_id_of_a_valid_traceparent() {
        assert_eq!(
            trace_id("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(trace_id("garbage"), None);
    }

    #[test]
    fn trace_id_rejects_what_w3c_trace_context_rejects() {
        let ok = "4bf92f3577b34da6a3ce929d0e0e4736";
        let p = "00f067aa0ba902b7";
        for bad in [
            format!("00-{}-{p}-01", ok.to_uppercase()),
            format!("00-{ok}-{}-01", p.to_uppercase()),
            format!("ff-{ok}-{p}-01"),
            format!("00-{}-{p}-01", "0".repeat(32)),
            format!("00-{ok}-{}-01", "0".repeat(16)),
            format!("00-{ok}-{p}-01-extra"),
            format!("00-{}-{p}-01", &ok[..31]),
            format!("00-{ok}-{p}-0g"),
            format!("0-{ok}-{p}-01"),
        ] {
            assert_eq!(trace_id(&bad), None, "{bad}");
        }
        // A later version may add fields.
        assert_eq!(trace_id(&format!("01-{ok}-{p}-01-extra")), Some(ok));
    }

    #[test]
    fn a_fresh_traceparent_keeps_the_trace_and_changes_the_parent() {
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        let (a, b) = (traceparent(trace), traceparent(trace));
        assert_eq!(trace_id(&a), Some(trace));
        assert_ne!(a, b);
    }
}
