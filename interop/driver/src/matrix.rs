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

/// The trace id of a W3C `traceparent` (`00-<32 hex>-<16 hex>-<2 hex>`), or `None` when the
/// value is not well formed.
pub fn trace_id(traceparent: &str) -> Option<&str> {
    let parts: Vec<&str> = traceparent.split('-').collect();
    let [version, trace, parent, flags] = parts.as_slice() else {
        return None;
    };
    let hex = |s: &str, n: usize| s.len() == n && s.bytes().all(|b| b.is_ascii_hexdigit());
    if hex(version, 2) && hex(trace, 32) && hex(parent, 16) && hex(flags, 2) {
        Some(trace)
    } else {
        None
    }
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
    fn a_fresh_traceparent_keeps_the_trace_and_changes_the_parent() {
        let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
        let (a, b) = (traceparent(trace), traceparent(trace));
        assert_eq!(trace_id(&a), Some(trace));
        assert_ne!(a, b);
    }
}
