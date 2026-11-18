//! The report: one verdict per check id, the level reached, the versions.

use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Pass,
    Fail,
    /// Not run for a stated capability reason (no real hub given, no retry policy).
    Skipped,
    /// A check id of the level the runner does not implement (with the reason).
    NotImplemented,
}

#[derive(Clone, Debug, Serialize)]
pub struct Verdict {
    pub id: String,
    pub status: Status,
    /// For a failure: what was expected and what was seen; for a skip: why; for a pass: notes.
    pub detail: String,
    pub millis: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub conformance_version: u32,
    pub runner_version: String,
    pub service: String,
    pub level: u32,
    /// Only some checks of the level were asked for (`--only`): the run never claims the level.
    pub partial: bool,
    /// No check failed and none is not implemented (skipped ones aside): the exit code is 0
    /// exactly then.
    pub passed: bool,
    /// Every check of the level ran and passed.
    pub complete: bool,
    /// The level proved: the level checked when the run is complete, else 0.
    pub level_reached: u32,
    pub checks: Vec<Verdict>,
}

/// A service-controlled text on one report line: control characters and line breaks escaped,
/// so it cannot forge lines of the report.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

impl Report {
    /// `partial`: the checks are a subset of the level's.
    pub fn new(service: &str, level: u32, partial: bool, checks: Vec<Verdict>) -> Report {
        let passed = checks
            .iter()
            .all(|c| c.status != Status::Fail && c.status != Status::NotImplemented);
        let complete = !partial && checks.iter().all(|c| c.status == Status::Pass);
        Report {
            conformance_version: crate::CONFORMANCE_VERSION,
            runner_version: crate::RUNNER_VERSION.to_string(),
            service: service.to_string(),
            level,
            partial,
            passed,
            complete,
            level_reached: if complete { level } else { 0 },
            checks,
        }
    }

    fn ids(&self, s: Status) -> Vec<&str> {
        self.checks
            .iter()
            .filter(|c| c.status == s)
            .map(|c| c.id.as_str())
            .collect()
    }

    /// One line of verdict about the level.
    pub fn summary(&self) -> String {
        let failed = self.ids(Status::Fail);
        let missing = self.ids(Status::NotImplemented);
        let skipped = self.ids(Status::Skipped);
        if self.partial {
            format!(
                "partial run ({} checks of level {}): no level claimed{}",
                self.checks.len(),
                self.level,
                if failed.is_empty() {
                    String::new()
                } else {
                    format!("; failed {failed:?}")
                }
            )
        } else if self.complete {
            format!("level {} reached", self.level)
        } else if !failed.is_empty() || !missing.is_empty() {
            format!(
                "level {} not reached: failed {failed:?}, not implemented {missing:?}",
                self.level
            )
        } else {
            format!("level {} not proved: skipped {skipped:?}", self.level)
        }
    }

    pub fn text(&self) -> String {
        let mut out = format!(
            "snodra-conformance {} (conformance version {}), service {}, level {}\n\n",
            self.runner_version,
            self.conformance_version,
            escape(&self.service),
            self.level
        );
        for c in &self.checks {
            let status = match c.status {
                Status::Pass => "PASS",
                Status::Fail => "FAIL",
                Status::Skipped => "SKIPPED",
                Status::NotImplemented => "NOT IMPLEMENTED",
            };
            out.push_str(&format!("{status:<16}{:<20}{:>7} ms", c.id, c.millis));
            if !c.detail.is_empty() {
                out.push_str(&format!("  {}", escape(&c.detail)));
            }
            out.push('\n');
        }
        let count = |s: Status| self.checks.iter().filter(|c| c.status == s).count();
        out.push_str(&format!(
            "\n{} passed, {} failed, {} skipped, {} not implemented: {}\n",
            count(Status::Pass),
            count(Status::Fail),
            count(Status::Skipped),
            count(Status::NotImplemented),
            self.summary()
        ));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(id: &str, status: Status) -> Verdict {
        Verdict {
            id: id.into(),
            status,
            detail: "line\nFAIL forged".into(),
            millis: 0,
        }
    }

    #[test]
    fn a_partial_run_never_claims_the_level() {
        let r = Report::new("s", 1, true, vec![v("L1-UNARY", Status::Pass)]);
        assert!(r.passed && !r.complete && r.level_reached == 0);
        assert!(r.summary().starts_with("partial run"));
    }

    #[test]
    fn skips_do_not_fail_the_run_but_leave_the_level_unproved() {
        let r = Report::new(
            "s",
            1,
            false,
            vec![v("A", Status::Pass), v("B", Status::Skipped)],
        );
        assert!(r.passed && r.level_reached == 0);
        assert!(r.summary().contains("not proved: skipped [\"B\"]"));
        let r = Report::new("s", 1, false, vec![v("A", Status::NotImplemented)]);
        assert!(!r.passed && r.summary().contains("not reached"));
    }

    #[test]
    fn service_text_cannot_forge_report_lines() {
        let r = Report::new("s", 1, false, vec![v("A", Status::Pass)]);
        assert!(!r.text().contains("\nFAIL forged"));
    }
}
