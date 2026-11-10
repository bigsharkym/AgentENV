//! Temporary, opt-in stage timing probe used for create-path profiling.
//!
//! Enabled only when `AENV_STAGE_PROBE_FILE` is set. Each probe appends one
//! tab-separated line on drop:
//!
//! ```text
//! PROBE\t<label>\t<micros>\t<context>
//! ```
//!
//! When the env var is unset every probe is a no-op, so this is safe to leave
//! compiled in.

use std::io::Write;
use std::sync::OnceLock;
use std::time::Instant;

fn probe_path() -> Option<&'static str> {
    static PATH: OnceLock<Option<String>> = OnceLock::new();
    PATH.get_or_init(|| std::env::var("AENV_STAGE_PROBE_FILE").ok())
        .as_deref()
}

pub struct StageProbe {
    label: &'static str,
    context: String,
    start: Option<Instant>,
}

impl StageProbe {
    pub fn start(label: &'static str, context: impl Into<String>) -> Self {
        let start = probe_path().map(|_| Instant::now());
        Self {
            label,
            context: context.into(),
            start,
        }
    }
}

impl Drop for StageProbe {
    fn drop(&mut self) {
        let (Some(start), Some(path)) = (self.start, probe_path()) else {
            return;
        };
        let micros = start.elapsed().as_micros();
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(file, "PROBE\t{}\t{}\t{}", self.label, micros, self.context);
        }
    }
}
