//! Test-only: resident memory of the process (Linux), to measure bytes/series.
#![cfg(test)]

pub(crate) fn rss_bytes() -> u64 {
    let s = std::fs::read_to_string("/proc/self/statm").unwrap_or_default();
    s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()).unwrap_or(0) * 4096
}
