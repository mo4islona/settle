//! Process counters from `/proc`. Linux only: elsewhere each reads None.

use std::time::Duration;

/// Linux reports CPU time in clock ticks of 1/100 s on every common
/// architecture (`USER_HZ`).
const TICKS_PER_SEC: f64 = 100.0;

/// The CPU time of this process, all threads, user and system.
pub fn cpu_time() -> Option<Duration> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The command name can hold spaces; the fields after it cannot.
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    Some(Duration::from_secs_f64((utime + stime) / TICKS_PER_SEC))
}

/// Bytes that this process caused to reach the block layer.
pub fn write_bytes() -> Option<u64> {
    let io = std::fs::read_to_string("/proc/self/io").ok()?;
    io.lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .and_then(|v| v.trim().parse().ok())
}

/// `VmRSS` or `VmHWM` of this process, in bytes.
pub fn memory(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kb: u64 = status
        .lines()
        .find_map(|line| line.strip_prefix(field)?.strip_prefix(':'))?
        .trim()
        .trim_end_matches(" kB")
        .parse()
        .ok()?;
    Some(kb * 1024)
}

/// Sets `VmHWM` to the current `VmRSS`.
pub fn reset_peak_memory() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// p-th percentile of `sorted`, nearest rank.
pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}
