//! Timings for `ChangeBatch::perf`. With collection off, no clock is read and
//! no node is built.

use crate::types::{PerfNode, PerfNodeKind};

#[cfg(not(target_arch = "wasm32"))]
type Started = std::time::Instant;
// wasm32 has no std clock: nodes keep their shape with zero durations.
#[cfg(target_arch = "wasm32")]
type Started = ();

/// The start of one perf node; empty when collection is off.
pub(crate) struct PerfTimer(Option<Started>);

impl PerfTimer {
    pub(crate) fn start(enabled: bool) -> Self {
        PerfTimer(enabled.then(now))
    }

    /// The finished node, or None when collection is off.
    pub(crate) fn stop(
        self,
        kind: PerfNodeKind,
        name: &str,
        children: Vec<PerfNode>,
    ) -> Option<PerfNode> {
        let started = self.0?;
        Some(PerfNode {
            kind,
            name: name.to_string(),
            duration_ms: elapsed_ms(started),
            children,
        })
    }
}

/// One "ingest" node for the per-block trees of an ingest. The durations are
/// summed into the first tree, matched by position two levels deep: every
/// block runs the same pipeline.
pub(crate) fn merge_blocks(blocks: Vec<PerfNode>) -> Option<PerfNode> {
    let mut blocks = blocks.into_iter();
    let mut merged = blocks.next()?;

    for node in blocks {
        merged.duration_ms += node.duration_ms;
        for (child, total) in node.children.into_iter().zip(&mut merged.children) {
            total.duration_ms += child.duration_ms;
            for (grandchild, total) in child.children.into_iter().zip(&mut total.children) {
                total.duration_ms += grandchild.duration_ms;
            }
        }
    }

    Some(PerfNode {
        kind: PerfNodeKind::Pipeline,
        name: "ingest".to_string(),
        duration_ms: merged.duration_ms,
        children: vec![merged],
    })
}

#[cfg(not(target_arch = "wasm32"))]
fn now() -> Started {
    std::time::Instant::now()
}

#[cfg(not(target_arch = "wasm32"))]
fn elapsed_ms(started: Started) -> f64 {
    started.elapsed().as_secs_f64() * 1000.0
}

#[cfg(target_arch = "wasm32")]
fn now() -> Started {}

#[cfg(target_arch = "wasm32")]
fn elapsed_ms(_: Started) -> f64 {
    0.0
}
