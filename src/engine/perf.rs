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
/// summed into the first tree at every depth, nodes matched by kind and
/// name: a block whose reducer emits nothing has no nodes for its MVs.
pub(crate) fn merge_blocks(blocks: Vec<PerfNode>) -> Option<PerfNode> {
    let mut blocks = blocks.into_iter();
    let mut merged = blocks.next()?;

    for node in blocks {
        add_durations(&mut merged, node);
    }

    Some(PerfNode {
        kind: PerfNodeKind::Pipeline,
        name: "ingest".to_string(),
        duration_ms: merged.duration_ms,
        children: vec![merged],
    })
}

fn add_durations(total: &mut PerfNode, node: PerfNode) {
    total.duration_ms += node.duration_ms;
    for child in node.children {
        let same = total
            .children
            .iter_mut()
            .find(|t| t.kind == child.kind && t.name == child.name);
        match same {
            Some(same) => add_durations(same, child),
            None => total.children.push(child),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn node(kind: PerfNodeKind, name: &str, duration_ms: f64, children: Vec<PerfNode>) -> PerfNode {
        PerfNode {
            kind,
            name: name.to_string(),
            duration_ms,
            children,
        }
    }

    fn block(reducer_ms: f64, views: Vec<PerfNode>) -> PerfNode {
        let raw = node(PerfNodeKind::RawTable, "swaps", 1.0, vec![]);
        let reducer = node(PerfNodeKind::Reducer, "positions", reducer_ms, views);
        node(PerfNodeKind::Pipeline, "block", 10.0, vec![raw, reducer])
    }

    /// An MV under a reducer sums over all blocks, also when the first block
    /// has no node for it.
    #[test]
    fn blocks_sum_at_every_depth() {
        let view = |ms| vec![node(PerfNodeKind::MV, "wallet_pnl", ms, vec![])];
        let blocks = vec![
            block(2.0, vec![]),
            block(3.0, view(4.0)),
            block(5.0, view(6.0)),
        ];

        let ingest = merge_blocks(blocks).unwrap();

        let merged = &ingest.children[0];
        assert_eq!(merged.duration_ms, 30.0);
        assert_eq!(merged.children[0].duration_ms, 3.0);
        let reducer = &merged.children[1];
        assert_eq!(reducer.duration_ms, 10.0);
        assert_eq!(reducer.children.len(), 1);
        assert_eq!(reducer.children[0].duration_ms, 10.0);
    }
}
