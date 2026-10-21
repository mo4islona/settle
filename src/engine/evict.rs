//! Group eviction, which bounds the groups that a reducer or a view keeps in
//! memory. A group may leave memory only when storage holds its state: it has
//! no unfinal block, no finalized change that waits for a checkpoint, and the
//! checkpoint that last wrote it is committed. A miss loads it again.

/// When a node evicts: once it holds more than its cap of groups, down to
/// three quarters of the cap, so that a round of eviction, which scans every
/// group, comes once in a quarter of the cap of new groups.
#[derive(Default)]
pub(super) struct Eviction {
    /// After a round that could not reach its target: the resident groups
    /// and the committed checkpoint then. The groups that must stay in
    /// memory do not leave before more of them are on disk, so the next
    /// round waits for that, or for a quarter of the cap more groups.
    stuck: Option<(usize, u64)>,
}

impl Eviction {
    /// How many of `resident` groups to evict now; 0 for none.
    pub(super) fn due(&self, resident: usize, cap: usize, committed: u64) -> usize {
        if resident <= cap {
            return 0;
        }

        let waits = self.stuck.is_some_and(|(stuck_at, stuck_committed)| {
            resident < stuck_at.saturating_add(cap / 4) && committed <= stuck_committed
        });
        if waits {
            return 0;
        }
        resident - target(cap)
    }

    /// Record a round that left `resident` groups.
    pub(super) fn done(&mut self, resident: usize, cap: usize, committed: u64) {
        self.stuck = (resident > target(cap)).then_some((resident, committed));
    }
}

fn target(cap: usize) -> usize {
    cap - cap / 4
}

/// Picks `count` of the groups that may leave memory, those that a
/// checkpoint wrote longest ago first.
pub(super) struct Victims {
    threshold: u64,
    /// The groups written at `threshold` that are still to pick.
    ties: usize,
}

impl Victims {
    /// `written` holds, for each group that may leave, the checkpoint that
    /// last wrote it. None if there is none to pick.
    pub(super) fn new(mut written: Vec<u64>, count: usize) -> Option<Self> {
        let count = count.min(written.len());
        if count == 0 {
            return None;
        }

        let (_, &mut threshold, _) = written.select_nth_unstable(count - 1);
        let older = written.iter().filter(|&&w| w < threshold).count();
        Some(Self {
            threshold,
            ties: count - older,
        })
    }

    /// Whether to evict a group that may leave and that checkpoint
    /// `written` wrote. Call it once for each such group.
    pub(super) fn pick(&mut self, written: u64) -> bool {
        if written < self.threshold {
            return true;
        }
        if written == self.threshold && self.ties > 0 {
            self.ties -= 1;
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn victims_are_the_oldest_writes() {
        let written = vec![5, 1, 3, 3, 9, 3, 0];
        let mut victims = Victims::new(written.clone(), 4).unwrap();
        let picked: Vec<u64> = written.into_iter().filter(|&w| victims.pick(w)).collect();
        assert_eq!(picked, [1, 3, 3, 0]);
    }

    #[test]
    fn no_victims_without_candidates() {
        assert!(Victims::new(Vec::new(), 3).is_none());
        assert!(Victims::new(vec![1, 2], 0).is_none());
    }

    #[test]
    fn eviction_goes_to_three_quarters_and_waits_when_stuck() {
        let mut eviction = Eviction::default();
        assert_eq!(eviction.due(100, 100, 0), 0);
        assert_eq!(eviction.due(101, 100, 0), 26);

        // 90 groups had to stay.
        eviction.done(90, 100, 7);
        assert_eq!(eviction.due(110, 100, 7), 0);
        assert_eq!(eviction.due(115, 100, 7), 40);
        assert_eq!(eviction.due(101, 100, 8), 26);

        eviction.done(75, 100, 8);
        assert_eq!(eviction.due(101, 100, 8), 26);
    }
}
