//! Known gaps of the gap register that change only what a call reports, not
//! the state. The driver tolerates each one only in the case that the
//! register describes, and counts the hits: `known_gaps_still_reproduce`
//! fails once a fix makes one vanish.
//! A gap that makes the state diverge is kept out of the histories instead;
//! the gap register gives its first test.

use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Gap {
    /// A raw delete has no previous values, nor does an update that merges
    /// a raw delete and a raw insert.
    Gap10,
    /// An in-line fork leaves an update equal to its previous values.
    Gap41,
    /// A view delete that merges the compensation update of an in-line fork
    /// carries the values of that update, not the deleted row.
    Gap47,
}

impl Gap {
    pub const ALL: [Gap; 3] = [Gap::Gap10, Gap::Gap41, Gap::Gap47];
}

#[derive(Default, Debug)]
pub struct Hits(pub BTreeMap<Gap, u64>);

impl Hits {
    pub fn hit(&mut self, gap: Gap) {
        *self.0.entry(gap).or_default() += 1;
    }

    pub fn merge(&mut self, other: &Hits) {
        for (gap, n) in &other.0 {
            *self.0.entry(*gap).or_default() += n;
        }
    }
}
