use std::collections::{BTreeMap, btree_map};
use std::fmt;
use std::marker::PhantomData;

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};

use crate::types::BlockNumber;

/// A value for each of some blocks, in block order: the data of the unfinal
/// blocks of a view aggregate, or of a reducer group.
///
/// Most groups hold one unfinal block or none. One block stays inline, so
/// those groups allocate nothing; a second block brings a map. A map that
/// empties goes: an emptied `BTreeMap` keeps its root node, which for a sum
/// is 640 bytes.
#[derive(Debug, Clone, Default)]
pub enum Blocks<V> {
    #[default]
    Empty,
    One(BlockNumber, V),
    Many(BTreeMap<BlockNumber, V>),
}

impl<V> Blocks<V> {
    pub const fn new() -> Self {
        Self::Empty
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(..) => 1,
            Self::Many(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn get(&self, block: BlockNumber) -> Option<&V> {
        match self {
            Self::Empty => None,
            Self::One(b, value) => (*b == block).then_some(value),
            Self::Many(map) => map.get(&block),
        }
    }

    /// The blocks and their values, in block order.
    pub fn iter(&self) -> Iter<'_, V> {
        match self {
            Self::Empty => Iter::Done,
            Self::One(block, value) => Iter::One(block, value),
            Self::Many(map) => Iter::Many(map.iter()),
        }
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &V> {
        self.iter().map(|(_, value)| value)
    }

    pub fn last_block(&self) -> Option<BlockNumber> {
        self.iter().next_back().map(|(block, _)| *block)
    }

    /// The value of the lowest block above `block`.
    pub fn first_above(&self, block: BlockNumber) -> Option<&V> {
        match self {
            Self::Many(map) => {
                let above = block.checked_add(1)?;
                map.range(above..).next().map(|(_, value)| value)
            }
            _ => self
                .iter()
                .find(|(b, _)| **b > block)
                .map(|(_, value)| value),
        }
    }

    /// Set the value of `block`.
    pub fn insert(&mut self, block: BlockNumber, value: V) {
        match self {
            Self::Many(map) => {
                map.insert(block, value);
            }
            Self::One(b, old) if *b == block => *old = value,
            Self::Empty => *self = Self::One(block, value),
            Self::One(..) => {
                self.spill(block, value);
            }
        }
    }

    /// The value of `block`, set to `value` if the block has none.
    pub fn or_insert(&mut self, block: BlockNumber, value: V) -> &mut V {
        match self {
            Self::Many(map) => return map.entry(block).or_insert(value),
            Self::One(b, _) if *b != block => return self.spill(block, value),
            Self::One(..) => {}
            Self::Empty => *self = Self::One(block, value),
        }

        let Self::One(_, value) = self else {
            unreachable!()
        };
        value
    }

    /// Turn `One` into `Many` with `block` added, and give its value.
    fn spill(&mut self, block: BlockNumber, value: V) -> &mut V {
        let Self::One(first_block, first) = std::mem::replace(self, Self::Many(BTreeMap::new()))
        else {
            unreachable!("spill of a single block")
        };
        let Self::Many(map) = self else {
            unreachable!()
        };
        map.insert(first_block, first);
        map.entry(block).or_insert(value)
    }

    pub fn remove(&mut self, block: BlockNumber) -> Option<V> {
        let removed = match self {
            Self::Empty => None,
            Self::One(b, _) if *b != block => None,
            Self::One(..) => match std::mem::take(self) {
                Self::One(_, value) => Some(value),
                _ => unreachable!(),
            },
            Self::Many(map) => map.remove(&block),
        };
        self.drop_empty_map();
        removed
    }

    /// Remove the blocks above `block`. Returns the value of the lowest of
    /// them.
    pub fn remove_above(&mut self, block: BlockNumber) -> Option<V> {
        let removed = match self {
            Self::Empty => None,
            Self::One(b, _) if *b <= block => None,
            Self::One(..) => match std::mem::take(self) {
                Self::One(_, value) => Some(value),
                _ => unreachable!(),
            },
            Self::Many(map) => {
                let above = block.checked_add(1)?;
                map.split_off(&above).into_values().next()
            }
        };
        self.drop_empty_map();
        removed
    }

    /// Remove the blocks at or below `block`, and hand each value to `f` in
    /// block order.
    pub fn drain_up_to(&mut self, block: BlockNumber, mut f: impl FnMut(V)) {
        match self {
            Self::Empty => {}
            Self::One(b, _) if *b > block => {}
            Self::One(..) => {
                if let Self::One(_, value) = std::mem::take(self) {
                    f(value);
                }
            }
            Self::Many(map) => {
                while let Some(entry) = map.first_entry() {
                    if *entry.key() > block {
                        break;
                    }
                    f(entry.remove());
                }
            }
        }
        self.drop_empty_map();
    }

    fn drop_empty_map(&mut self) {
        if let Self::Many(map) = self
            && map.is_empty()
        {
            *self = Self::Empty;
        }
    }
}

/// The iterator of `Blocks::iter`. A fold goes straight to the map: a window
/// folds thousands of blocks for each expiry.
pub enum Iter<'a, V> {
    Done,
    One(&'a BlockNumber, &'a V),
    Many(btree_map::Iter<'a, BlockNumber, V>),
}

impl<'a, V> Iterator for Iter<'a, V> {
    type Item = (&'a BlockNumber, &'a V);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Many(iter) => iter.next(),
            Self::One(block, value) => {
                let item = (*block, *value);
                *self = Self::Done;
                Some(item)
            }
            Self::Done => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Many(iter) => iter.size_hint(),
            Self::One(..) => (1, Some(1)),
            Self::Done => (0, Some(0)),
        }
    }

    #[inline]
    fn fold<B, F: FnMut(B, Self::Item) -> B>(self, init: B, mut f: F) -> B {
        match self {
            Self::Many(iter) => iter.fold(init, f),
            Self::One(block, value) => f(init, (block, value)),
            Self::Done => init,
        }
    }
}

impl<V> DoubleEndedIterator for Iter<'_, V> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Many(iter) => iter.next_back(),
            _ => self.next(),
        }
    }
}

impl<V> From<BTreeMap<BlockNumber, V>> for Blocks<V> {
    fn from(mut map: BTreeMap<BlockNumber, V>) -> Self {
        match map.len() {
            0 => Self::Empty,
            1 => {
                let (block, value) = map.pop_first().expect("one entry");
                Self::One(block, value)
            }
            _ => Self::Many(map),
        }
    }
}

/// The encoding of a `BTreeMap`, so that stored aggregates read either way.
impl<V: Serialize> Serialize for Blocks<V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for (block, value) in self.iter() {
            map.serialize_entry(block, value)?;
        }
        map.end()
    }
}

impl<'de, V: Deserialize<'de>> Deserialize<'de> for Blocks<V> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct BlocksVisitor<V>(PhantomData<V>);

        impl<'de, V: Deserialize<'de>> Visitor<'de> for BlocksVisitor<V> {
            type Value = Blocks<V>;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a map of block numbers")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut map = BTreeMap::new();
                while let Some((block, value)) = access.next_entry()? {
                    map.insert(block, value);
                }
                Ok(Blocks::from(map))
            }
        }

        deserializer.deserialize_map(BlocksVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(blocks: &Blocks<u64>) -> Vec<(BlockNumber, u64)> {
        blocks.iter().map(|(b, v)| (*b, *v)).collect()
    }

    /// The same operations on `Blocks` and on a `BTreeMap` give the same
    /// entries, and `Blocks` never holds an empty map.
    #[test]
    fn blocks_act_as_a_map() {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let mut next = |n: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % n
        };

        for _ in 0..2000 {
            let mut blocks = Blocks::new();
            let mut map = BTreeMap::new();
            for _ in 0..12 {
                let block = next(6);
                match next(6) {
                    0 => {
                        *blocks.or_insert(block, 0) += 1;
                        *map.entry(block).or_insert(0) += 1;
                    }
                    1 => {
                        blocks.insert(block, block * 10);
                        map.insert(block, block * 10);
                    }
                    2 => assert_eq!(blocks.remove(block), map.remove(&block)),
                    3 => {
                        let tail = map.split_off(&(block + 1));
                        assert_eq!(blocks.remove_above(block), tail.into_values().next());
                    }
                    4 => {
                        let mut drained = Vec::new();
                        blocks.drain_up_to(block, |v| drained.push(v));
                        let kept = map.split_off(&(block + 1));
                        let expected: Vec<u64> =
                            std::mem::replace(&mut map, kept).into_values().collect();
                        assert_eq!(drained, expected);
                    }
                    _ => {
                        let above = map.range(block + 1..).next().map(|(_, v)| v);
                        assert_eq!(blocks.first_above(block), above);
                        assert_eq!(blocks.get(block), map.get(&block));
                    }
                }

                let expected: Vec<(BlockNumber, u64)> = map.iter().map(|(b, v)| (*b, *v)).collect();
                assert_eq!(entries(&blocks), expected);
                assert_eq!(blocks.last_block(), map.keys().next_back().copied());
                assert!(!matches!(&blocks, Blocks::Many(m) if m.is_empty()));
            }
        }
    }

    #[test]
    fn blocks_encode_as_a_map() {
        let map = BTreeMap::from([(3u64, 30u64), (7, 70)]);
        for source in [BTreeMap::new(), BTreeMap::from([(3, 30)]), map] {
            let blocks = Blocks::from(source.clone());
            let encoded = rmp_serde::to_vec(&blocks).unwrap();
            assert_eq!(encoded, rmp_serde::to_vec(&source).unwrap());

            let decoded: Blocks<u64> = rmp_serde::from_slice(&encoded).unwrap();
            assert_eq!(entries(&decoded), source.into_iter().collect::<Vec<_>>());
        }
    }
}
