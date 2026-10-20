//! Deterministic randomness, so a failing seed replays exactly (HC-20).

/// xorshift64*.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True with a probability of `percent` / 100.
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }
}

/// The seeds of a test run. `SETTLE_SEED=n` runs only seed `n`, and
/// `SETTLE_ORACLE_SCALE=k` runs `k` times the per-PR count (P-ORACLE-SEEDS).
pub fn seeds(per_pr: u64) -> std::ops::Range<u64> {
    if let Some(seed) = env_u64("SETTLE_SEED") {
        return seed..seed + 1;
    }
    let scale = env_u64("SETTLE_ORACLE_SCALE").unwrap_or(1);
    0..per_pr * scale
}

fn env_u64(name: &str) -> Option<u64> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be a number, got {value:?}")),
    )
}
