//! A synthetic stream of Solana DEX swaps with the shape of the real one:
//! about 250 swaps in a slot, 4% of slots without a block, a third of the
//! swaps from a few thousand bots and the rest from a long tail of wallets,
//! established tokens next to a stream of new memecoins whose trading fades
//! within minutes, ten DEX programs, and sells that come out of the
//! positions that the wallets bought. Deterministic for a seed.
//!
//! Quotes are SOL, USDC, USDT and a few hub tokens that other established
//! tokens trade against, as BONK or JUP. The swaps carry no oracle price:
//! the pipeline prices each token in USDC from the swaps themselves.
//!
//! `write` saves it for the Node bench, which then sees the same swaps
//! (`bindings/typescript/settle/bench/solana-dex.mjs` reads the format).

#![allow(dead_code)]

use std::io::Write;
use std::ops::Range;

use rustc_hash::FxHashMap;

pub const SOL: &str = "So11111111111111111111111111111111111111112";
pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";

const DEXES: [&str; 10] = [
    "raydium_amm_v4",
    "raydium_clmm",
    "raydium_cpmm",
    "orca_whirlpool",
    "meteora_dlmm",
    "meteora_damm_v2",
    "pumpfun",
    "pumpswap",
    "phoenix",
    "lifinity_v2",
];

/// (DEX index, share) for each kind of base token.
const MEME_DEXES: &[(usize, f64)] = &[(6, 0.55), (7, 0.30), (0, 0.10), (4, 0.05)];
const ESTABLISHED_DEXES: &[(usize, f64)] = &[
    (0, 0.25),
    (3, 0.25),
    (4, 0.20),
    (1, 0.15),
    (2, 0.05),
    (5, 0.05),
    (8, 0.03),
    (9, 0.02),
];
const SOL_DEXES: &[(usize, f64)] = &[(3, 0.35), (1, 0.25), (8, 0.15), (4, 0.15), (9, 0.10)];

/// (quote: 0 SOL, 1 USDC, 2 USDT, 3 a hub token, share) for each kind of
/// base token.
const MEME_QUOTES: &[(usize, f64)] = &[(0, 0.97), (1, 0.03)];
const ESTABLISHED_QUOTES: &[(usize, f64)] = &[(0, 0.70), (1, 0.15), (2, 0.03), (3, 0.12)];
const SOL_QUOTES: &[(usize, f64)] = &[(1, 0.85), (2, 0.15)];
const STABLE_QUOTES: &[(usize, f64)] = &[(1, 1.0)];

/// The most traded established tokens; the others also trade against them.
const HUBS: usize = 16;

const FIRST_SLOT: u64 = 370_000_000;
const FIRST_TIMESTAMP_MS: u64 = 1_760_000_000_000;
const SLOT_MS: u64 = 400;
const SKIPPED_SLOTS: f64 = 0.04;
const MIN_SWAPS_PER_SLOT: usize = 150;
const MAX_SWAPS_PER_SLOT: usize = 350;

const BOTS: usize = 3_000;
const RETAIL: usize = 2_000_000;
const BOT_SHARE: f64 = 0.35;

const ESTABLISHED: usize = 4_000;
const LIVE_MEMES: usize = 1_500;
const MEME_BIRTHS_PER_SLOT: f64 = 0.2;
/// The mean distance from the newest memecoin of the one a swap trades.
const MEME_RECENCY: f64 = 120.0;
const SOL_SHARE: f64 = 0.10;
/// USDT against USDC.
const STABLE_SHARE: f64 = 0.02;
const ESTABLISHED_SHARE: f64 = 0.52;

/// Positions that the generator remembers for each wallet, to sell later.
const HELD_PER_WALLET: usize = 32;
/// Retail sells of tokens that came by transfer, not by a swap here.
const TRANSFER_SELLS: f64 = 0.03;

pub struct Block {
    pub slot: u64,
    pub timestamp_ms: u64,
    pub swaps: Range<usize>,
}

/// One swap; the `u32` fields index `Dataset::strings`.
pub struct Swap {
    pub dex: u32,
    pub pool: u32,
    pub trader: u32,
    pub token: u32,
    pub quote: u32,
    pub buy: bool,
    pub base_amount: f64,
    pub quote_amount: f64,
}

pub struct Dataset {
    pub strings: Vec<String>,
    pub blocks: Vec<Block>,
    pub swaps: Vec<Swap>,
    pub traders: usize,
    pub tokens: usize,
    pub pools: usize,
    pub positions: usize,
}

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, 1).
    fn f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn normal(&mut self) -> f64 {
        let u1 = self.f64().max(f64::MIN_POSITIVE);
        let u2 = self.f64();
        (-2.0 * u1.ln()).sqrt() * (std::f64::consts::TAU * u2).cos()
    }

    fn pick(&mut self, weights: &[(usize, f64)]) -> usize {
        let mut u = self.f64();
        for &(item, share) in weights {
            if u < share {
                return item;
            }
            u -= share;
        }
        weights[weights.len() - 1].0
    }
}

struct Zipf {
    cdf: Vec<f64>,
}

impl Zipf {
    fn new(n: usize, s: f64) -> Self {
        let mut cdf = Vec::with_capacity(n);
        let mut total = 0.0;
        for rank in 1..=n {
            total += 1.0 / (rank as f64).powf(s);
            cdf.push(total);
        }
        for c in &mut cdf {
            *c /= total;
        }
        Self { cdf }
    }

    fn sample(&self, rng: &mut Rng) -> usize {
        let u = rng.f64();
        self.cdf.partition_point(|&c| c < u).min(self.cdf.len() - 1)
    }
}

const BASE58: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// A base58 address of 32 bytes that depends on `kind` and `index` only.
fn address(kind: u64, index: u64) -> String {
    let mut rng = Rng(kind << 40 ^ index);
    let mut bytes = [0u8; 32];
    for chunk in bytes.chunks_mut(8) {
        chunk.copy_from_slice(&rng.next_u64().to_le_bytes());
    }

    // Little-endian digits of the base58 number.
    let mut digits: Vec<u8> = Vec::with_capacity(44);
    for &byte in &bytes {
        let mut carry = byte as u32;
        for digit in digits.iter_mut() {
            carry += (*digit as u32) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let zeros = bytes.iter().take_while(|&&b| b == 0).count();

    let mut out = String::with_capacity(44);
    out.extend(std::iter::repeat_n('1', zeros));
    out.extend(digits.iter().rev().map(|&d| BASE58[d as usize] as char));
    out
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Sol,
    Stable,
    Established,
    Meme,
}

/// The index in `Generator::tokens` of SOL, USDT and the first established
/// token.
const SOL_TOKEN: usize = 0;
const USDT_TOKEN: usize = 1;
const FIRST_ESTABLISHED: usize = 2;

struct Token {
    name: u32,
    kind: Kind,
    price_usd: f64,
}

struct Generator {
    rng: Rng,
    strings: Vec<String>,
    traders: FxHashMap<usize, u32>,
    pools: FxHashMap<(usize, usize, usize), u32>,
    tokens: Vec<Token>,
    memes: Vec<usize>,
    /// Tokens and amounts that each wallet holds, oldest first.
    held: FxHashMap<usize, Vec<(usize, f64)>>,
    positions: rustc_hash::FxHashSet<(usize, usize)>,
    dexes: [u32; 10],
    quotes: [u32; 3],
    bot_zipf: Zipf,
    retail_zipf: Zipf,
    established_zipf: Zipf,
    sol_usd: f64,
}

impl Generator {
    fn new(seed: u64) -> Self {
        let mut strings = Vec::new();
        let mut intern = |s: String| {
            strings.push(s);
            (strings.len() - 1) as u32
        };
        let dexes = DEXES.map(|d| intern(d.to_string()));
        let quotes = [SOL, USDC, USDT].map(|q| intern(q.to_string()));

        let mut rng = Rng(seed);
        let mut tokens = vec![
            Token {
                name: quotes[0],
                kind: Kind::Sol,
                price_usd: 0.0,
            },
            Token {
                name: quotes[2],
                kind: Kind::Stable,
                price_usd: 1.0,
            },
        ];
        for i in 0..ESTABLISHED {
            // Prices from $0.0001 to $300.
            let price_usd = 10f64.powf(-4.0 + rng.f64() * 6.5);
            tokens.push(Token {
                name: intern(address(1, i as u64)),
                kind: Kind::Established,
                price_usd,
            });
        }

        let mut generator = Self {
            rng,
            strings,
            traders: FxHashMap::default(),
            pools: FxHashMap::default(),
            tokens,
            memes: Vec::new(),
            held: FxHashMap::default(),
            positions: Default::default(),
            dexes,
            quotes,
            bot_zipf: Zipf::new(BOTS, 0.9),
            retail_zipf: Zipf::new(RETAIL, 1.0),
            established_zipf: Zipf::new(ESTABLISHED, 1.1),
            sol_usd: 165.0,
        };
        for _ in 0..LIVE_MEMES {
            generator.launch_meme();
        }
        generator
    }

    fn intern(&mut self, s: String) -> u32 {
        self.strings.push(s);
        (self.strings.len() - 1) as u32
    }

    /// A new pump.fun token: its address ends in "pump", and it starts at
    /// a market cap of about $5k for a billion tokens.
    fn launch_meme(&mut self) {
        let index = self.memes.len() as u64;
        let mut name = address(2, index);
        name.truncate(name.len() - 4);
        name.push_str("pump");
        let price_usd = 5e-6 * (0.5 * self.rng.normal()).exp();
        let name = self.intern(name);
        self.tokens.push(Token {
            name,
            kind: Kind::Meme,
            price_usd,
        });
        self.memes.push(self.tokens.len() - 1);
    }

    fn trader(&mut self) -> (usize, bool) {
        let bot = self.rng.f64() < BOT_SHARE;
        let rank = if bot {
            self.bot_zipf.sample(&mut self.rng)
        } else {
            BOTS + self.retail_zipf.sample(&mut self.rng)
        };
        (rank, bot)
    }

    fn trader_name(&mut self, rank: usize) -> u32 {
        if let Some(&name) = self.traders.get(&rank) {
            return name;
        }
        let name = self.intern(address(3, rank as u64));
        self.traders.insert(rank, name);
        name
    }

    fn pick_token(&mut self) -> usize {
        let u = self.rng.f64();
        if u < SOL_SHARE {
            return SOL_TOKEN;
        }
        if u < SOL_SHARE + STABLE_SHARE {
            return USDT_TOKEN;
        }
        if u < SOL_SHARE + STABLE_SHARE + ESTABLISHED_SHARE {
            return FIRST_ESTABLISHED + self.established_zipf.sample(&mut self.rng);
        }
        let p = 1.0 / MEME_RECENCY;
        let back = ((1.0 - self.rng.f64()).ln() / (1.0 - p).ln()) as usize;
        let back = back.min(self.memes.len() - 1);
        self.memes[self.memes.len() - 1 - back]
    }

    /// The price of `token` after one more swap, with a small push in the
    /// direction of the trade.
    fn step_price(&mut self, token: usize, buy: bool) -> f64 {
        match self.tokens[token].kind {
            Kind::Sol => return self.sol_usd,
            Kind::Stable => {
                let price = (0.0005 * self.rng.normal()).exp();
                self.tokens[token].price_usd = price;
                return price;
            }
            Kind::Established | Kind::Meme => {}
        }
        let sigma = if self.tokens[token].kind == Kind::Meme {
            0.015
        } else {
            0.002
        };
        let push = if buy { 0.0004 } else { -0.0004 };
        let step = (sigma * self.rng.normal() + push).exp();
        let price = (self.tokens[token].price_usd * step).clamp(1e-12, 1e6);
        self.tokens[token].price_usd = price;
        price
    }

    /// The size of a trade in USD: a median of $25 for a wallet, $120 for a bot.
    fn trade_usd(&mut self, bot: bool) -> f64 {
        let (median, sigma) = if bot { (120.0, 1.0) } else { (25.0, 1.3) };
        (median * (sigma * self.rng.normal()).exp()).clamp(0.05, 250_000.0)
    }

    fn price_usd(&self, token: usize) -> f64 {
        match self.tokens[token].kind {
            Kind::Sol => self.sol_usd,
            _ => self.tokens[token].price_usd,
        }
    }

    /// The DEX, pool and quote of a swap of `token`, and the USD price of
    /// the quote.
    fn pool(&mut self, token: usize) -> (u32, u32, u32, f64) {
        let (dexes, quotes) = match self.tokens[token].kind {
            Kind::Sol => (SOL_DEXES, SOL_QUOTES),
            Kind::Stable => (SOL_DEXES, STABLE_QUOTES),
            Kind::Established => (ESTABLISHED_DEXES, ESTABLISHED_QUOTES),
            Kind::Meme => (MEME_DEXES, MEME_QUOTES),
        };
        let dex = self.rng.pick(dexes);
        let quote_token = match self.rng.pick(quotes) {
            0 => Some(SOL_TOKEN),
            1 => None,
            2 => Some(USDT_TOKEN),
            _ => {
                let hub = FIRST_ESTABLISHED + self.rng.below(HUBS);
                Some(if hub == token { hub + 1 } else { hub })
            }
        };
        let (quote, quote_usd) = match quote_token {
            Some(t) => (self.tokens[t].name, self.price_usd(t)),
            None => (self.quotes[1], 1.0),
        };

        let key = (token, quote as usize, dex);
        let pool = match self.pools.get(&key) {
            Some(&pool) => pool,
            None => {
                let index = self.pools.len() as u64;
                let pool = self.intern(address(4, index));
                self.pools.insert(key, pool);
                pool
            }
        };
        (self.dexes[dex], pool, quote, quote_usd)
    }

    fn swap(&mut self) -> Swap {
        let (rank, bot) = self.trader();
        let trader = self.trader_name(rank);

        let held = self.held.entry(rank).or_default();
        let sell_share = if bot { 0.5 } else { 0.4 };
        let sell_held = !held.is_empty() && self.rng.f64() < sell_share;

        let (token, buy, base_amount, price) = if sell_held {
            let held = self.held.get_mut(&rank).unwrap();
            let at = (self.rng.next_u64() % held.len() as u64) as usize;
            let (token, qty) = held[at];
            let u = self.rng.f64();
            let fraction = if u < 0.5 {
                1.0
            } else if u < 0.75 {
                0.5
            } else {
                0.25
            };
            if fraction == 1.0 {
                held.remove(at);
            } else {
                held[at].1 = qty * (1.0 - fraction);
            }
            let price = self.step_price(token, false);
            (token, false, qty * fraction, price)
        } else {
            let token = self.pick_token();
            let transfer_sell = !bot && self.rng.f64() < TRANSFER_SELLS;
            let price = self.step_price(token, !transfer_sell);
            let base_amount = self.trade_usd(bot) / price;
            if !transfer_sell {
                let held = self.held.get_mut(&rank).unwrap();
                match held.iter_mut().find(|(t, _)| *t == token) {
                    Some(position) => position.1 += base_amount,
                    None => {
                        if held.len() == HELD_PER_WALLET {
                            held.remove(0);
                        }
                        held.push((token, base_amount));
                    }
                }
            }
            (token, !transfer_sell, base_amount, price)
        };

        self.positions.insert((rank, token));
        let (dex, pool, quote, quote_usd) = self.pool(token);
        let quote_amount = base_amount * price / quote_usd;

        Swap {
            dex,
            pool,
            trader,
            token: self.tokens[token].name,
            quote,
            buy,
            base_amount,
            quote_amount,
        }
    }
}

/// `slots` slots of swaps, from slot 370,000,000 on.
pub fn generate(slots: usize, seed: u64) -> Dataset {
    let mut g = Generator::new(seed);
    let mut blocks = Vec::new();
    let mut swaps = Vec::new();

    for offset in 0..slots as u64 {
        g.sol_usd *= (0.0003 * g.rng.normal()).exp();
        if g.rng.f64() < MEME_BIRTHS_PER_SLOT {
            g.launch_meme();
        }
        if g.rng.f64() < SKIPPED_SLOTS {
            continue;
        }

        let count = MIN_SWAPS_PER_SLOT + g.rng.below(MAX_SWAPS_PER_SLOT - MIN_SWAPS_PER_SLOT + 1);
        let start = swaps.len();
        for _ in 0..count {
            swaps.push(g.swap());
        }
        blocks.push(Block {
            slot: FIRST_SLOT + offset,
            timestamp_ms: FIRST_TIMESTAMP_MS + offset * SLOT_MS,
            swaps: start..swaps.len(),
        });
    }

    Dataset {
        traders: g.traders.len(),
        tokens: g.tokens.len(),
        pools: g.pools.len(),
        positions: g.positions.len(),
        strings: g.strings,
        blocks,
        swaps,
    }
}

impl Dataset {
    /// Little-endian, each section padded to 8 bytes:
    /// `SDEX0002`, string count, string bytes, the strings joined by '\n';
    /// block count, swap count; per block slot, timestamp (f64) and swap
    /// count (u32); per swap base amount, quote amount (f64), then dex, pool,
    /// trader, token, quote (u32), then side (u8, 1 = buy).
    pub fn write(&self, path: &str) -> std::io::Result<()> {
        let mut out: Vec<u8> = Vec::new();
        let pad = |out: &mut Vec<u8>| out.resize(out.len().next_multiple_of(8), 0);

        out.extend_from_slice(b"SDEX0002");
        let joined = self.strings.join("\n");
        out.extend_from_slice(&(self.strings.len() as u32).to_le_bytes());
        out.extend_from_slice(&(joined.len() as u32).to_le_bytes());
        out.extend_from_slice(joined.as_bytes());
        pad(&mut out);

        out.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        out.extend_from_slice(&(self.swaps.len() as u32).to_le_bytes());
        for b in &self.blocks {
            out.extend_from_slice(&(b.slot as f64).to_le_bytes());
        }
        for b in &self.blocks {
            out.extend_from_slice(&(b.timestamp_ms as f64).to_le_bytes());
        }
        for b in &self.blocks {
            out.extend_from_slice(&(b.swaps.len() as u32).to_le_bytes());
        }
        pad(&mut out);

        for s in &self.swaps {
            out.extend_from_slice(&s.base_amount.to_le_bytes());
        }
        for s in &self.swaps {
            out.extend_from_slice(&s.quote_amount.to_le_bytes());
        }
        let ids: [fn(&Swap) -> u32; 5] = [
            |s| s.dex,
            |s| s.pool,
            |s| s.trader,
            |s| s.token,
            |s| s.quote,
        ];
        for id in ids {
            for s in &self.swaps {
                out.extend_from_slice(&id(s).to_le_bytes());
            }
        }
        out.extend(self.swaps.iter().map(|s| s.buy as u8));
        pad(&mut out);

        std::fs::File::create(path)?.write_all(&out)
    }
}
