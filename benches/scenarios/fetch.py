#!/usr/bin/env python3
"""Record the fixture of a scenario bench from the public SQD Portal.

    python3 benches/scenarios/fetch.py erc20_balances --out "$SETTLE_FIXTURES"

Writes <out>/<scenario>/rows.bin and manifest.json. The query asks for the
fields that the pipes SDK `evmDecoder` asks for, so the size of each block's
JSON line is the size that a pipes app receives; the harness cuts batches from
these sizes as the pipes SDK does.

While it fetches, it polls the Portal's head and finalized head, at least
`--finality-minutes` long. The tip phase replays that lag.

rows.bin is the magic b"SETFIX01", then each block:
    u64 number, u64 timestamp (s), 32 bytes hash, u32 JSON bytes, u32 rows,
    then each row: the columns of the manifest, fixed width.
All integers are little-endian except the 256-bit ones, which are big-endian
ABI words. Column types: u8 (1 byte), addr (20 bytes), u256 and i256 (32 bytes;
i256 is two's complement).
"""

import argparse
import datetime
import hashlib
import http.client
import json
import os
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
import zlib

PORTAL = "https://portal.sqd.dev"
MAGIC = b"SETFIX01"
USER_AGENT = "settle-bench-fetch/1"

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
USDC_ETHEREUM = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

# Uniswap V3 pool events.
SWAP = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
MINT = "0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"
BURN = "0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"

# The field set of `evmDecoder` in @subsquid/pipes 1.0.0-beta.8.
DECODER_FIELDS = {
    "block": {"number": True, "hash": True, "timestamp": True},
    "transaction": {"from": True, "to": True, "hash": True, "sighash": True},
    "log": {
        "address": True,
        "topics": True,
        "data": True,
        "transactionHash": True,
        "logIndex": True,
        "transactionIndex": True,
    },
}


def word(hex_str):
    raw = bytes.fromhex(hex_str[2:])
    if len(raw) != 32:
        raise ValueError(f"not a 32-byte word: {hex_str}")
    return raw


def decode_transfers(block):
    rows = []
    for log in block.get("logs", []):
        topics = log["topics"]
        # ERC-721 shares the signature and indexes the token id as a fourth topic.
        if len(topics) != 3:
            continue
        rows.append(word(topics[1])[12:] + word(topics[2])[12:] + word(log["data"]))
    return rows


def decode_pool_events(block):
    rows = []
    zero = bytes(32)
    for log in block.get("logs", []):
        topics = log["topics"]
        data = bytes.fromhex(log["data"][2:])
        words = [data[i : i + 32] for i in range(0, len(data), 32)]
        pool = bytes.fromhex(log["address"][2:])
        # Other contracts can share a signature; keep the shapes of V3 only.
        if topics[0] == SWAP and len(topics) == 3 and len(words) == 5:
            # amount0, amount1, sqrtPriceX96, liquidity, tick
            rows.append(b"\x00" + pool + words[0] + words[1] + words[2] + words[4])
        elif topics[0] == MINT and len(topics) == 4 and len(words) == 4:
            # sender, amount, amount0, amount1
            rows.append(b"\x01" + pool + words[2] + words[3] + zero + zero)
        elif topics[0] == BURN and len(topics) == 4 and len(words) == 3:
            # amount, amount0, amount1
            rows.append(b"\x02" + pool + words[1] + words[2] + zero + zero)
    return rows


SCENARIOS = {
    "erc20_balances": {
        "dataset": "ethereum-mainnet",
        "from_block": 26_210_000,
        "to_block": 26_239_999,
        "query": {
            "type": "evm",
            "fields": DECODER_FIELDS,
            "logs": [
                {"address": [USDC_ETHEREUM], "topic0": [TRANSFER], "transaction": True}
            ],
        },
        "columns": [
            {"name": "from", "type": "addr"},
            {"name": "to", "type": "addr"},
            {"name": "value", "type": "u256"},
        ],
        "decode": decode_transfers,
    },
    "uniswap_v3": {
        "dataset": "ethereum-mainnet",
        "from_block": 26_140_000,
        "to_block": 26_239_999,
        "query": {
            "type": "evm",
            "fields": DECODER_FIELDS,
            "logs": [{"topic0": [SWAP, MINT, BURN], "transaction": True}],
        },
        # kind: 0 swap, 1 mint, 2 burn. Mint and burn rows have no price and tick.
        "columns": [
            {"name": "kind", "type": "u8"},
            {"name": "pool", "type": "addr"},
            {"name": "amount0", "type": "i256"},
            {"name": "amount1", "type": "i256"},
            {"name": "sqrt_price_x96", "type": "u256"},
            {"name": "tick", "type": "i256"},
        ],
        "decode": decode_pool_events,
    },
}

RETRY_STATUS = {429, 500, 502, 503, 504, 529}


def log(msg):
    print(f"[fetch] {msg}", file=sys.stderr, flush=True)


def get_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as res:
        return json.load(res)


def stream_lines(url, query):
    """The block lines of one stream request, and the response headers."""
    req = urllib.request.Request(
        url,
        data=json.dumps(query).encode(),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": USER_AGENT,
        },
    )
    res = urllib.request.urlopen(req, timeout=120)
    gzip = res.headers.get("Content-Encoding") == "gzip"
    inflate = zlib.decompressobj(wbits=31) if gzip else None

    def lines():
        tail = b""
        with res:
            while True:
                chunk = res.read(1 << 20)
                if not chunk:
                    break
                if inflate:
                    chunk = inflate.decompress(chunk)
                parts = (tail + chunk).split(b"\n")
                tail = parts.pop()
                yield from (p for p in parts if p)
        if tail:
            yield tail

    return res.status, res.headers, lines()


class FinalitySampler(threading.Thread):
    """Polls (time ms, head, finalized head) every `period` seconds."""

    def __init__(self, dataset, period=2.0):
        super().__init__(daemon=True)
        self.base = f"{PORTAL}/datasets/{dataset}"
        self.period = period
        self.samples = []
        self.stop = threading.Event()

    def run(self):
        while not self.stop.is_set():
            try:
                head = get_json(f"{self.base}/head")["number"]
                finalized = get_json(f"{self.base}/finalized-head")["number"]
                self.samples.append([int(time.time() * 1000), head, finalized])
            except (OSError, ValueError, KeyError):
                pass
            self.stop.wait(self.period)


def fetch(name, out_dir, finality_minutes, limit_blocks=None):
    sc = SCENARIOS[name]
    first, last = sc["from_block"], sc["to_block"]
    if limit_blocks:
        last = min(last, first + limit_blocks - 1)
    url = f"{PORTAL}/datasets/{sc['dataset']}/stream"

    os.makedirs(out_dir, exist_ok=True)
    rows_path = os.path.join(out_dir, "rows.bin")
    tmp_path = rows_path + ".part"

    sampler = FinalitySampler(sc["dataset"])
    sampler.start()
    started = time.time()

    sha = hashlib.sha256()
    finalized_head = None
    blocks = rows = json_bytes = 0
    next_block = first
    delay = 1.0

    with open(tmp_path, "wb") as out:
        def write(data):
            out.write(data)
            sha.update(data)

        write(MAGIC)
        while next_block <= last:
            query = dict(sc["query"], fromBlock=next_block, toBlock=last)
            got = 0
            try:
                status, headers, lines = stream_lines(url, query)
                if status != 200:
                    raise RuntimeError(f"stream answered {status} from block {next_block}")
                if headers.get("X-Sqd-Finalized-Head-Number"):
                    finalized_head = {
                        "number": int(headers["X-Sqd-Finalized-Head-Number"]),
                        "hash": headers["X-Sqd-Finalized-Head-Hash"],
                    }
                for line in lines:
                    block = json.loads(line)
                    header = block["header"]
                    number = header["number"]
                    if number < next_block or number > last:
                        raise RuntimeError(f"block {number} outside {next_block}..{last}")
                    decoded = sc["decode"](block)
                    write(
                        struct.pack(
                            "<QQ32sII",
                            number,
                            header["timestamp"],
                            word(header["hash"]),
                            len(line),
                            len(decoded),
                        )
                    )
                    write(b"".join(decoded))
                    blocks += 1
                    rows += len(decoded)
                    json_bytes += len(line)
                    next_block = number + 1
                    got += 1
                if got == 0:
                    raise RuntimeError(f"stream returned no blocks from block {next_block}")
                delay = 1.0
            except urllib.error.HTTPError as e:
                if e.code not in RETRY_STATUS:
                    raise
                log(f"HTTP {e.code} at block {next_block}, retry in {delay:.0f} s")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue
            # A cut stream: the blocks before the cut are written, so go on
            # from the next one.
            except (OSError, zlib.error, http.client.HTTPException, json.JSONDecodeError) as e:
                log(f"{type(e).__name__} at block {next_block}: {e}; retry in {delay:.0f} s")
                time.sleep(delay)
                delay = min(delay * 2, 30.0)
                continue

            done = (next_block - first) / (last - first + 1)
            log(f"{done:6.1%}  block {next_block - 1}  {blocks} blocks  {rows} rows")

    remaining = finality_minutes * 60 - (time.time() - started)
    if remaining > 0:
        log(f"data done; sampling finality {remaining / 60:.1f} more minutes")
        time.sleep(remaining)
    sampler.stop.set()
    sampler.join()

    if finalized_head is None or finalized_head["number"] < last:
        raise RuntimeError(f"finalized head {finalized_head} is below the last block {last}")

    os.replace(tmp_path, rows_path)
    manifest = {
        "format": 1,
        "scenario": name,
        "dataset": sc["dataset"],
        "portal": PORTAL,
        "from_block": first,
        "to_block": last,
        "query": sc["query"],
        "columns": sc["columns"],
        "blocks": blocks,
        "rows": rows,
        "json_bytes": json_bytes,
        "rows_sha256": sha.hexdigest(),
        "recorded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "finalized_head": finalized_head,
        "finality_samples": sampler.samples,
    }
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=1)
        f.write("\n")
    log(f"wrote {rows_path}: {blocks} blocks, {rows} rows, {len(sampler.samples)} finality samples")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--out", default=os.environ.get("SETTLE_FIXTURES"), help="fixture root (default: $SETTLE_FIXTURES)")
    parser.add_argument("--finality-minutes", type=float, default=15.0)
    parser.add_argument("--limit-blocks", type=int, help="record only the first N blocks (a smoke test)")
    args = parser.parse_args()
    if not args.out:
        parser.error("--out or SETTLE_FIXTURES is required")
    fetch(args.scenario, os.path.join(args.out, args.scenario), args.finality_minutes, args.limit_blocks)


if __name__ == "__main__":
    main()
