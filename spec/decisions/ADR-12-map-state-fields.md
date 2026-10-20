# ADR-12 — Map state fields

Status: Accepted (2026-10-20)

## Context

A reducer state (DEF-26) is a tuple of values. A pipeline that keeps a map in a group, for example the positions of a wallet by token, has kept it in one JSON field. Then in each block, for each group that the block touches, Settle:

- copies the whole map into the undo entry of the group, and drops the copy at finality;
- encodes the whole map into the checkpoint;
- converts all of it to Lua or JS values and back.

A change of one entry costs as much as all the entries of the group, and the cost grows with the data. On the Solana DEX bench the JSON layout fell below real time.

A price map for cross rates (one group, an entry for each token) has the same problem at a larger scale: every swap changes it.

The alternatives:

- A proxy that tracks the keys that a body reads and writes in a JSON field. Each runtime needs its own proxy, and a body that keeps a table from the map changes it without the proxy.
- One group for each entry, with scalar state. This works when each row touches one entry. It does not work when a row reads several entries, as a price chain does.

## Decision

1. A state field can be `Map(String, Type)`: entries by String key, each with a value of `Type`. It takes no DEFAULT; a new group has no entries. An entry never holds Null; a set to Null removes it.
2. A body reads and writes entries one at a time: `get`, `set` and `delete` by key. In Lua, `state.<field>:get(key)` returns a copy, so a changed table needs a `set`. A Map field takes no index. A Rust closure uses `StateCtx::map`.
3. Settle keeps an undo journal of the changed entries for each group, not a copy of the map. A rollback applies the journal in reverse. Finality drops the journal and marks the keys for the next checkpoint.
4. Each entry is a separate stored record. A checkpoint writes the entries that changed, each as of finality, and deletes the entries that were absent then. A group loads its entries on its first row.
5. A reducer can have no GROUP BY in the schema text; it then has one group.

## Consequences

- A change of one entry costs O(1) in memory, at rollback and at a checkpoint, whatever the size of the map.
- A JS or WASM body makes one host call for each get, set or delete. The entries stay in Settle, so the journal and the checkpoint are the same for each runtime.
- A body cannot list the entries of a Map field yet. A pipeline that needs that keeps a JSON field or a group per entry.
- A group with many entries loads all of them on its first row after an open.
- Shapes: DEF-24, DEF-26, REQ-23, IB-63, IB-64.
