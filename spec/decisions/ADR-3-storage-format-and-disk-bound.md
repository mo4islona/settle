# ADR-3 — The storage format and the disk bound

Status: Accepted (historical)

This record holds four related decisions about the stored format and the space on disk. Each part gives its context, its decision and its consequences.

## 1. Positional storage with a format version and stored layouts

**Context.** Settle first stored rows and state with named keys in MessagePack. A positional binary format made raw ingestion 17% faster and rollback 15% faster, and later positional records cut instructions by 28% on a view benchmark. A positional value has no name, so a schema with another column order reads it wrong without an error. A review found exactly that: a column swap read 11 as 101 (0a9ca0a).

**Decision.** Settle stores rows, reducer state and view state by position. The store holds a format version and, for each table, reducer and view, its layout as tagged JSON. An open refuses a version that differs, data without a version, and a layout that differs. The format version and the layouts go into the store with the first data commit. So a database has a version exactly when it has data.

**Consequences.**

- Reads and writes are fast, and a wrong layout can never read silently.
- A layout change needs a new object name or a new database. Settle has no migration (CN-31).
- Reducer bodies, defaults and output names can change freely, because they are not in the layout.
- The virtual flag and the runtime kind are not in the layout either, which is too loose (GAP-42).
- Shapes: REQ-25, WP-51, CN-30, CN-32, INV-45, NG-4.

## 2. Format version 1 and no migration before a stored release

**Context.** Several format changes happened during development. Each one could have had its own version number and a migration. No released database stored data with an older format at the time of the decision (483d627).

**Decision.** The format version starts again at 1. A format change before the first release that stores data needs no migration. A development database from an earlier build is deleted, not converted.

**Consequences.**

- The code carries no migration paths.
- A development database of an older build can open as version 1 and read wrong. The team deletes such databases.
- After the first release that stores data, each format change needs a new version and a decision about migration. This part then needs a successor.
- Shapes: P-FORMAT-VERSION, CN-31, FM-75, NG-4.

## 3. Raw-row purge with a retention margin for every table

**Context.** Settle never deleted raw rows, virtual tables included. Raw rows were 79% to 86% of the bytes on disk. With two or more raw tables, each compaction of raw rows rewrote the whole history, because the keys start with the table name. Nobody reads old raw rows: a rollback never goes below finality, and a replay starts above the durable watermark (4f9682c).

**Decision.** Each checkpoint deletes the stored raw rows of the blocks at or below `D − P-RAW-RETENTION`, for every table, in steps of a tenth of the margin. The default margin is 1,000 blocks. The largest margin value turns the purge off.

**Consequences.**

- The raw rows on disk stay bounded by the margin and the finality lag. Raw compaction input fell from 246.5 MB to 14.4 MB in a two-table backfill.
- Settle is not an archive of the source (NG-8, RS-5).
- The purge emits no change records; the target keeps its copy (RS-3).
- Shapes: RS-1, RS-2, RS-3, REQ-44, P-RAW-RETENTION.

## 4. Compaction stays on, and the log has a bound

**Context.** A configuration switch could turn RocksDB compaction off. A probe showed that compaction used less than 1% of the process CPU, at a write amplification of 1.1. Without it, the view and reducer column families held 12 to 18 times their live data. The process also hit the limit of open files after about 1,000 files. A separate probe showed that the reopen time was the replay of the write-ahead log. At the tip, 2.7 GB of log took 3.8 s, because rarely written column families kept old log files alive (7fecaf8, ee19974).

**Decision.** Compaction stays on, and the switch is removed. The total log size is bounded at P-WAL-BOUND, 256 MiB.

**Consequences.**

- The disk stays within a small factor of the live data (RS-21).
- A reopen at the tip took 0.6 s in place of 3.7 s, at the same ingest speed (LIV-3).
- Shapes: RS-21, PF-5, REQ-43, REQ-44, HZ-5, HZ-6.
