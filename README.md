# 🔥 GitHub Description

> **FlashLog** — a lightning-fast, append-only key-value engine.
> Memory-indexed. Crash-safe. Built for hot data and cold latency.

---

# 💎 README.md

# ⚡ FlashLog

> Append boldly. Read instantly.

FlashLog is a minimalist, log-structured key-value storage engine inspired by Bitcask.  
It is built to be brutally fast, simple to understand, and rock-solid under pressure.

No trees. No page caches. No excuses.  
Just sequential writes, in-memory indexes, and predictable performance.

---

## ✨ Why FlashLog?

FlashLog follows one simple rule:

> **Sequential writes + RAM indexes = dangerously fast storage.**

By keeping all keys in memory and writing only append-only logs to disk, FlashLog achieves
high throughput, low latency, and extremely predictable behavior — even under heavy load.

---

## 🚀 Features

- ⚡ **Blazing-fast writes** — pure sequential I/O
- 🧠 **All keys indexed in RAM** — one disk seek per read
- 🧱 **Crash-safe** — log replay recovery
- 🧹 **Automatic log compaction**
- 🔬 **Simple on-disk format** — easy to inspect & debug
- 📦 **Embeddable engine** — drop into any project
- 🪶 **Tiny codebase** — made to be understood

---

## 🗃 Storage Model

Everything is written to append-only log segments:

```

| CRC | Timestamp | KeySize | ValueSize | Key | Value |

```

New versions are appended.  
Old versions are garbage-collected during compaction.

Time is the index.

---

## 🧠 Read & Write Paths

### Write Path

```

Client → Append Log → Update In-Memory Index

```

### Read Path

```

Client → RAM Index → Single Disk Seek → Value

```

One seek. One read. No wandering.

---

## 🧹 Compaction

FlashLog periodically rewrites only the _latest_ version of every key into new segments,
reclaiming space and keeping the store lean and fast.

Old segments quietly vanish.

---

## 🧪 Perfect For

- Caches
- Embedded databases
- Time-series stores
- Event logs
- Learning storage internals
- Systems that demand speed and simplicity

---

## 🛠 Philosophy

FlashLog is intentionally:

- Minimal
- Deterministic
- Hackable
- Transparent

It’s meant to be understood, not feared.

---

## 🖤 Taglines

- _Append boldly._
- _Fast logs. Cold latency._
- _Where your data strikes first._
- _Time is the index._
- _Built for speed. Designed for trust._

---

## ⚙️ Status

FlashLog is an educational engine under active development.  
Not production-ready — but dangerously fun.
