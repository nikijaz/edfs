# edfs

An ephemeral distributed file system written in Rust. Files written to a FUSE
mount are split into content-addressed chunks and replicated across peers
discovered on the local network. Everything lives in memory and metadata
converges through CRDTs.

## Installation

Needs Linux with FUSE support (`/dev/fuse`) and a recent Rust toolchain (edition
2024).

```bash
git clone https://github.com/nikijaz/edfs.git
cd edfs/
cargo build --release
```

## Usage

Start a few nodes that share the same secret:

```bash
./target/release/edfs secret --mountpoint ./1
./target/release/edfs secret --mountpoint ./2
./target/release/edfs secret --mountpoint ./3
```

Files created in one mount show up in the others. Every chunk is held by exactly
two peers, so the filesystem survives individual node crashes. Kill any node and
the rest keep reading.

```bash
echo "hello" > ./1/hello.txt
cat ./2/hello.txt # hello
```

## Architecture

The codebase has three layers, top to bottom:

- **FUSE** (`src/fuse/`) implements the kernel FUSE protocol. Translates VFS
  operations into filesystem calls, maps inodes to node ids and buffers open
  files so reads and writes work on a consistent snapshot.
- **Core** (`src/filesystem/`) implements the in-memory `FileSystem`, made of a
  CRDT tree for metadata and a content-addressed chunk store for data.
- **Network** (`src/network/`) implements a libp2p swarm driven by a dispatcher
  that routes events to different handlers: mDNS discovery, tree sync, chunk
  retrieval, etc.

`src/port.rs` links them. It defines the `FileSystemGateway` trait,
which FUSE calls into and the network side implements. That way FUSE never sees
the network and the network never sees FUSE.

## Technologies Used

**Rust** is the implementation language, with **tokio** as the asyncruntime.
**libp2p** provides the networking stack: TCP transport with **Noise**
encryption and **yamux** multiplexing, **mDNS** for peer discovery,
**gossipsub** for metadata broadcast and a **CBOR** request-response protocol
for chunk and tree sync. **async-fuser** implements the FUSE mount, **clap**
powers the CLI.

On the algorithm side, files are split with **FastCDC** content-defined chunking
and every chunk is addressed by its **SHA-256** hash. Metadata lives in a **CRDT
tree** ordered by **hybrid logical clocks**, so concurrent edits converge
without coordination. Replica placement uses **rendezvous hashing** to
deterministically pick the two holders of each chunk and reads go through an
**LRU cache**.
