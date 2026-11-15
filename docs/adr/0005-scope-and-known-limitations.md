# ADR 0005 — Scope cuts and known limitations

**Status:** accepted

Each item below is a deliberate omission. It notes what would be needed to
lift it.

## Static membership

The member list is fixed at startup. Adding or removing nodes requires
joint consensus or single-server changes (Raft §4), including catch-up of
new members as non-voting learners. The core's quorum computation is
already isolated in one function.

## Snapshots are one message

InstallSnapshot carries the whole state machine in one frame (peer frames
are capped at 512 MiB) and is built in memory on the Raft thread, pausing
it for the duration. Production systems stream snapshots in chunks from a
point-in-time engine snapshot on another thread. The engine's `iter()`
already provides a consistent point-in-time view, which is the hard part.

## No pre-vote

A node rejoining after a partition increments its term and forces the
current leader to step down once (a disruptive but safe election).
Pre-vote (Raft thesis §9.6) would avoid it. Check-quorum is implemented,
which covers the opposite case: a stale leader on the minority side steps
down by itself.

## Peer messages use bincode, the client protocol is hand-written

The client protocol is a public contract, so it is specified byte by byte
and decoded with explicit bounds checks. Peer messages are internal and
versioned together with the binary, so serde + bincode is used there.

## No security on the wire

Neither port authenticates or encrypts. The peer port validates that
messages come from configured member ids and are addressed to this node,
but an attacker on the network can impersonate a member. Deploy on a
private network; mTLS on both ports is the fix.

## Compaction and sessions grow with data

See ADR 0001 for full-merge compaction. Client session records (one per
client id) are never expired; a TTL applied deterministically through the
log would bound them.
