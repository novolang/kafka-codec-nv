# Changelog

All notable changes to kafka-codec-nv are recorded here. The format is
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
package follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
with the pre-1.0 rule that a breaking change bumps the MINOR number.

## [0.0.1] — 2026-09-17

**The interface, published before anyone implements it.** Every public
type and function carries its full signature, its effect row and its
doc comment; every body is `todo()`; the release is recorded
`implemented = false`.

### Added

- `kafkaframe` — the load-bearing interface, and the reason the package
  is a state machine rather than a pair of free functions. A Kafka
  response body has no type tag, no length of its own and no version
  field: its shape comes from the API key and version of the request it
  answers, and the wire carries neither. So a `KafkaExchange` keeps a
  table from correlation id to (key, version), and a caller drives it
  with four calls — `send` answers framed bytes, `feed` takes whatever
  a socket read returned however partial, `drain` answers every
  complete response the buffer now holds, `reset` throws the table away
  and hands back what was in flight. Nothing waits, reads, writes or
  sleeps. The one hole in the table is a `Produce` with `acks = 0`,
  which gets no response at all — not an empty one — so `send` does not
  queue it and `expects_response` is public because that is a fact
  about the protocol rather than about this implementation.
- `kafkaprim` — the two spellings, carried rather than passed. Before
  KIP-482 a string is an `int16` length and an array an `int32` count,
  with `-1` for null; from each API's flexible version they are
  unsigned varints holding the count plus one, with `0` for null, and
  every structure ends in a tagged-field section. The switch is per
  message version and applies to every field at once, so
  `KafkaWireStyle` lives on the cursor and on the writer and no other
  module in the package contains an `if` about it.
  `read_tagged_fields` hands the unknown tags back rather than
  discarding them, because a decoder that dropped them strips whatever
  a newer broker added from anything it re-encodes. The unsigned varint
  and the record batch's zigzag one are separate functions with
  separate names, because mixing them produces plausible wrong numbers
  rather than an error.
- `kafkaapi` — the table, and the package's one deliberate refusal.
  `KafkaApiKey` has eleven arms and no twelfth, so an API key this
  release does not encode is `KafkaUnknownApiKey` carrying the number.
  The alternative would be an `Unknown(Int)` arm, and it cannot work: a
  Kafka response body has no length of its own, the frame's size prefix
  covers header and body together, and "skip the rest of the frame" and
  "lose the stream" are the same action. The opposite rule holds for an
  error code, which is total — a client chooses which request it sends
  and does not choose which error it is told about. Also here: the
  per-key flexible floor, the negotiation against a broker's
  `ApiVersions` reply, and the protocol's one header exception, which
  is that an `ApiVersions` response uses header v0 even at its flexible
  versions.
- `kafkarecords` — v2 record batches, with compression declared and
  handed in. A `KafkaCodecSet` is four optional slots of
  caller-supplied `fn(Bytes) -> Result<Bytes, Str>` pairs, so this
  package depends on no compression library: a program that reads only
  uncompressed topics links none, and one that reads zstd chooses its
  own. A batch whose declared codec is absent is
  `KafkaCompressionNotSupplied` naming it, rather than an empty batch —
  a consumer that silently returned no records from a snappy partition
  would look like a consumer that had caught up. The checksum is
  CRC-32C over the compressed bytes, so the order is records,
  compression, header, checksum. `absolute_offset` and
  `absolute_timestamp` exist because everything inside a batch is a
  delta, and `tombstone` is spelled separately from a record with an
  empty value because on a compacted topic the difference is whether
  the key survives.
- `kafkareq` and `kafkaresp` — the eleven requests and their replies as
  values, with one encoder and one decoder between them. A request
  value carries no version, because the version is negotiated per
  connection and the same value is legitimately sent to two brokers at
  two versions during a rolling upgrade. Fields a version lacks are
  dropped, except for three whose absence changes the meaning — a
  transactional id below `Produce` v3, a `rack_id` below `Fetch` v11, a
  group instance id below `JoinGroup` v5 — which are refused by name.
  No response type has a single error field standing for the response,
  because a produce to twelve partitions carries twelve independent
  codes; `errors_of` is the general answer and `first_error` says in
  its name that it is not.
- `kafkahdr` — the four request-header fields and the one response
  field, with the header version read from `kafkaapi` rather than
  computed at each call site, and `check_correlation`, whose refusal is
  fatal to the connection because a length-prefixed stream with no
  magic number cannot be resynchronised.
- `kafkaerror` — fifty-six named broker error codes plus
  `KafkaErrOther`, with three predicates rather than one:
  `is_retriable` says whether the same bytes could succeed,
  `needs_metadata_refresh` says the partition-to-leader map is stale,
  and `needs_coordinator_refresh` says the same about the group
  coordinator, which is a different lookup with a different cache. A
  client with only the first would retry a `NOT_LEADER_OR_FOLLOWER`
  against the broker that just said it is not the leader, forever.
  Sixteen codec faults beside them, each a statement about bytes.

### Known

- `novo test` is red, and that is this release's expected state: every
  assertion in the five suites reaches `not implemented:
  kafka-codec-nv.<module>.<fn>`.
- **There are no captured frames in the test suite.** The assertions
  are about the shape of the API, not about bytes. A protocol codec
  cannot be trusted without a version matrix run against real frames,
  and the implementation lane has to add one — the Kafka distribution's
  `clients/src/test/resources` and a packet capture of a handshake are
  both usable.
- **CRC-32C comes from crc-nv 0.1.4 and nothing else is needed to
  implement this package.** Unlike a package waiting on a primitive
  that does not exist, every byte here can be produced with what is on
  the registry today.
- **No SASL, no transactions, no administration.** Twenty-two API keys
  are named in the README's "What is not included", and each is a typed
  refusal rather than a silent skip. A broker that requires SASL cannot
  be authenticated by anything built on this release.
- **Only record batches, magic `2`.** The pre-0.11 message sets are
  `KafkaBadMagic`.
