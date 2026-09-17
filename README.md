# kafka-codec-nv

Apache Kafka is a distributed log. Clients talk to its brokers over a
binary TCP protocol, described in the
[Kafka protocol guide](https://kafka.apache.org/protocol.html). This
package reads and writes that protocol in novo-lang, and performs no
input or output while doing it: bytes arrive as arguments and leave as
return values. The client that owns the sockets is
[kafka-nv](https://novo-lang.org/packages/kafka-nv), which is built on
this package.

**Status: NOT IMPLEMENTED — interface only.** Every function is
declared with its full signature, but every body is a `todo()` that
panics when called. The package is published so its design can be
reviewed and depended on before it is implemented. Version 0.1.0 will
be the first working release.

## What the protocol is

A Kafka connection carries **requests** from the client and
**responses** from the broker, one after another. Each is a **frame**:
a four-byte big-endian length, then that many bytes. The length covers
everything after itself, and there is no other delimiter and no magic
number.

Inside a frame is a **header** and then a **body**. A request header
names four things: the **API key**, a number saying which request this
is; the **API version**, a number saying which revision of that
request; a **correlation id**, chosen by the client; and a **client
id**, a name the broker records. A response header is the correlation
id the request carried, echoed back.

There are about seventy API keys. This package encodes eleven of them,
listed under [What the package contains](#what-the-package-contains).

A response body's shape is not in its bytes. It has no type tag, no
length of its own and no version field: it is whatever the API key and
version of the request said it would be. A decoder therefore has to
remember what it sent, which is why this package is a state machine
rather than a pair of functions.

Every API is versioned separately, and a broker tells a client which
versions it speaks when the client sends an `ApiVersions` request. The
client then uses, per API, the highest version both sides support. That
is **negotiation**, and it is the first thing that happens on every
connection.

From a certain version of each API — a different one for each — the
protocol uses a second spelling of its variable-length fields, called
the **flexible** spelling and introduced in
[KIP-482](https://cwiki.apache.org/confluence/display/KAFKA/KIP-482%3A+The+Kafka+Protocol+should+Support+Optional+Tagged+Fields).
A string stops being a two-byte length followed by UTF-8 and becomes an
**unsigned varint** — seven bits of the number per byte, low group
first — holding the byte count plus one. Arrays change the same way.
Every structure also gains a **tagged-field section** at its end: a
count, then that many numbered fields, which lets a later release add a
field without a version bump.

Records travel in **record batches**. A batch is the unit a broker
stores: a header, and then the records, each carrying an **offset
delta** and a **timestamp delta** relative to the batch rather than
absolute values. The batch header holds a **CRC-32C** checksum — the
Castagnoli polynomial, not the CRC-32 of zip — over everything after
the checksum field. Three bits of the header's attributes say which of
five **compression codecs** the record bytes are in.

| Codec | Attribute bits | Novo package |
| --- | --- | --- |
| none | 0 | — |
| gzip | 1 | flate-nv |
| snappy | 2 | snappy-nv |
| lz4 | 3 | lz4-nv |
| zstd | 4 | zstd-nv |

This package depends on none of them. A caller that reads or writes a
compressed topic passes the compression and decompression functions in
as values.

## Install

```
novo pkg add kafka-codec-nv
```

## Example

```novo
use std.bytes
use kafkaerror
use kafkaframe
use kafkarecords
use kafkareq

fn main() [io]
    // A connection's protocol state. It owns no socket: the caller
    // writes the bytes it produces and feeds it the bytes it reads.
    let conn = kafkaframe.exchange("orders-indexer")

    // The first request on every connection asks what the broker
    // speaks.
    let ask = KafkaApiVersionsReq(
                  kafkareq.api_versions_request("novo-kafka-nv", "0.0.1"))

    // Build it. `out.bytes` is framed and ready to write to a socket.
    match kafkaframe.send(conn, ask, kafkarecords.no_codecs())
        Err(e)  => println("could not build it: ${kafkaerror.fault_message(e)}")
        Ok(out) =>
            println("write ${bytes.len(out.bytes)} bytes")

            // Whatever the socket read comes back in here, however
            // partial. Nothing is decoded yet.
            let fed = kafkaframe.feed(out.exchange, bytes.zeros(0))

            // Every complete response the buffer now holds. An empty
            // list means the reply has not arrived yet.
            match kafkaframe.drain(fed, kafkarecords.no_codecs())
                Err(e) => println("the connection is lost: ${kafkaerror.fault_message(e)}")
                Ok(d)  => println("${list.len(d.answers)} responses")
```

Build and test with `novo pkg build` and `novo test`. Today `novo test`
fails on purpose: every test reaches a
`not implemented: kafka-codec-nv.<module>.<fn>` panic. The tests are
the specification the implementation will have to satisfy.

## What the package contains

| Module | Contents |
| --- | --- |
| `kafkaerror` | The broker's error codes, what each one means a client should do next, and the faults this codec raises about bytes. |
| `kafkaprim` | The primitive types: fixed-width integers, both varint encodings, strings and arrays in both spellings, and tagged fields. |
| `kafkaapi` | Which requests this package encodes, at which versions, in which spelling, behind which header version. |
| `kafkahdr` | The request and response headers, and the correlation-id check. |
| `kafkarecords` | Record batches: the header, the checksum, the attribute bits, the records inside, control batches, and the compression codecs a caller supplies. |
| `kafkareq` | The eleven requests as values, and the one function that turns any of them into bytes. |
| `kafkaresp` | The eleven responses as values, and the one function that reads any of them out of bytes. |
| `kafkaframe` | The state machine: the four-byte framing, the table of outstanding requests, and the four calls a caller drives it with. |

The eleven requests are `ApiVersions`, `Metadata`, `Produce`, `Fetch`,
`ListOffsets`, `FindCoordinator`, `JoinGroup`, `SyncGroup`,
`Heartbeat`, `OffsetCommit` and `OffsetFetch`. Between them they are
enough to produce to a topic, read from one, and belong to a consumer
group.

## How to choose an entry point

**`kafkaframe` is the entry point for a client.** It holds the four
calls — `send`, `feed`, `drain` and `reset` — and it is the only thing
that knows which request each outstanding correlation id belongs to. A
program talking to a broker uses this and nothing below it.

**`kafkareq.encode_request` and `kafkaresp.decode_response` are for a
caller that owns the correlation itself.** A proxy forwarding frames it
did not originate already knows the key and version from the request it
is relaying, so it needs no table.

**`kafkarecords` is for a caller that has record bytes and no frame.**
A tool reading a broker's log segments off disk, or a fixture building
a batch to feed to something else, uses it on its own.

**`kafkaprim` is for reading a field the eleven requests do not
contain.** Its cursor and writer are the ones every other module here
uses, so a caller decoding a twelfth API by hand gets the same varints
and the same compact strings.

## The rules a user needs

1. **A response cannot be decoded without knowing what was requested.**
   The body carries no key and no version. `kafkaframe` keeps the table
   that remembers; a caller that bypasses it passes the key and version
   to `kafkaresp.decode_response` itself. Kafka protocol guide,
   "Requests and Responses".
2. **An API key this package does not encode is refused**, as
   `kafkaerror.KafkaUnknownApiKey` carrying the number.
   `kafkaapi.unencoded_api_keys()` lists them.
3. **An error code this package does not name is not refused.** It
   arrives as `KafkaErrOther` carrying the number. Brokers add codes,
   and a client that refused an unfamiliar one could not talk to a
   newer broker.
4. **Which spelling a message uses depends on its own version.**
   `kafkaapi.flexible_from` gives the first flexible version of each
   API: `Metadata` at 9, `Produce` at 9, `Fetch` at 12, `Heartbeat` at
   4, `ApiVersions` at 3. KIP-482.
5. **The `ApiVersions` response header is version 0 at every version**,
   including the flexible ones, while every other flexible response
   uses header version 1. A decoder that applies the general rule reads
   a tagged-field count that is not there, and every later field is
   wrong. `kafkaapi.header_response_version` holds the exception.
6. **Null and empty are different values.** A null topic array in a
   `Metadata` request asks for every topic in the cluster; an empty one
   asks for none. A null record value on a compacted topic deletes the
   key; an empty one stores no bytes under it. Every function here that
   can produce either says which it got.
7. **A produce request with `acks = 0` gets no response at all.** Not
   an empty one — none. `kafkaframe.expects_response` answers `false`
   for it, and `kafkaframe.send` does not add it to the outstanding
   table. Kafka protocol guide, "Produce API".
8. **A record's offset is the batch's base offset plus the record's
   offset delta.** The delta alone is `0` for the first record of every
   batch. `kafkarecords.absolute_offset` does the addition.
9. **A committed offset is the next offset to read, not the last one
   read.** `kafkarecords.next_offset` gives the right value. A consumer
   that commits the last offset it processed reprocesses one record per
   partition after every restart.
10. **An error code is per partition.** A produce or fetch covering
    twelve partitions carries twelve independent codes.
    `kafkaresp.errors_of` walks them all; `kafkaresp.first_error` is a
    convenience for a caller that asked about a single partition.
11. **A compression codec is supplied by the caller.** A batch whose
    attributes declare a codec that is not in the `KafkaCodecSet` is
    `kafkaerror.KafkaCompressionNotSupplied` naming the codec, not an
    empty batch.
12. **A record batch's checksum covers the compressed bytes.** Compress
    the records, then fill in the header, then checksum. Kafka protocol
    guide, "Record Batch".
13. **The last record batch in a fetch response may be incomplete.** A
    broker truncates the response at the byte limit rather than at a
    batch boundary. `kafkarecords.decode_batches` drops a trailing
    partial batch; treating it as corruption stalls any partition whose
    next batch is larger than `max_bytes`.
14. **A control batch carries no application data.** It is a
    transaction marker written by the coordinator, and
    `kafkarecords.is_control_batch` is the check.
15. **`throttle_time_ms` in a response is time the broker has already
    waited.** It is telling the client to slow down. A client that
    ignores it keeps exceeding the quota; one that then sleeps for the
    same length has delayed itself twice.
16. **A correlation-id mismatch means the connection must be closed.**
    The framing is a length prefix with no magic number, so a stream
    whose ordering has been lost cannot be resynchronised.
    `kafkaerror.is_fatal_to_connection` says which faults are like
    that.
17. **The current time is always an argument.** This package declares
    no effects, so a batch's base timestamp and each record's timestamp
    are millisecond counts the caller read. `kafkarecords.record_at`
    takes one.
18. **Three fields are refused rather than dropped at a version that
    cannot carry them**: a transactional id on a `Produce` below v3, a
    `rack_id` on a `Fetch` below v11, and a group instance id on a
    `JoinGroup` below v5. Each changes where records go or which member
    is fenced. Every other field a version lacks is simply omitted.

## What is not included

- **The other API keys.** `LeaveGroup`, `DescribeGroups`,
  `ListGroups`, `SaslHandshake`, `SaslAuthenticate`, `CreateTopics`,
  `DeleteTopics`, `CreatePartitions`, `DescribeConfigs`,
  `AlterConfigs`, `DeleteGroups`, `DeleteRecords`,
  `OffsetForLeaderEpoch`, `InitProducerId`, `AddPartitionsToTxn`,
  `AddOffsetsToTxn`, `EndTxn`, `TxnOffsetCommit`, `WriteTxnMarkers`,
  `DescribeCluster`, `DescribeProducers` and `ConsumerGroupHeartbeat`
  are not encoded. Each is a typed refusal from
  `kafkaapi.api_key_of_number` naming the number, rather than a frame
  this package skips, because a response body has no length of its own
  and skipping one loses the stream.
  `kafkaapi.unencoded_api_keys()` returns the list.
- **Transactions.** The transactional request set is in the list above,
  so an exactly-once producer cannot be built on this release. A batch
  can still declare itself transactional, and a consumer can still read
  the control batches a transaction leaves behind and skip them.
- **Administration.** Creating and deleting topics, reading and
  altering configuration, and describing groups are in the list above.
- **SASL.** `SaslHandshake` and `SaslAuthenticate` are not encoded, so
  a connection to a broker that requires SASL cannot be authenticated.
  Kafka's SASL exchange is a pair of ordinary requests, so adding them
  is a later release of this package rather than a different package.
- **The compression codecs themselves.** They are flate-nv, snappy-nv,
  lz4-nv and zstd-nv, and a caller hands the functions in. See rule 11.
- **The pre-0.11 message sets.** Magic bytes `0` and `1` are refused as
  `kafkaerror.KafkaBadMagic`. Only record batches, magic `2`, are read.
  A broker serving a topic still stored in the old format needs a
  different decoder.
- **The consumer group assignment formats.** `range`, `roundrobin`,
  `sticky` and `cooperative-sticky` decide which member reads which
  partition. This package carries the bytes of a subscription and of an
  assignment without interpreting them, because the assignment is
  computed by whichever member the coordinator made leader, and that
  computation belongs to the program doing it. kafka-nv encodes the
  standard formats.
- **A correction for the LZ4 header-checksum bug.** Kafka's LZ4 framing
  before message format v1 wrote an incorrect header checksum, and
  brokers accept both spellings. The codec functions here receive
  exactly the bytes from the batch, and their output goes in exactly as
  returned.
- **A socket, a clock and a retry policy.** See kafka-nv.

## Related packages

- [kafka-nv](https://novo-lang.org/packages/kafka-nv) is the client:
  connections, a producer with batching, a consumer with group
  membership, and administration, all over this package. Take that one
  to talk to a cluster. Take this one to read a captured frame, write a
  broker test double, or build something that is not a client.
- [crc-nv](https://novo-lang.org/packages/crc-nv) supplies the CRC-32C
  a record batch carries.
- [flate-nv](https://novo-lang.org/packages/flate-nv),
  [snappy-nv](https://novo-lang.org/packages/snappy-nv),
  [lz4-nv](https://novo-lang.org/packages/lz4-nv) and
  [zstd-nv](https://novo-lang.org/packages/zstd-nv) are the four
  compression codecs. A caller that reads compressed topics takes the
  ones it needs and passes them to `kafkarecords.with_codec`.
- [redis-nv](https://novo-lang.org/packages/redis-nv) and
  [postgres-nv](https://novo-lang.org/packages/postgres-nv) are the
  other two binary-protocol clients on the registry. Each keeps its
  codec as a module rather than as a separate package.

## Tests

The suites are written against the signatures and are red until the
bodies land. `kafkaprim_tests` asserts that the wire style is carried
by the cursor and that null and empty stay distinct. `kafkaapi_tests`
asserts the version table, the `ApiVersions` header exception, and that
an unknown API key is refused while an unknown error code is not.
`kafkarecords_tests` asserts the offset arithmetic, the tombstone, and
that a missing codec is named. `kafkaframe_tests` asserts that a short
read is not an error and that a produce with `acks = 0` is never
queued. `kafkacover_tests` states what every remaining public function
answers.

There are no captured frames in the suite yet. The implementation
should add them: the Kafka distribution's own
`clients/src/test/resources` and a packet capture of a broker handshake
are both usable as fixtures, and a version matrix is the one thing a
protocol codec cannot be trusted on without them.

## Implementation status

| Module | Status |
| --- | --- |
| `kafkaerror` | Declared. Every body is a `todo()`. |
| `kafkaprim` | Declared. Every body is a `todo()`. |
| `kafkaapi` | Declared. Every body is a `todo()`. |
| `kafkahdr` | Declared. Every body is a `todo()`. |
| `kafkarecords` | Declared. Every body is a `todo()`. |
| `kafkareq` | Declared. Every body is a `todo()`. |
| `kafkaresp` | Declared. Every body is a `todo()`. |
| `kafkaframe` | Declared. Every body is a `todo()`. |

## Licence

Apache-2.0. See [LICENSE](LICENSE).
