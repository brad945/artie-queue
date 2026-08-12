// Package wal implements the append-only write-ahead log that backs every
// queue. There is no database here on purpose: the assignment forbids
// delegating storage, so durability is our problem and this file is where it
// starts.
//
// Record framing:
//
//	[4B length][1B type][4B header crc32c][4B record crc32c][payload]
//
// Two checksums, not one, and the reason is the whole corruption policy.
//
// The record CRC covers length, type and payload — it catches bit rot anywhere
// in the record, including in the length field, which is what tells the reader
// where the *next* record begins. Without that, one flipped bit would silently
// re-frame the entire remainder of the log.
//
// But the record CRC can only be checked after reading `length` bytes, and the
// interesting failure is precisely when `length` is the thing that got
// corrupted: a garbage length runs past the end of the file, which is
// indistinguishable from a write that was interrupted part-way — a torn tail.
// Treating that as torn means truncating, which would throw away every valid
// record after it, with nothing but a warning. That is the silent data loss
// this whole design exists to prevent.
//
// So the header carries its own checksum, verifiable before the payload is
// read. It splits the ambiguous case cleanly:
//
//	header CRC ok, length runs past EOF -> the header committed and the payload
//	                                       did not: a genuine torn tail
//	header CRC bad                      -> the framing itself is damaged: real
//	                                       corruption, refuse to start
//
// Four extra bytes per record to turn "probably fine" into "known".
package wal

import (
	"encoding/binary"
	"hash/crc32"
)

// Type identifies what a record means. Every record in the log is one of
// these; an unrecognised type is treated as corruption rather than skipped.
type Type uint8

const (
	// TypeMeta carries the queue's immutable configuration. It is always the
	// first record in a log, and is rewritten as the first record of every
	// compacted snapshot.
	TypeMeta Type = 1
	// TypeEnqueue carries a full encoded message. During normal operation the
	// message is always fresh (Attempts=0, not dead); in a compaction snapshot
	// the same record type carries live messages with their current state.
	TypeEnqueue Type = 2
	// TypeAck removes a message permanently.
	TypeAck Type = 3
	// TypeNack returns a leased message to the ready set and increments its
	// attempt count.
	TypeNack Type = 4
	// TypeLeaseExpire is written when a visibility deadline passes with no ack.
	// Like TypeNack it increments the attempt count; whether that tips the
	// message into the dead-letter queue is derived deterministically at replay
	// from the queue's immutable max_attempts.
	TypeLeaseExpire Type = 5
	// TypeDedup carries a deduplication entry whose message no longer exists
	// (it was acked, but the dedup window has not closed yet). Only written by
	// compaction; during normal operation dedup state is recovered from the
	// TypeEnqueue records themselves.
	TypeDedup Type = 6
)

func (t Type) String() string {
	switch t {
	case TypeMeta:
		return "META"
	case TypeEnqueue:
		return "ENQUEUE"
	case TypeAck:
		return "ACK"
	case TypeNack:
		return "NACK"
	case TypeLeaseExpire:
		return "LEASE_EXPIRE"
	case TypeDedup:
		return "DEDUP"
	default:
		return "UNKNOWN"
	}
}

func (t Type) valid() bool {
	return t >= TypeMeta && t <= TypeDedup
}

const (
	// HeaderSize is length(4) + type(1) + header crc(4) + record crc(4).
	HeaderSize = 13

	// MaxRecordSize bounds a single payload. A corrupt length field would
	// otherwise convince the reader to allocate an arbitrary amount of memory
	// before it ever gets to check the checksum.
	MaxRecordSize = 64 << 20
)

// crc32c (Castagnoli) rather than IEEE: hardware-accelerated on both arm64 and
// amd64, so checksumming does not become the bottleneck in front of fsync.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Record is a decoded log entry. Offset is the byte position the record was
// read from, which is what corruption reports point at.
type Record struct {
	Type    Type
	Payload []byte
	Offset  int64
}

// EncodedLen reports how many bytes Encode will append for this payload.
func EncodedLen(payload []byte) int { return HeaderSize + len(payload) }

// Encode appends the framed record to dst and returns the extended slice. It
// appends rather than allocating so that a batch of records can be built up in
// one buffer and handed to a single write syscall.
func Encode(dst []byte, t Type, payload []byte) []byte {
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = byte(t)
	binary.LittleEndian.PutUint32(hdr[5:9], headerChecksum(hdr[:]))
	binary.LittleEndian.PutUint32(hdr[9:13], recordChecksum(hdr[:], payload))

	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// headerChecksum covers the framing fields only — length and type — so it can
// be verified before trusting length enough to read the payload.
func headerChecksum(hdr []byte) uint32 {
	return crc32.Checksum(hdr[0:5], crcTable)
}

// recordChecksum covers the framing fields and the payload.
func recordChecksum(hdr []byte, payload []byte) uint32 {
	crc := crc32.Update(0, crcTable, hdr[0:5])
	return crc32.Update(crc, crcTable, payload)
}
