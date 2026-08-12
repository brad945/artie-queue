package queue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Wire encoding for record payloads.
//
// Messages use a hand-rolled binary layout because they are the hot path and
// every enqueue pays for this encoding. META uses JSON because it is written
// once per queue (and once per compaction), it is the field set most likely to
// grow, and being able to read a queue's configuration out of a hexdump is
// worth more there than a few saved bytes.

// Version 2 widened Priority from 32 to 64 bits. In v1 a priority outside the
// int32 range was accepted, acknowledged, and then silently reinterpreted on
// replay — so a running queue ordered on the value the client sent while a
// recovered queue ordered on its low 32 bits, sign-extended. Rejecting large
// priorities would have fixed the symptom; making the stored field the same
// width as the in-memory one removes the failure mode, so there is no rule
// left to forget. A v1 log is refused loudly rather than misread.
const messageFormatVersion = 2

var errShortPayload = errors.New("queue: truncated record payload")

// encodeMessage serialises a message. Attempts and the dead flag are included
// because compaction re-emits live messages as ENQUEUE records and must not
// lose how many times they have already been tried.
func encodeMessage(m *Message) []byte {
	buf := make([]byte, 0, 50+len(m.ID)+len(m.DedupID)+len(m.Payload))
	buf = append(buf, messageFormatVersion)
	buf = binary.LittleEndian.AppendUint64(buf, m.Seq)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(int64(m.Priority)))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.VisibleAt.UnixNano()))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.CreatedAt.UnixNano()))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(m.Attempts))
	var flags byte
	if m.state == stateDead {
		flags |= 1
	}
	buf = append(buf, flags)
	buf = appendString16(buf, m.ID)
	buf = appendString16(buf, m.DedupID)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(m.Payload)))
	return append(buf, m.Payload...)
}

func decodeMessage(b []byte) (*Message, error) {
	if len(b) < 38 {
		return nil, errShortPayload
	}
	if b[0] != messageFormatVersion {
		return nil, fmt.Errorf("queue: unsupported message format version %d (this build writes version %d)",
			b[0], messageFormatVersion)
	}
	m := &Message{}
	p := 1
	m.Seq = binary.LittleEndian.Uint64(b[p:])
	p += 8
	priority := int64(binary.LittleEndian.Uint64(b[p:]))
	if int64(int(priority)) != priority {
		// Only reachable on a 32-bit build reading a log written by a 64-bit
		// one. Refusing beats silently reordering the queue.
		return nil, fmt.Errorf("queue: priority %d does not fit this platform's int", priority)
	}
	m.Priority = int(priority)
	p += 8
	m.VisibleAt = time.Unix(0, int64(binary.LittleEndian.Uint64(b[p:])))
	p += 8
	m.CreatedAt = time.Unix(0, int64(binary.LittleEndian.Uint64(b[p:])))
	p += 8
	m.Attempts = int(binary.LittleEndian.Uint32(b[p:]))
	p += 4
	flags := b[p]
	p++

	var err error
	if m.ID, p, err = readString16(b, p); err != nil {
		return nil, err
	}
	if m.DedupID, p, err = readString16(b, p); err != nil {
		return nil, err
	}
	if p+4 > len(b) {
		return nil, errShortPayload
	}
	n := int(binary.LittleEndian.Uint32(b[p:]))
	p += 4
	if p+n > len(b) {
		return nil, errShortPayload
	}
	m.Payload = append([]byte(nil), b[p:p+n]...)

	if m.ID == "" {
		return nil, errors.New("queue: message record has empty id")
	}
	if flags&1 != 0 {
		m.state = stateDead
	}
	m.effPriority = m.Priority
	return m, nil
}

// dedupEntry remembers that a DedupID was used, and by which message, until
// the window closes. It outlives the message itself: the whole point is to
// reject a producer's retry even after the original has been acked.
type dedupEntry struct {
	DedupID   string
	MessageID string
	ExpiresAt time.Time
}

func encodeDedup(e dedupEntry) []byte {
	buf := make([]byte, 0, 12+len(e.DedupID)+len(e.MessageID))
	buf = appendString16(buf, e.DedupID)
	buf = appendString16(buf, e.MessageID)
	return binary.LittleEndian.AppendUint64(buf, uint64(e.ExpiresAt.UnixNano()))
}

func decodeDedup(b []byte) (dedupEntry, error) {
	var e dedupEntry
	var err error
	p := 0
	if e.DedupID, p, err = readString16(b, p); err != nil {
		return e, err
	}
	if e.MessageID, p, err = readString16(b, p); err != nil {
		return e, err
	}
	if p+8 > len(b) {
		return e, errShortPayload
	}
	e.ExpiresAt = time.Unix(0, int64(binary.LittleEndian.Uint64(b[p:])))
	return e, nil
}

func encodeConfig(c Config) ([]byte, error) { return json.Marshal(c) }

func decodeConfig(b []byte) (Config, error) {
	var c Config
	err := json.Unmarshal(b, &c)
	return c, err
}

func appendString16(dst []byte, s string) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func readString16(b []byte, p int) (string, int, error) {
	if p+2 > len(b) {
		return "", p, errShortPayload
	}
	n := int(binary.LittleEndian.Uint16(b[p:]))
	p += 2
	if p+n > len(b) {
		return "", p, errShortPayload
	}
	return string(b[p : p+n]), p + n, nil
}
