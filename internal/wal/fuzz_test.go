package wal

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// FuzzReplay throws arbitrary bytes at the log reader.
//
// Replay is the one place in this codebase that parses input it did not write:
// a log file can be truncated, corrupted, or hand-crafted by anyone with disk
// access, and it is read before the server will serve a single request. A
// panic here is a crash loop on startup that no amount of retrying fixes.
//
// The properties asserted hold for any input at all:
//
//  1. Replay never panics and never hangs.
//  2. Every record handed to the callback has a verified checksum and a known
//     type — nothing unverified reaches queue state.
//  3. ValidBytes never exceeds the file, and a torn tail always starts exactly
//     at ValidBytes.
//  4. Replay is stable: truncating the file to ValidBytes and replaying again
//     yields exactly the same records with no torn tail. This is what startup
//     actually does, so if it were not stable, recovery would not converge.
//
// Run the seed corpus with `go test ./internal/wal`, or fuzz for real with
// `go test ./internal/wal -run FuzzReplay -fuzz FuzzReplay`.
func FuzzReplay(f *testing.F) {
	// Seeds: a valid log, and the interesting damaged shapes.
	valid := Encode(nil, TypeMeta, []byte(`{"name":"q","ordering":"fifo"}`))
	valid = Encode(valid, TypeEnqueue, []byte("payload-one"))
	valid = Encode(valid, TypeAck, []byte("abc123"))
	f.Add(valid)
	f.Add([]byte{})
	f.Add(valid[:len(valid)-1])              // torn tail: one byte short
	f.Add(valid[:HeaderSize])                // header with no payload
	f.Add(valid[:HeaderSize-1])              // partial header
	f.Add(append(bytes.Clone(valid), 0x00))  // one stray byte
	f.Add(Encode(nil, Type(200), []byte{1})) // unknown record type

	corrupt := bytes.Clone(valid)
	corrupt[HeaderSize+2] ^= 0xff // payload bit flip
	f.Add(corrupt)

	badLen := bytes.Clone(valid)
	badLen[1] = 0xff // inflate a length field
	f.Add(badLen)

	f.Fuzz(func(t *testing.T, data []byte) {
		dir := t.TempDir()
		path := filepath.Join(dir, "wal.log")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Skip()
		}

		var seen [][]byte
		var types []Type
		res, err := Replay(path, func(r Record) error {
			if !r.Type.valid() {
				t.Fatalf("an unrecognised record type %d reached the callback", r.Type)
			}
			if r.Offset < 0 || r.Offset >= int64(len(data)) {
				t.Fatalf("record offset %d is outside the file (%d bytes)", r.Offset, len(data))
			}
			seen = append(seen, bytes.Clone(r.Payload))
			types = append(types, r.Type)
			return nil
		})

		if err != nil {
			// The only errors allowed are corruption reports and read errors;
			// either way nothing may claim a valid prefix.
			var ce *CorruptionError
			if errors.As(err, &ce) {
				if ce.Offset < 0 || ce.Offset > int64(len(data)) {
					t.Fatalf("corruption reported at offset %d, outside the %d byte file", ce.Offset, len(data))
				}
				if ce.Reason == "" {
					t.Fatal("corruption error with no reason")
				}
			}
			return
		}

		if res.ValidBytes < 0 || res.ValidBytes > int64(len(data)) {
			t.Fatalf("ValidBytes = %d, file is %d bytes", res.ValidBytes, len(data))
		}
		if res.Torn != nil {
			if res.Torn.Offset != res.ValidBytes {
				t.Fatalf("torn tail starts at %d but ValidBytes is %d", res.Torn.Offset, res.ValidBytes)
			}
			if res.Torn.Bytes <= 0 || res.Torn.Offset+res.Torn.Bytes != int64(len(data)) {
				t.Fatalf("torn tail %+v does not run to the end of a %d byte file", res.Torn, len(data))
			}
		} else if res.ValidBytes != int64(len(data)) {
			t.Fatalf("clean replay consumed %d of %d bytes", res.ValidBytes, len(data))
		}
		if res.Records != len(seen) {
			t.Fatalf("reported %d records, delivered %d", res.Records, len(seen))
		}

		// Stability: truncating to the verified prefix — which is exactly what
		// startup does — must replay identically, with nothing left torn.
		truncated := filepath.Join(dir, "truncated.log")
		if err := os.WriteFile(truncated, data[:res.ValidBytes], 0o644); err != nil {
			t.Skip()
		}
		var again [][]byte
		res2, err2 := Replay(truncated, func(r Record) error {
			again = append(again, bytes.Clone(r.Payload))
			return nil
		})
		if err2 != nil {
			t.Fatalf("replaying the verified prefix failed: %v", err2)
		}
		if res2.Torn != nil {
			t.Fatalf("the verified prefix still reports a torn tail: %+v", res2.Torn)
		}
		if len(again) != len(seen) {
			t.Fatalf("replay is not stable: %d records then %d", len(seen), len(again))
		}
		for i := range seen {
			if !bytes.Equal(seen[i], again[i]) {
				t.Fatalf("record %d differs between replays", i)
			}
		}
	})
}

// FuzzEncodeDecode checks the writer and reader agree for any payload.
func FuzzEncodeDecode(f *testing.F) {
	f.Add([]byte("hello"), uint8(TypeEnqueue))
	f.Add([]byte{}, uint8(TypeAck))
	f.Add(bytes.Repeat([]byte{0xff}, 1000), uint8(TypeMeta))

	f.Fuzz(func(t *testing.T, payload []byte, typ uint8) {
		rt := Type(typ)
		if !rt.valid() {
			t.Skip()
		}
		path := filepath.Join(t.TempDir(), "wal.log")
		if err := os.WriteFile(path, Encode(nil, rt, payload), 0o644); err != nil {
			t.Skip()
		}
		var got []Record
		res, err := Replay(path, func(r Record) error {
			got = append(got, Record{Type: r.Type, Payload: bytes.Clone(r.Payload), Offset: r.Offset})
			return nil
		})
		if err != nil {
			t.Fatalf("a record we just encoded failed to replay: %v", err)
		}
		if res.Torn != nil {
			t.Fatalf("a complete record was read as torn: %+v", res.Torn)
		}
		if len(got) != 1 {
			t.Fatalf("got %d records, want 1", len(got))
		}
		if got[0].Type != rt || !bytes.Equal(got[0].Payload, payload) {
			t.Fatalf("round-trip mismatch: type %v/%v, %d/%d bytes", got[0].Type, rt, len(got[0].Payload), len(payload))
		}
		if want := int64(EncodedLen(payload)); res.ValidBytes != want {
			t.Fatalf("ValidBytes = %d, want %d", res.ValidBytes, want)
		}
	})
}
