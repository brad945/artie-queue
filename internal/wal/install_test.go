package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// Install must report whether the rename landed, separately from whether it
// returned an error, because the caller has to handle the two cases in
// opposite ways.
//
// Before the rename, nothing has changed and the old log is still live, so the
// caller carries on. After it, the descriptor the caller holds points at an
// unlinked inode: anything appended there is invisible to every future reader,
// so continuing would acknowledge writes that are already lost. Collapsing
// both into a bare error is what let a failed directory fsync turn into silent
// loss of everything written afterwards.
func TestInstallReportsWhetherTheRenameLanded(t *testing.T) {
	t.Run("failure before the rename leaves the old log intact", func(t *testing.T) {
		dir := t.TempDir()
		final := filepath.Join(dir, "wal.log")
		l := writeRecords(t, final, "original-1", "original-2")
		l.Close()
		before, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}

		w, err := NewWriter(final + ".compact")
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(TypeMeta, []byte(`{"name":"q"}`)); err != nil {
			t.Fatal(err)
		}
		// Close the descriptor early so the flush inside Install fails, which
		// happens before any rename can occur.
		if err := w.f.Close(); err != nil {
			t.Fatal(err)
		}

		installed, err := w.Install(final)
		if err == nil {
			t.Fatal("expected Install to fail")
		}
		if installed {
			t.Error("Install reported the rename landed, but it failed before renaming")
		}
		after, err := os.ReadFile(final)
		if err != nil {
			t.Fatal(err)
		}
		if string(before) != string(after) {
			t.Error("the live log was modified by a failed Install")
		}
	})

	t.Run("success reports installed", func(t *testing.T) {
		dir := t.TempDir()
		final := filepath.Join(dir, "wal.log")
		l := writeRecords(t, final, "original")
		l.Close()

		w, err := NewWriter(final + ".compact")
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(TypeMeta, []byte(`{"name":"q"}`)); err != nil {
			t.Fatal(err)
		}
		if err := w.Append(TypeEnqueue, []byte("snapshotted")); err != nil {
			t.Fatal(err)
		}
		size := w.Size()

		installed, err := w.Install(final)
		if err != nil {
			t.Fatalf("Install: %v", err)
		}
		if !installed {
			t.Fatal("Install succeeded but reported the rename did not land")
		}

		recs, res, err := collect(t, final)
		if err != nil {
			t.Fatal(err)
		}
		if len(recs) != 2 || string(recs[1].Payload) != "snapshotted" {
			t.Fatalf("snapshot did not replace the log: %+v", recs)
		}
		if res.ValidBytes != size {
			t.Errorf("valid bytes = %d, want %d", res.ValidBytes, size)
		}
		if _, err := os.Stat(final + ".compact"); !os.IsNotExist(err) {
			t.Error("the temp file survived a successful Install")
		}
	})
}
