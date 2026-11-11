package tools

import "testing"

func TestGenerateDiffString(t *testing.T) {
	old := "a\nb\nc\nd\ne\nf\ng\nh\ni\nj\n"
	updated := "a\nb\nc\nd\nE\nf\ng\nh\ni\nj\n"
	diff, first := generateDiffString(old, updated, 2)
	want := "    ...\n  3 c\n  4 d\n- 5 e\n+ 5 E\n  6 f\n  7 g\n    ..."
	if diff != want || first != 5 {
		t.Fatalf("diff=%q first=%d", diff, first)
	}
	patch := generateUnifiedPatch("p", old, updated, 2)
	wantPatch := "--- p\n+++ p\n@@ -3,5 +3,5 @@\n c\n d\n-e\n+E\n f\n g\n"
	if patch != wantPatch {
		t.Fatalf("patch=%q", patch)
	}
	if patch := generateUnifiedPatch("p", "x", "y", 4); patch != "--- p\n+++ p\n@@ -1,1 +1,1 @@\n-x\n\\ No newline at end of file\n+y\n\\ No newline at end of file\n" {
		t.Fatalf("patch=%q", patch)
	}
}
