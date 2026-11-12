package durable

import (
	"context"
	"errors"
	"testing"
)

func partsOf(parts []ContentPart) []string {
	var out []string
	for _, part := range parts {
		if part.Image != nil {
			out = append(out, "img:"+part.Image.Data)
		} else {
			out = append(out, part.Text)
		}
	}
	return out
}

func TestOrderedContentPlacesImagesAtTheirOffsets(t *testing.T) {
	a, b := ImageContent{MimeType: "image/png", Data: "a"}, ImageContent{MimeType: "image/png", Data: "b"}
	for _, tc := range []struct {
		name    string
		content string
		offsets []int
		want    []string
	}{
		{"after the text", "hi", nil, []string{"hi", "img:a", "img:b"}},
		{"interleaved", "one two", []int{0, 4}, []string{"img:a", "one ", "img:b", "two"}},
		{"together", "x", []int{1, 1}, []string{"x", "img:a", "img:b"}},
		{"clamped past the text", "x", []int{0, 99}, []string{"img:a", "x", "img:b"}},
		{"inside a rune", "é", []int{1, 1}, []string{"é", "img:a", "img:b"}},
		{"a count mismatch puts them after", "x", []int{0}, []string{"x", "img:a", "img:b"}},
	} {
		if got := partsOf(OrderedContent(tc.content, []ImageContent{a, b}, tc.offsets)); !equalStrings(got, tc.want) {
			t.Errorf("%s: parts=%q, want %q", tc.name, got, tc.want)
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSubmittedImageOffsetsAreCheckedAndKept(t *testing.T) {
	h := testHarness(t)
	root, err := h.Root(context.Background(), ConversationOptions{})
	if err != nil {
		t.Fatal(err)
	}
	png := ImageContent{MimeType: "image/png", Data: "iVBORw0KGgo="}
	for _, offsets := range [][]int{{3, 1}, {99, 99}, {0}, {1, 1}} {
		_, err := root.Submit(context.Background(), SubmissionRequest{Content: "éé", Images: []ImageContent{png, png}, ImageOffsets: offsets, RequestID: "bad"})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("offsets %v: %v", offsets, err)
		}
	}
	if _, err := root.Submit(context.Background(), SubmissionRequest{Content: "éé", Images: []ImageContent{png, png}, ImageOffsets: []int{0, 2}}); err != nil {
		t.Fatal(err)
	}
	entries, _ := root.Entries(context.Background())
	var found bool
	for _, entry := range entries {
		if m := entry.Message; m != nil && m.Role == "user" && len(m.ImageOffsets) == 2 && m.ImageOffsets[1] == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("no user message kept its offsets: %+v", entries)
	}
}
