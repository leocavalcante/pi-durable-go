package durable

import (
	"fmt"
	"sort"
	"unicode/utf8"
)

// ContentPart is one block of a message's ordered content, as pi-ai's
// content blocks: text, or an image.
type ContentPart struct {
	Text  string
	Image *ImageContent
}

// OrderedContent is a message's content in order: its text split where
// ImageOffsets place its images (upstream's ordered text and image blocks).
// Without offsets every image follows the text. Offsets past the text, or
// inside a UTF-8 sequence, move to the next valid place; empty text blocks
// are left out.
func OrderedContent(content string, images []ImageContent, offsets []int) []ContentPart {
	var parts []ContentPart
	if len(offsets) != len(images) {
		offsets = nil
	}
	positions := make([]int, len(images))
	for i := range images {
		at := len(content)
		if offsets != nil {
			at = offsets[i]
		}
		if at < 0 {
			at = 0
		}
		if at > len(content) {
			at = len(content)
		}
		for at < len(content) && !utf8.RuneStart(content[at]) {
			at++
		}
		positions[i] = at
	}
	order := make([]int, len(images))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return positions[order[a]] < positions[order[b]] })
	written := 0
	for _, i := range order {
		if at := positions[i]; at > written {
			parts = append(parts, ContentPart{Text: content[written:at]})
			written = at
		}
		image := images[i]
		parts = append(parts, ContentPart{Image: &image})
	}
	if written < len(content) {
		parts = append(parts, ContentPart{Text: content[written:]})
	}
	return parts
}

// validImageOffsets checks a submission's image offsets: one per image,
// non-decreasing, within the content, and at UTF-8 boundaries.
func validImageOffsets(content string, images []ImageContent, offsets []int) error {
	if offsets == nil {
		return nil
	}
	if len(offsets) != len(images) {
		return fmt.Errorf("%w: one image offset per image", ErrInvalid)
	}
	previous := 0
	for _, at := range offsets {
		if at < previous || at > len(content) || at < len(content) && !utf8.RuneStart(content[at]) {
			return fmt.Errorf("%w: image offsets must be non-decreasing positions in the content", ErrInvalid)
		}
		previous = at
	}
	return nil
}
