package tools

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	durable "github.com/leocavalcante/pi-durable-go"
)

// A port of upstream image.ts and the read tool's image path without an
// image processor: images in a format providers take inline pass through as
// they are when they fit the default limits.

// Upstream DEFAULT_IMAGE_LIMITS.
const (
	maxImageWidth  = 2000
	maxImageHeight = 2000
	maxImageBytes  = 4.5 * 1024 * 1024 // base64 bytes
)

// imageHeadBytes is how much of a file image detection reads.
const imageHeadBytes = 64 * 1024

var pngSignature = []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a}

func inlineImageType(mimeType string) bool {
	switch mimeType {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
		return true
	}
	return false
}

func base64Length(n int64) int64 { return (n + 2) / 3 * 4 }

func asciiAt(b []byte, offset int, s string) bool {
	return len(b) >= offset+len(s) && string(b[offset:offset+len(s)]) == s
}

func u16le(b []byte, offset int) int {
	if len(b) < offset+2 {
		return 0
	}
	return int(binary.LittleEndian.Uint16(b[offset:]))
}

func u16be(b []byte, offset int) int {
	if len(b) < offset+2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(b[offset:]))
}

func u32le(b []byte, offset int) uint32 {
	if len(b) < offset+4 {
		return 0
	}
	return binary.LittleEndian.Uint32(b[offset:])
}

func u32be(b []byte, offset int) uint32 {
	if len(b) < offset+4 {
		return 0
	}
	return binary.BigEndian.Uint32(b[offset:])
}

// detectImageType is upstream detectSupportedImageMimeType over a file's head.
func detectImageType(b []byte) string {
	switch {
	case bytes.HasPrefix(b, []byte{0xff, 0xd8, 0xff}):
		if len(b) > 3 && b[3] == 0xf7 {
			return ""
		}
		return "image/jpeg"
	case bytes.HasPrefix(b, pngSignature):
		if len(b) >= 16 && u32be(b, 8) == 13 && asciiAt(b, 12, "IHDR") && !animatedPNG(b) {
			return "image/png"
		}
		return ""
	case asciiAt(b, 0, "GIF87a") || asciiAt(b, 0, "GIF89a"):
		return "image/gif"
	case asciiAt(b, 0, "RIFF") && asciiAt(b, 8, "WEBP"):
		return "image/webp"
	case asciiAt(b, 0, "BM") && isBMP(b):
		return "image/bmp"
	}
	return ""
}

func animatedPNG(b []byte) bool {
	for offset := len(pngSignature); offset+8 <= len(b); {
		length := int(u32be(b, offset))
		if asciiAt(b, offset+4, "acTL") {
			return true
		}
		if asciiAt(b, offset+4, "IDAT") {
			return false
		}
		next := offset + 8 + length + 4
		if next <= offset || next > len(b) {
			return false
		}
		offset = next
	}
	return false
}

func isBMP(b []byte) bool {
	if len(b) < 26 {
		return false
	}
	declared, pixels, dib := u32le(b, 2), u32le(b, 10), u32le(b, 14)
	if declared != 0 && declared < 26 || pixels < 14+dib || declared != 0 && pixels >= declared {
		return false
	}
	var planes, bits int
	switch {
	case dib == 12:
		planes, bits = u16le(b, 22), u16le(b, 24)
	case dib >= 40 && dib <= 124:
		if len(b) < 30 {
			return false
		}
		planes, bits = u16le(b, 26), u16le(b, 28)
	default:
		return false
	}
	switch bits {
	case 1, 4, 8, 16, 24, 32:
		return planes == 1
	}
	return false
}

// imageDimensions reads the size an image declares in its header (upstream
// imageDimensions); ok is false when it cannot.
func imageDimensions(b []byte, mimeType string) (width, height int, ok bool) {
	switch mimeType {
	case "image/png":
		if len(b) < 24 {
			return 0, 0, false
		}
		return int(u32be(b, 16)), int(u32be(b, 20)), true
	case "image/gif":
		if len(b) < 10 {
			return 0, 0, false
		}
		return u16le(b, 6), u16le(b, 8), true
	case "image/webp":
		if len(b) < 30 {
			return 0, 0, false
		}
		switch {
		case asciiAt(b, 12, "VP8 "):
			return u16le(b, 26) & 0x3fff, u16le(b, 28) & 0x3fff, true
		case asciiAt(b, 12, "VP8L"):
			bits := u32le(b, 21)
			return int(bits&0x3fff) + 1, int((bits>>14)&0x3fff) + 1, true
		case asciiAt(b, 12, "VP8X"):
			u24 := func(offset int) int { return u16le(b, offset) + int(b[offset+2])<<16 }
			return u24(24) + 1, u24(27) + 1, true
		}
		return 0, 0, false
	case "image/jpeg":
		for offset := 2; offset+9 <= len(b); {
			if b[offset] != 0xff {
				return 0, 0, false
			}
			marker := b[offset+1]
			if marker == 0xff {
				offset++
				continue
			}
			// SOF0 to SOF15, except DHT (C4), JPG (C8), and DAC (CC).
			if marker >= 0xc0 && marker <= 0xcf && marker != 0xc4 && marker != 0xc8 && marker != 0xcc {
				return u16be(b, offset+7), u16be(b, offset+5), true
			}
			offset += 2 + u16be(b, offset+2)
		}
	}
	return 0, 0, false
}

func imageError(message string) (durable.ToolResult, error) {
	return durable.ToolResult{IsError: true, Diagnostics: []durable.ToolDiagnostic{{Severity: "error", Code: "unsupported_image", Message: message}}}, nil
}

// readImage reads an image the head identified, as upstream does without an
// image processor; vision says whether the model takes images.
func readImage(r io.Reader, size int64, path, mimeType string, vision bool) (durable.ToolResult, error) {
	if !inlineImageType(mimeType) {
		return imageError(fmt.Sprintf("%s is an image (%s) that needs converting, and no image processor is configured", path, mimeType))
	}
	const unshrinkable = "and no image processor is configured to shrink it"
	if base64Length(size) > maxImageBytes {
		return imageError(fmt.Sprintf("%s is an image (%s) of %s, too large to send, %s", path, mimeType, formatSize(int(size)), unshrinkable))
	}
	data, err := io.ReadAll(io.LimitReader(r, size+1))
	if err != nil {
		return durable.ToolResult{}, err
	}
	width, height, ok := imageDimensions(data, mimeType)
	if !ok {
		return imageError(fmt.Sprintf("%s is an image (%s) whose size cannot be read", path, mimeType))
	}
	if width > maxImageWidth || height > maxImageHeight {
		return imageError(fmt.Sprintf("%s is an image (%s) of %dx%d, larger than %dx%d, %s", path, mimeType, width, height, maxImageWidth, maxImageHeight, unshrinkable))
	}
	note := fmt.Sprintf("Read image file [%s].", mimeType)
	if !vision {
		note += " The current model does not support images; it sees a placeholder instead."
	}
	return durable.ToolResult{
		Images:      []durable.ImageContent{{MimeType: mimeType, Data: base64.StdEncoding.EncodeToString(data)}},
		Diagnostics: []durable.ToolDiagnostic{{Severity: "info", Code: "image", Message: note}},
	}, nil
}

// fileSize reports a reader's total size when it can tell.
func fileSize(r io.Reader) (int64, bool) {
	if f, ok := r.(interface{ Stat() (os.FileInfo, error) }); ok {
		if info, err := f.Stat(); err == nil {
			return info.Size(), true
		}
	}
	if b, ok := r.(*bytes.Reader); ok {
		return b.Size(), true
	}
	return 0, false
}
