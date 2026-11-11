package tools

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/draw"
	_ "image/gif" // GIF decoding
	"image/jpeg"
	"image/png"
	"math"

	durable "github.com/leocavalcante/pi-durable-go"
	_ "golang.org/x/image/bmp" // BMP decoding
	xdraw "golang.org/x/image/draw"
	_ "golang.org/x/image/webp" // WebP decoding
)

// ImageLimits bound an image a model receives: its size in pixels and its
// base64 length (upstream ImageLimits).
type ImageLimits = durable.ImageLimits

// DefaultImageLimits are upstream DEFAULT_IMAGE_LIMITS.
var DefaultImageLimits = ImageLimits{MaxWidth: maxImageWidth, MaxHeight: maxImageHeight, MaxBytes: maxImageBytes}

// withDefaults fills the limits a model leaves unset (upstream imageModel).
func withDefaults(limits ImageLimits) ImageLimits {
	if limits.MaxWidth <= 0 {
		limits.MaxWidth = DefaultImageLimits.MaxWidth
	}
	if limits.MaxHeight <= 0 {
		limits.MaxHeight = DefaultImageLimits.MaxHeight
	}
	if limits.MaxBytes <= 0 {
		limits.MaxBytes = DefaultImageLimits.MaxBytes
	}
	return limits
}

// PreparedImage is an image ready for a model, with what preparing it did.
type PreparedImage struct {
	MimeType string
	// Data is base64.
	Data string
	// ConvertedFrom is the source format when it changed.
	ConvertedFrom string
	// Resized is set when the image was scaled: from the upright source size
	// to the size sent.
	Resized *ImageResize
}

// ImageResize is a scaling an image preparation did.
type ImageResize struct {
	FromWidth, FromHeight int
	ToWidth, ToHeight     int
}

// ImageProcessor prepares images for a model (upstream ImageProcessor):
// upright, within the limits, and in a format providers take inline. Prepare
// returns nil when the image cannot be prepared.
type ImageProcessor interface {
	Prepare(data []byte, mimeType string, limits ImageLimits) *PreparedImage
}

// StdImages is an ImageProcessor on the Go standard library and
// golang.org/x/image, ported from upstream's Photon processor. An image
// already upright, inline, and within the limits is returned as it is once
// it decodes. Otherwise it is turned upright by its EXIF orientation, scaled
// to the maximum width and height, and encoded as PNG or JPEG at falling
// qualities (JPEG first for JPEG sources), then at smaller dimensions, 3/4 at
// a time, until the base64 fits. BMP becomes PNG or JPEG.
func StdImages() ImageProcessor { return stdImages{} }

type stdImages struct{}

// jpegQualities are tried in order.
var jpegQualities = []int{80, 70, 55, 40}

func (stdImages) Prepare(data []byte, mimeType string, limits ImageLimits) *PreparedImage {
	orientation := exifOrientation(data)
	// Decoded even when returned as it is: a damaged file is an error, not
	// an image the provider rejects.
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil
	}
	bounds := img.Bounds()
	if inlineImageType(mimeType) && orientation == 1 && bounds.Dx() <= limits.MaxWidth && bounds.Dy() <= limits.MaxHeight &&
		base64Length(int64(len(data))) <= int64(limits.MaxBytes) {
		return &PreparedImage{MimeType: mimeType, Data: base64.StdEncoding.EncodeToString(data)}
	}
	upright := orient(img, orientation)
	width, height := upright.Bounds().Dx(), upright.Bounds().Dy()
	scale := math.Min(1, math.Min(float64(limits.MaxWidth)/float64(width), float64(limits.MaxHeight)/float64(height)))
	targetW := max(1, int(math.Round(float64(width)*scale)))
	targetH := max(1, int(math.Round(float64(height)*scale)))
	for {
		if encoded, encodedType := encodeWithin(upright, targetW, targetH, limits.MaxBytes, mimeType == "image/jpeg"); encoded != nil {
			prepared := &PreparedImage{MimeType: encodedType, Data: base64.StdEncoding.EncodeToString(encoded)}
			if encodedType != mimeType {
				prepared.ConvertedFrom = mimeType
			}
			if targetW != width || targetH != height {
				prepared.Resized = &ImageResize{FromWidth: width, FromHeight: height, ToWidth: targetW, ToHeight: targetH}
			}
			return prepared
		}
		if targetW == 1 && targetH == 1 {
			return nil
		}
		targetW, targetH = max(1, targetW*3/4), max(1, targetH*3/4)
	}
}

// encodeWithin encodes img at the size in the first encoding whose base64
// fits: PNG, then JPEG at falling qualities, or the JPEG qualities first.
func encodeWithin(img image.Image, width, height, maxBytes int, jpegFirst bool) ([]byte, string) {
	scaled := img
	if b := img.Bounds(); b.Dx() != width || b.Dy() != height {
		dst := image.NewRGBA(image.Rect(0, 0, width, height))
		xdraw.CatmullRom.Scale(dst, dst.Bounds(), img, b, draw.Over, nil)
		scaled = dst
	}
	type candidate struct {
		mimeType string
		encode   func(*bytes.Buffer) error
	}
	pngCandidate := candidate{"image/png", func(b *bytes.Buffer) error { return png.Encode(b, scaled) }}
	var candidates []candidate
	if !jpegFirst {
		candidates = append(candidates, pngCandidate)
	}
	for _, quality := range jpegQualities {
		quality := quality
		candidates = append(candidates, candidate{"image/jpeg", func(b *bytes.Buffer) error {
			return jpeg.Encode(b, scaled, &jpeg.Options{Quality: quality})
		}})
	}
	if jpegFirst {
		candidates = append(candidates, pngCandidate)
	}
	for _, c := range candidates {
		var out bytes.Buffer
		if c.encode(&out) != nil {
			continue
		}
		if base64Length(int64(out.Len())) <= int64(maxBytes) {
			return out.Bytes(), c.mimeType
		}
	}
	return nil, ""
}

// orient turns img upright for its EXIF orientation (upstream: flips for 2
// to 4, a transposition for 5 to 8, which swap width and height).
func orient(img image.Image, orientation int) image.Image {
	if orientation <= 1 || orientation > 8 {
		return img
	}
	b := img.Bounds()
	width, height := b.Dx(), b.Dy()
	src := image.NewRGBA(image.Rect(0, 0, width, height))
	draw.Draw(src, src.Bounds(), img, b.Min, draw.Src)
	if orientation >= 5 {
		dst := image.NewRGBA(image.Rect(0, 0, height, width))
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				outX := height - 1 - y
				if orientation == 5 || orientation == 8 {
					outX = y
				}
				outY := width - 1 - x
				if orientation == 5 || orientation == 6 {
					outY = x
				}
				dst.Set(outX, outY, src.At(x, y))
			}
		}
		return dst
	}
	dst := image.NewRGBA(src.Bounds())
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			outX, outY := x, y
			if orientation != 4 { // flip horizontally
				outX = width - 1 - x
			}
			if orientation != 2 { // flip vertically
				outY = height - 1 - y
			}
			dst.Set(outX, outY, src.At(x, y))
		}
	}
	return dst
}

// exifOrientation is upstream exifOrientation: the EXIF Orientation of a
// JPEG's APP1 or a WebP's EXIF chunk, 1 when there is none.
func exifOrientation(b []byte) int {
	tiff := tiffStart(b)
	if tiff < 0 || tiff+8 > len(b) {
		return 1
	}
	little := b[tiff] == 0x49 && b[tiff+1] == 0x49
	u16 := func(at int) int {
		if at+2 > len(b) {
			return 0
		}
		if little {
			return int(b[at]) | int(b[at+1])<<8
		}
		return int(b[at])<<8 | int(b[at+1])
	}
	u32 := func(at int) int {
		if at+4 > len(b) {
			return 0
		}
		if little {
			return int(b[at]) | int(b[at+1])<<8 | int(b[at+2])<<16 | int(b[at+3])<<24
		}
		return int(b[at])<<24 | int(b[at+1])<<16 | int(b[at+2])<<8 | int(b[at+3])
	}
	directory := tiff + u32(tiff+4)
	if directory+2 > len(b) {
		return 1
	}
	entries := u16(directory)
	for index := 0; index < entries; index++ {
		entry := directory + 2 + index*12
		if entry+12 > len(b) {
			return 1
		}
		// Tag 0x0112 is Orientation, a SHORT whose value sits in the entry.
		if u16(entry) == 0x0112 {
			if value := u16(entry + 8); value >= 1 && value <= 8 {
				return value
			}
			return 1
		}
	}
	return 1
}

// tiffStart finds the TIFF header of a JPEG's APP1 or a WebP's EXIF chunk.
func tiffStart(b []byte) int {
	if len(b) >= 2 && b[0] == 0xff && b[1] == 0xd8 {
		for offset := 2; offset+4 <= len(b); {
			if b[offset] != 0xff {
				return -1
			}
			marker := b[offset+1]
			if marker == 0xff {
				offset++
				continue
			}
			if marker == 0xe1 && exifHeader(b, offset+4) {
				return offset + 10
			}
			offset += 2 + (int(b[offset+2])<<8 | int(b[offset+3]))
		}
		return -1
	}
	if asciiAt(b, 0, "RIFF") && asciiAt(b, 8, "WEBP") {
		for offset := 12; offset+8 <= len(b); {
			size := int(u32le(b, offset+4))
			// Some WebP files prefix the TIFF header with "Exif\0\0".
			if asciiAt(b, offset, "EXIF") {
				if exifHeader(b, offset+8) {
					return offset + 14
				}
				return offset + 8
			}
			// RIFF chunks are padded to an even size.
			next := offset + 8 + size + size%2
			if next <= offset {
				return -1
			}
			offset = next
		}
	}
	return -1
}

func exifHeader(b []byte, offset int) bool {
	return asciiAt(b, offset, "Exif") && len(b) > offset+5 && b[offset+4] == 0 && b[offset+5] == 0
}
