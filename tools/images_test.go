package tools

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	"golang.org/x/image/bmp"
)

func solid(width, height int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.Set(x, y, color.RGBA{R: uint8(x), G: uint8(y), B: 90, A: 255})
		}
	}
	return img
}

func decodedSize(t *testing.T, prepared *PreparedImage) (int, int) {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(prepared.Data)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return cfg.Width, cfg.Height
}

func TestStdImagesResizesConvertsAndTurnsUpright(t *testing.T) {
	processor := StdImages()
	var wide bytes.Buffer
	if err := png.Encode(&wide, solid(3000, 1500)); err != nil {
		t.Fatal(err)
	}
	prepared := processor.Prepare(wide.Bytes(), "image/png", DefaultImageLimits)
	if prepared == nil || prepared.Resized == nil || prepared.Resized.ToWidth != 2000 || prepared.Resized.ToHeight != 1000 {
		t.Fatalf("wide=%+v", prepared)
	}
	if w, h := decodedSize(t, prepared); w != 2000 || h != 1000 {
		t.Fatalf("wide decoded %dx%d", w, h)
	}

	var small bytes.Buffer
	if err := png.Encode(&small, solid(4, 3)); err != nil {
		t.Fatal(err)
	}
	if prepared := processor.Prepare(small.Bytes(), "image/png", DefaultImageLimits); prepared == nil || prepared.Data != base64.StdEncoding.EncodeToString(small.Bytes()) || prepared.Resized != nil {
		t.Fatalf("a fitting image was changed: %+v", prepared)
	}

	var bitmap bytes.Buffer
	if err := bmp.Encode(&bitmap, solid(5, 4)); err != nil {
		t.Fatal(err)
	}
	if prepared := processor.Prepare(bitmap.Bytes(), "image/bmp", DefaultImageLimits); prepared == nil || prepared.ConvertedFrom != "image/bmp" || prepared.MimeType != "image/png" {
		t.Fatalf("bmp=%+v", prepared)
	}

	// A JPEG whose EXIF orientation 6 turns it a quarter clockwise.
	var photo bytes.Buffer
	if err := jpeg.Encode(&photo, solid(40, 20), nil); err != nil {
		t.Fatal(err)
	}
	rotated := withOrientation(photo.Bytes(), 6)
	if exifOrientation(rotated) != 6 {
		t.Fatal("orientation not read")
	}
	prepared = processor.Prepare(rotated, "image/jpeg", DefaultImageLimits)
	if prepared == nil || prepared.MimeType != "image/jpeg" {
		t.Fatalf("rotated=%+v", prepared)
	}
	if w, h := decodedSize(t, prepared); w != 20 || h != 40 {
		t.Fatalf("rotated decoded %dx%d", w, h)
	}

	if processor.Prepare([]byte("\x89PNG\r\n\x1a\nbroken"), "image/png", DefaultImageLimits) != nil {
		t.Fatal("a damaged image was prepared")
	}
}

// withOrientation inserts an APP1 Exif segment with one Orientation entry
// after a JPEG's SOI marker.
func withOrientation(jpegData []byte, orientation uint16) []byte {
	var tiff bytes.Buffer
	tiff.WriteString("II*\x00")
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(8))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(1))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(0x0112))
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(3))
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(1))
	_ = binary.Write(&tiff, binary.LittleEndian, orientation)
	_ = binary.Write(&tiff, binary.LittleEndian, uint16(0))
	_ = binary.Write(&tiff, binary.LittleEndian, uint32(0))
	payload := append([]byte("Exif\x00\x00"), tiff.Bytes()...)
	segment := []byte{0xff, 0xe1}
	segment = binary.BigEndian.AppendUint16(segment, uint16(len(payload)+2))
	segment = append(segment, payload...)
	out := append([]byte{}, jpegData[:2]...)
	out = append(out, segment...)
	return append(out, jpegData[2:]...)
}

func TestReadImageWithAProcessorSaysWhatItDid(t *testing.T) {
	var wide bytes.Buffer
	if err := png.Encode(&wide, solid(3000, 1500)); err != nil {
		t.Fatal(err)
	}
	result, err := readImage(bytes.NewReader(wide.Bytes()), int64(wide.Len()), "wide.png", "image/png", true, StdImages())
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || len(result.Images) != 1 || len(result.Diagnostics) != 1 ||
		!strings.Contains(result.Diagnostics[0].Message, "Resized from 3000x1500 to 2000x1000. Multiply coordinates by 1.50 to map them to the original.") {
		t.Fatalf("result=%+v", result.Diagnostics)
	}
}
