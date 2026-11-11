package tools

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"image"
	"image/color"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCodingReadReturnsImages(t *testing.T) {
	dir := t.TempDir()
	img := image.NewRGBA(image.Rect(0, 0, 3, 2))
	img.Set(1, 1, color.RGBA{R: 255, A: 255})
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, img); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "dot.png"), encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	result, _ := runCoding(t, dir, "read", `{"path":"dot.png"}`)
	if result.IsError || len(result.Images) != 1 || result.Images[0].MimeType != "image/png" ||
		result.Images[0].Data != base64.StdEncoding.EncodeToString(encoded.Bytes()) || !strings.Contains(result.Content, "[info] Read image file [image/png].") {
		t.Fatalf("result=%+v", result)
	}

	// Too large for the limits without an image processor.
	wide := append([]byte{}, pngSignature...)
	wide = binary.BigEndian.AppendUint32(wide, 13)
	wide = append(wide, "IHDR"...)
	wide = binary.BigEndian.AppendUint32(wide, 3000)
	wide = binary.BigEndian.AppendUint32(wide, 10)
	wide = append(wide, 8, 6, 0, 0, 0, 0, 0, 0, 0)
	if err := os.WriteFile(filepath.Join(dir, "wide.png"), wide, 0600); err != nil {
		t.Fatal(err)
	}
	result, _ = runCoding(t, dir, "read", `{"path":"wide.png"}`)
	if !result.IsError || len(result.Images) != 0 || !strings.Contains(result.Content, "[error] wide.png is an image (image/png) of 3000x10, larger than 2000x2000, and no image processor is configured to shrink it") {
		t.Fatalf("wide=%+v", result)
	}

	// BMP needs converting.
	bmp := make([]byte, 40)
	copy(bmp, "BM")
	binary.LittleEndian.PutUint32(bmp[10:], 54)
	binary.LittleEndian.PutUint32(bmp[14:], 40)
	binary.LittleEndian.PutUint16(bmp[26:], 1)
	binary.LittleEndian.PutUint16(bmp[28:], 24)
	if err := os.WriteFile(filepath.Join(dir, "pic.bmp"), bmp, 0600); err != nil {
		t.Fatal(err)
	}
	result, _ = runCoding(t, dir, "read", `{"path":"pic.bmp"}`)
	if !result.IsError || !strings.Contains(result.Content, "pic.bmp is an image (image/bmp) that needs converting, and no image processor is configured") {
		t.Fatalf("bmp=%+v", result)
	}
}

func TestImageDimensionsFromHeaders(t *testing.T) {
	gif := []byte("GIF89a\x40\x01\xf0\x00")
	if w, h, ok := imageDimensions(gif, "image/gif"); !ok || w != 320 || h != 240 {
		t.Fatalf("gif %dx%d %v", w, h, ok)
	}
	jpeg := []byte{0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0, 0, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x01, 0xe0, 0x02, 0x80, 0x03}
	if w, h, ok := imageDimensions(jpeg, "image/jpeg"); !ok || w != 640 || h != 480 {
		t.Fatalf("jpeg %dx%d %v", w, h, ok)
	}
	if detectImageType(jpeg) != "image/jpeg" || detectImageType([]byte("plain text")) != "" {
		t.Fatal("detection")
	}
}
