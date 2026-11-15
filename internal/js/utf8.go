package js

import (
	"strings"
	"unicode/utf8"
)

// UTF8Decoder decodes UTF-8 as a streaming TextDecoder("utf-8", {ignoreBOM:
// true}) does (the WHATWG Encoding Standard's decoder): a character split
// across chunks is held back until its end arrives, and each maximal invalid
// subpart becomes one U+FFFD. Its zero value is ready.
type UTF8Decoder struct {
	codePoint, needed, seen int
	lower, upper            byte
}

// Decode decodes the next chunk; with stream false it also ends the
// stream, so an incomplete character becomes U+FFFD.
func (d *UTF8Decoder) Decode(chunk []byte, stream bool) string {
	var out strings.Builder
	out.Grow(len(chunk))
	if d.lower == 0 {
		d.lower, d.upper = 0x80, 0xbf
	}
	for i := 0; i < len(chunk); i++ {
		b := chunk[i]
		if d.needed == 0 {
			switch {
			case b <= 0x7f:
				out.WriteByte(b)
			case b >= 0xc2 && b <= 0xdf:
				d.needed, d.codePoint = 1, int(b&0x1f)
			case b >= 0xe0 && b <= 0xef:
				if b == 0xe0 {
					d.lower = 0xa0
				}
				if b == 0xed {
					d.upper = 0x9f
				}
				d.needed, d.codePoint = 2, int(b&0x0f)
			case b >= 0xf0 && b <= 0xf4:
				if b == 0xf0 {
					d.lower = 0x90
				}
				if b == 0xf4 {
					d.upper = 0x8f
				}
				d.needed, d.codePoint = 3, int(b&0x07)
			default:
				out.WriteRune(utf8.RuneError)
			}
			continue
		}
		if b < d.lower || b > d.upper {
			// The byte starts over after the invalid subpart.
			d.codePoint, d.needed, d.seen, d.lower, d.upper = 0, 0, 0, 0x80, 0xbf
			out.WriteRune(utf8.RuneError)
			i--
			continue
		}
		d.lower, d.upper = 0x80, 0xbf
		d.codePoint = d.codePoint<<6 | int(b&0x3f)
		d.seen++
		if d.seen == d.needed {
			out.WriteRune(rune(d.codePoint))
			d.codePoint, d.needed, d.seen = 0, 0, 0
		}
	}
	if !stream && d.needed != 0 {
		d.codePoint, d.needed, d.seen, d.lower, d.upper = 0, 0, 0, 0x80, 0xbf
		out.WriteRune(utf8.RuneError)
	}
	return out.String()
}
