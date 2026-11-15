package js

import "testing"

// Decoding follows TextDecoder: maximal subparts, held-back characters, and
// the replacement at the end of the stream.
func TestUTF8DecoderMatchesTextDecoder(t *testing.T) {
	for _, c := range []struct {
		chunks []string
		want   string
	}{
		{[]string{"h\xc3\xa9llo"}, "héllo"},
		{[]string{"h\xc3", "\xa9llo"}, "héllo"},
		{[]string{"\xe2\x9c", "\x93", "!"}, "✓!"},
		{[]string{"\xf0\x9f", "\x98", "\x80"}, "😀"},
		{[]string{"\xe2\x82A"}, "\ufffdA"},               // a maximal subpart is one replacement
		{[]string{"\xff\xfeA"}, "\ufffd\ufffdA"},         // each invalid byte is one
		{[]string{"\xed\xa0\x80"}, "\ufffd\ufffd\ufffd"}, // surrogates are invalid
		{[]string{"\xf4\x90\x80\x80"}, "\ufffd\ufffd\ufffd\ufffd"},
		{[]string{"\xc0\xaf"}, "\ufffd\ufffd"}, // overlong
		{[]string{"ok\xe2\x9c"}, "ok\ufffd"},   // incomplete at the end
		{[]string{"\xef\xbb\xbfbom"}, "\ufeffbom"},
	} {
		var d UTF8Decoder
		got := ""
		for _, chunk := range c.chunks {
			got += d.Decode([]byte(chunk), true)
		}
		got += d.Decode(nil, false)
		if got != c.want {
			t.Errorf("%q: got %q, want %q", c.chunks, got, c.want)
		}
	}
}
