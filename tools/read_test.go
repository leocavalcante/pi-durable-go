package tools

import "testing"

// Sizes are written as upstream's formatSize writes them: toFixed rounds an
// exact half up, where fmt rounds it to even.
func TestSizesAreFormattedAsUpstream(t *testing.T) {
	for n, want := range map[int]string{0: "0B", 1023: "1023B", 1024: "1.0KB", 1280: "1.3KB", 2304: "2.3KB", 1048575: "1024.0KB", 1048576: "1.0MB", 1310720: "1.3MB", 1153433: "1.1MB", 5376: "5.3KB", 123456789: "117.7MB"} {
		if got := formatSize(n); got != want {
			t.Errorf("%d: %s, want %s", n, got, want)
		}
	}
	for x, want := range map[float64]string{1.125: "1.13", 1.5: "1.50", 2: "2.00", 1.005: "1.00", 0.125: "0.13"} {
		if got := jsToFixed(x, 2); got != want {
			t.Errorf("%v: %s, want %s", x, got, want)
		}
	}
}
