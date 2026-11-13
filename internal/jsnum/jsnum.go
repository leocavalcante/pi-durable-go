// Package jsnum formats numbers as JavaScript does.
package jsnum

import (
	"math"
	"strconv"
	"strings"
)

// String is Number.prototype.toString(): the shortest digits that round
// trip, in fixed notation from 1e-6 below 1e21 and exponent notation
// otherwise.
func String(f float64) string {
	switch {
	case f == 0:
		return "0"
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	}
	sign := ""
	if f < 0 {
		sign, f = "-", -f
	}
	formatted := strconv.FormatFloat(f, 'e', -1, 64) // d.dddde±x
	mantissa, exponent, _ := strings.Cut(formatted, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	e, _ := strconv.Atoi(exponent)
	k, n := len(digits), e+1
	switch {
	case k <= n && n <= 21:
		return sign + digits + strings.Repeat("0", n-k)
	case 0 < n && n <= 21:
		return sign + digits[:n] + "." + digits[n:]
	case -6 < n && n <= 0:
		return sign + "0." + strings.Repeat("0", -n) + digits
	}
	exp := n - 1
	expText := strconv.Itoa(exp)
	if exp >= 0 {
		expText = "+" + expText
	}
	if k == 1 {
		return sign + digits + "e" + expText
	}
	return sign + digits[:1] + "." + digits[1:] + "e" + expText
}
