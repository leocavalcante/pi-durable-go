package js

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

// isSpace is JavaScript's WhiteSpace or LineTerminator, which trim removes.
func isSpace(r rune) bool {
	switch r {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return r >= 0x2000 && r <= 0x200a
}

// Trim is String.prototype.trim.
func Trim(s string) string { return strings.TrimFunc(s, isSpace) }

// TrimStart is String.prototype.trimStart.
func TrimStart(s string) string { return strings.TrimLeftFunc(s, isSpace) }

// TrimEnd is String.prototype.trimEnd.
func TrimEnd(s string) string { return strings.TrimRightFunc(s, isSpace) }

// Len is a string's JavaScript length: UTF-16 code units.
func Len(text string) int {
	n := 0
	for _, r := range text {
		if r >= 0x10000 {
			n += 2
		} else {
			n++
		}
	}
	return n
}

// ToFixed is Number.prototype.toFixed for a finite number: the decimal with
// digits fraction digits nearest its exact value, the larger on a tie (fmt
// rounds ties to even).
func ToFixed(x float64, digits int) string {
	negative := x < 0
	exact := new(big.Rat).SetFloat64(math.Abs(x))
	if exact == nil {
		return strconv.FormatFloat(x, 'f', digits, 64)
	}
	scaled := new(big.Rat).Mul(exact, new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(digits)), nil)))
	// floor(scaled + 1/2): the nearest integer, the larger on a tie.
	twice := new(big.Int).Mul(scaled.Num(), big.NewInt(2))
	twice.Add(twice, scaled.Denom())
	n := new(big.Int).Quo(twice, new(big.Int).Mul(scaled.Denom(), big.NewInt(2)))
	text := n.String()
	if digits > 0 {
		if len(text) <= digits {
			text = strings.Repeat("0", digits-len(text)+1) + text
		}
		text = text[:len(text)-digits] + "." + text[len(text)-digits:]
	}
	if negative && strings.Trim(text, "0.") != "" {
		text = "-" + text
	}
	return text
}
