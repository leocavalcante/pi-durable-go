package durable

import (
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

// Schema patterns are ECMAScript regular expressions, as pi-schema compiles
// them (sharedRegex): with the u flag, or without it when the source is
// invalid with it. A match that runs longer than jsRegexpTimeout fails,
// which JavaScript would not bound.
const jsRegexpTimeout = 5 * time.Second

// jsRegexp is a compiled schema pattern.
type jsRegexp struct {
	source string
	re     *regexp2.Regexp
}

func compileJSRegexp(source string) (*jsRegexp, error) {
	pattern := jsDots(source)
	re, err := regexp2.Compile(pattern, regexp2.ECMAScript|regexp2.Unicode)
	if err != nil {
		if re, err = regexp2.Compile(pattern, regexp2.ECMAScript); err != nil {
			return nil, err
		}
	}
	re.MatchTimeout = jsRegexpTimeout
	return &jsRegexp{source: source, re: re}, nil
}

// MatchString reports whether s contains a match, as RegExp.prototype.test.
func (r *jsRegexp) MatchString(s string) bool {
	matched, err := r.re.MatchString(s)
	return err == nil && matched
}

func (r *jsRegexp) String() string { return r.source }

// jsRegexpEngine compiles the JSON Schema library's patterns as pi-schema.
func jsRegexpEngine(source string) (jsonschema.Regexp, error) {
	re, err := compileJSRegexp(source)
	if err != nil {
		return nil, err
	}
	return re, nil
}

// jsDots spells out JavaScript's dot, which matches no line terminator
// (\n, \r, U+2028, U+2029), where regexp2's excludes only \n: each dot
// outside a character class and not escaped.
func jsDots(source string) string {
	var b strings.Builder
	class := false
	for i := 0; i < len(source); i++ {
		c := source[i]
		switch {
		case c == '\\' && i+1 < len(source):
			b.WriteByte(c)
			i++
			b.WriteByte(source[i])
			continue
		case class && c == ']':
			class = false
		case !class && c == '[':
			class = true
		case !class && c == '.':
			b.WriteString(`[^\n\r\u2028\u2029]`)
			continue
		}
		b.WriteByte(c)
	}
	return b.String()
}
