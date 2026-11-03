package store

// Canonical forms used for the backup checksum and MAC.
//
// Root cause of the "checksum mismatch" bug: ExportBackup hashed the exact
// text PostgreSQL produced (jsonb::text, with literal <, >, & and U+2028/9),
// but the HTTP handler then wrote the file with json.Encoder, which
// re-escapes a json.RawMessage (HTML-safe: < > &
//  ). Any backup containing such a character (workflow code with "<",
// "&&", "=>" ...) therefore failed verification on restore.
//
// Two canonical forms exist, selected by Backup.Canonical:
//
//	""   (legacy, all existing v3 files): compact the JSON, then turn the five
//	     HTML-safe escapes back into literal characters. This reproduces the
//	     exact bytes that were hashed at export time, whether or not the file
//	     was escaped, indented or re-flowed afterwards.
//	"c2" (new exports): the JSON is parsed and re-emitted with object keys
//	     sorted, no whitespace and one fixed string encoding, so key order,
//	     whitespace and escape style can never change the digest. Array order
//	     and number tokens are preserved verbatim.

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"strings"
)

const (
	canonLegacy = ""
	canonV2     = "c2"
)

func canonicalize(tables []byte, mode string) ([]byte, error) {
	switch mode {
	case canonLegacy:
		return canonLegacyForm(tables)
	case canonV2:
		return canonSortedForm(tables)
	}
	return nil, errors.New("unsupported canonical form " + mode)
}

func canonLegacyForm(in []byte) ([]byte, error) {
	var c bytes.Buffer
	if err := json.Compact(&c, in); err != nil {
		return nil, err
	}
	src := c.Bytes()
	out := make([]byte, 0, len(src))
	inStr := false
	for i := 0; i < len(src); i++ {
		ch := src[i]
		if !inStr {
			if ch == '"' {
				inStr = true
			}
			out = append(out, ch)
			continue
		}
		switch {
		case ch == '"':
			inStr = false
			out = append(out, ch)
		case ch == '\\' && i+1 < len(src):
			if src[i+1] == 'u' && i+5 < len(src) {
				switch strings.ToLower(string(src[i+2 : i+6])) {
				case "003c":
					out, i = append(out, '<'), i+5
					continue
				case "003e":
					out, i = append(out, '>'), i+5
					continue
				case "0026":
					out, i = append(out, '&'), i+5
					continue
				case "2028":
					out, i = append(out, " "...), i+5
					continue
				case "2029":
					out, i = append(out, " "...), i+5
					continue
				}
			}
			// any other escape: copy both bytes so "\\" never pairs wrongly
			out = append(out, ch, src[i+1])
			i++
		default:
			out = append(out, ch)
		}
	}
	return out, nil
}

func canonSortedForm(in []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(in))
	dec.UseNumber()
	var out bytes.Buffer
	if err := canonValue(dec, &out); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data after JSON value")
	}
	return out.Bytes(), nil
}

func canonString(out *bytes.Buffer, s string) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		return err
	}
	out.Write(bytes.TrimRight(b.Bytes(), "\n"))
	return nil
}

func canonValue(dec *json.Decoder, out *bytes.Buffer) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch v := tok.(type) {
	case json.Delim:
		switch v {
		case '[':
			out.WriteByte('[')
			for first := true; dec.More(); first = false {
				if !first {
					out.WriteByte(',')
				}
				if err := canonValue(dec, out); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			out.WriteByte(']')
		case '{':
			type kv struct {
				k string
				v []byte
			}
			var items []kv
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := kt.(string)
				if !ok {
					return errors.New("invalid object key")
				}
				var vb bytes.Buffer
				if err := canonValue(dec, &vb); err != nil {
					return err
				}
				items = append(items, kv{key, vb.Bytes()})
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
			sort.SliceStable(items, func(i, j int) bool { return items[i].k < items[j].k })
			out.WriteByte('{')
			for i, it := range items {
				if i > 0 {
					out.WriteByte(',')
				}
				if err := canonString(out, it.k); err != nil {
					return err
				}
				out.WriteByte(':')
				out.Write(it.v)
			}
			out.WriteByte('}')
		default:
			return errors.New("unexpected delimiter")
		}
	case string:
		return canonString(out, v)
	case json.Number:
		out.WriteString(v.String())
	case bool:
		if v {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case nil:
		out.WriteString("null")
	}
	return nil
}
