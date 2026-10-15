// Copyright © 2026 Meroxa, Inc. and Miquido
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package decimaltext turns decoded Avro decimals into exact decimal text.
//
// The SDK's schema middleware decodes an Avro decimal (bytes or fixed with
// logicalType decimal) into a *big.Rat. encoding/json writes a *big.Rat as its
// ratio, so -123.456 reached Elasticsearch as "-15432/125" (#171). Render
// replaces every *big.Rat in a decoded value with a string such as "-123.456".
//
// Invariants:
//
//   - The text is exact: parsing it back gives the same rational number. A
//     value that has no exact decimal text is an error, never a rounded string
//     or a float.
//   - When the Avro schema gives the decimal's scale, the text has exactly
//     that many fractional digits ("1.50" at scale 2, "100" at scale 0), the
//     same text for every value of a field. Without a usable scale (no schema,
//     or a plain union value with two decimal branches of different scales)
//     the text has the fewest digits that are exact ("1.5").
//   - Render never mutates its input. It returns new maps and slices for every
//     container on the way to a decimal.
//
// Render walks the shapes the conduit-commons Avro decoder produces: records
// and maps as map[string]any (opencdc.StructuredData at the top), arrays as
// []any. Whether a union value is plain or still wrapped as
// {"<branch full name>": value} follows from the schema and the value's
// position alone; unionshape.go mirrors the codec's and commons' rules. A
// wrapper selects its branch by name and keeps its shape; Render does not
// unwrap it (see #173). A plain value is never read as a wrapper, whatever
// its keys.
package decimaltext

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/iskorotkov/avro/v2"
)

// ErrNotExact is returned when a value has no exact decimal text at the
// requested scale, or no finite decimal expansion at all.
var ErrNotExact = errors.New("decimal value has no exact decimal text")

var ten = big.NewInt(10)

// Format returns r as plain decimal text with exactly scale fractional digits
// (no fractional part when scale is 0). It returns ErrNotExact when r cannot be
// written at that scale without rounding, and an error for a negative scale.
func Format(r *big.Rat, scale int) (string, error) {
	if scale < 0 {
		return "", fmt.Errorf("negative decimal scale %d", scale)
	}
	if !exactAt(r, scale) {
		return "", fmt.Errorf("%w: %s at scale %d", ErrNotExact, r.RatString(), scale)
	}
	// FloatString rounds to scale digits; exactAt guarantees there is nothing
	// to round.
	return r.FloatString(scale), nil
}

// FormatExact returns r as plain decimal text with the fewest fractional
// digits that represent it exactly ("1.5", "-0.125", "7"). It returns
// ErrNotExact when r has no finite decimal expansion (a denominator with a
// prime factor other than 2 and 5). An Avro decimal always has one: its value
// is unscaled/10^scale.
func FormatExact(r *big.Rat) (string, error) {
	// The denominator is 2^a * 5^b when the expansion is finite, and both a
	// and b are at most its bit length, so that many digits always suffice.
	// One divisibility check replaces factoring the denominator.
	digits := r.Denom().BitLen()
	if !exactAt(r, digits) {
		return "", fmt.Errorf("%w: %s", ErrNotExact, r.RatString())
	}
	s := r.FloatString(digits)
	if digits == 0 {
		return s, nil
	}
	end := len(s)
	for s[end-1] == '0' {
		end--
	}
	if s[end-1] == '.' {
		end--
	}
	return s[:end], nil
}

// exactAt reports whether r*10^scale is an integer.
func exactAt(r *big.Rat, scale int) bool {
	if r.IsInt() {
		return true
	}
	n := new(big.Int).Exp(ten, big.NewInt(int64(scale)), nil)
	n.Mul(n, r.Num())
	return n.Mod(n, r.Denom()).Sign() == 0
}

// Contains reports whether v holds a *big.Rat anywhere in the shapes Render
// walks. It lets callers skip schema lookups and copies for the common case of
// a payload without decimals.
func Contains(v any) bool {
	switch v := v.(type) {
	case *big.Rat:
		return true
	case opencdc.StructuredData:
		return Contains(map[string]any(v))
	case map[string]any:
		for _, item := range v {
			if Contains(item) {
				return true
			}
		}
	case []any:
		for _, item := range v {
			if Contains(item) {
				return true
			}
		}
	}
	return false
}

// Render returns v with every *big.Rat replaced by its exact decimal text (see
// the package invariants). sch is the Avro schema v was decoded with, or nil
// when it is not known; it only supplies scales and never makes Render fail.
// The only error is a *big.Rat without an exact decimal text, which an Avro
// decoder never produces.
func Render(v any, sch avro.Schema) (any, error) {
	return render(v, sch, root, "")
}

func render(v any, sch avro.Schema, at position, path string) (any, error) {
	sch = deref(sch)
	if u, ok := sch.(*avro.UnionSchema); ok {
		return renderUnion(v, u, at, path)
	}
	switch v := v.(type) {
	case *big.Rat:
		return renderRat(v, sch, path)
	case opencdc.StructuredData:
		out, err := renderMap(v, sch, at, path)
		return opencdc.StructuredData(out), err
	case map[string]any:
		return renderMap(v, sch, at, path)
	case []any:
		if !Contains(v) {
			return v, nil
		}
		items := arrayItems(sch)
		child := at.child(arrayItem)
		out := make([]any, len(v))
		for i, item := range v {
			r, err := render(item, items, child, fmt.Sprintf("%s[%d]", path, i))
			if err != nil {
				return nil, err
			}
			out[i] = r
		}
		return out, nil
	default:
		return v, nil
	}
}

// renderUnion renders v, decoded with the union u at position at. When the
// value is still wrapped (see wrapped), its one key names the branch. A plain
// value is rendered with the only branch of its shape, or without a schema
// when there are two (two record branches), so its decimals get the fewest
// exact digits.
func renderUnion(v any, u *avro.UnionSchema, at position, path string) (any, error) {
	if !Contains(v) {
		return v, nil
	}
	if wrapped(u, at) {
		if w, ok := v.(map[string]any); ok && len(w) == 1 {
			for name, inner := range w {
				r, err := render(inner, branchNamed(u, name), at.throughUnion(u), path)
				if err != nil {
					return nil, err
				}
				return map[string]any{name: r}, nil
			}
		}
		// Not the shape the rules predict: render without a schema.
		return render(v, nil, at.throughUnion(u), path)
	}
	inner := at.throughUnion(u)
	switch v := v.(type) {
	case *big.Rat:
		return renderRat(v, u, path)
	case map[string]any:
		return renderMap(v, plainMapBranch(u, v), inner, path)
	case []any:
		return render(v, onlyBranch(u, avro.Array), inner, path)
	default:
		return v, nil
	}
}

// onlyBranch returns the single branch of u with one of the given types, or
// nil when there is none or more than one: a plain value does not say which
// of two record branches it came from.
func onlyBranch(u *avro.UnionSchema, types ...avro.Type) avro.Schema {
	var found avro.Schema
	for _, b := range u.Types() {
		b = deref(b)
		for _, t := range types {
			if b.Type() == t {
				if found != nil {
					return nil
				}
				found = b
			}
		}
	}
	return found
}

// plainMapBranch returns the branch of u a plain (unwrapped) map value v came
// from: the map branch, or a record branch whose field names are exactly the
// keys of v (the decoder writes every field). When that leaves more or fewer
// than one candidate, it returns nil and the entries get the fewest exact
// digits: once commons has unwrapped the value, nothing else says which
// branch it was.
func plainMapBranch(u *avro.UnionSchema, v map[string]any) avro.Schema {
	var found avro.Schema
	for _, b := range u.Types() {
		switch b := deref(b).(type) {
		case *avro.MapSchema:
		case *avro.RecordSchema:
			if !sameKeys(b, v) {
				continue
			}
		default:
			continue
		}
		if found != nil {
			return nil
		}
		found = deref(b)
	}
	return found
}

func sameKeys(r *avro.RecordSchema, v map[string]any) bool {
	if len(r.Fields()) != len(v) {
		return false
	}
	for _, f := range r.Fields() {
		if _, ok := v[f.Name()]; !ok {
			return false
		}
	}
	return true
}

func renderMap(v map[string]any, sch avro.Schema, at position, path string) (map[string]any, error) {
	if !Contains(v) {
		return v, nil
	}
	childOf := mapChildren(sch)
	kind := mapValue
	if _, ok := sch.(*avro.RecordSchema); ok {
		kind = recordField
	}
	child := at.child(kind)
	out := make(map[string]any, len(v))
	for k, item := range v {
		p := k
		if path != "" {
			p = path + "." + k
		}
		r, err := render(item, childOf(k), child, p)
		if err != nil {
			return nil, err
		}
		out[k] = r
	}
	return out, nil
}

func renderRat(r *big.Rat, sch avro.Schema, path string) (string, error) {
	if scale, ok := decimalScale(sch); ok {
		if s, err := Format(r, scale); err == nil {
			return s, nil
		}
		// The value does not fit the schema's scale, so the schema is not the
		// one it was decoded with. The exact text is still correct.
	}
	s, err := FormatExact(r)
	if err != nil {
		return "", fmt.Errorf("field %q: %w", path, err)
	}
	return s, nil
}

// deref resolves named-type references, so a decimal fixed declared once and
// referenced by name elsewhere still gives its scale.
func deref(sch avro.Schema) avro.Schema {
	for {
		ref, ok := sch.(*avro.RefSchema)
		if !ok {
			return sch
		}
		sch = ref.Schema()
	}
}

// decimalScale returns the scale of the decimal sch describes. For a union it
// returns the scale shared by all decimal branches, and false when they
// disagree: the decoded *big.Rat does not say which branch it came from.
func decimalScale(sch avro.Schema) (int, bool) {
	switch s := sch.(type) {
	case *avro.PrimitiveSchema:
		return logicalScale(s.Logical())
	case *avro.FixedSchema:
		return logicalScale(s.Logical())
	case *avro.UnionSchema:
		scale, found := 0, false
		for _, branch := range s.Types() {
			bs, ok := decimalScale(deref(branch))
			if !ok {
				continue
			}
			if found && bs != scale {
				return 0, false
			}
			scale, found = bs, true
		}
		return scale, found
	default:
		return 0, false
	}
}

func logicalScale(l avro.LogicalSchema) (int, bool) {
	d, ok := l.(*avro.DecimalLogicalSchema)
	if !ok {
		return 0, false
	}
	return d.Scale(), true
}

// arrayItems returns the item schema of an array schema, or nil.
func arrayItems(sch avro.Schema) avro.Schema {
	if a, ok := sch.(*avro.ArraySchema); ok {
		return a.Items()
	}
	return nil
}

// mapChildren returns a function giving the schema of each entry of a map
// decoded with sch: the field type for a record, the value type for a map.
func mapChildren(sch avro.Schema) func(string) avro.Schema {
	switch s := sch.(type) {
	case *avro.RecordSchema:
		fields := make(map[string]avro.Schema, len(s.Fields()))
		for _, f := range s.Fields() {
			fields[f.Name()] = f.Type()
		}
		return func(k string) avro.Schema { return fields[k] }
	case *avro.MapSchema:
		values := s.Values()
		return func(string) avro.Schema { return values }
	default:
		return func(string) avro.Schema { return nil }
	}
}
