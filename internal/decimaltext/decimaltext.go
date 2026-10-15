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
//     or a union with two decimal branches of different scales) the text has
//     the fewest digits that are exact ("1.5").
//   - Render never mutates its input. It returns new maps and slices for every
//     container on the way to a decimal.
//
// Render walks the shapes the conduit-commons Avro decoder produces: records
// and maps as map[string]any (opencdc.StructuredData at the top), arrays as
// []any, and union values unwrapped to the branch value.
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
	return render(v, sch, "")
}

func render(v any, sch avro.Schema, path string) (any, error) {
	sch = deref(sch)
	switch v := v.(type) {
	case *big.Rat:
		return renderRat(v, sch, path)
	case opencdc.StructuredData:
		out, err := renderMap(v, sch, path)
		return opencdc.StructuredData(out), err
	case map[string]any:
		return renderMap(v, sch, path)
	case []any:
		if !Contains(v) {
			return v, nil
		}
		items := arrayItems(sch)
		out := make([]any, len(v))
		for i, item := range v {
			r, err := render(item, items, fmt.Sprintf("%s[%d]", path, i))
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

func renderMap(v map[string]any, sch avro.Schema, path string) (map[string]any, error) {
	if !Contains(v) {
		return v, nil
	}
	childOf := mapChildren(v, sch)
	out := make(map[string]any, len(v))
	for k, item := range v {
		p := k
		if path != "" {
			p = path + "." + k
		}
		r, err := render(item, childOf(k), p)
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

// arrayItems returns the item schema of an array schema, or of the array
// branch of a union (Avro allows at most one), or nil.
func arrayItems(sch avro.Schema) avro.Schema {
	switch s := sch.(type) {
	case *avro.ArraySchema:
		return s.Items()
	case *avro.UnionSchema:
		for _, branch := range s.Types() {
			if a, ok := deref(branch).(*avro.ArraySchema); ok {
				return a.Items()
			}
		}
	}
	return nil
}

// mapChildren returns a function giving the schema of each entry of v, which
// was decoded with sch: the field type for a record, the value type for a map.
// For a union it picks the one record branch whose field names are exactly the
// keys of v (the decoder writes every field) or the map branch; when that
// leaves more or fewer than one candidate, entries get no schema.
func mapChildren(v map[string]any, sch avro.Schema) func(string) avro.Schema {
	if u, ok := sch.(*avro.UnionSchema); ok {
		var candidates []avro.Schema
		for _, branch := range u.Types() {
			switch b := deref(branch).(type) {
			case *avro.MapSchema:
				candidates = append(candidates, b)
			case *avro.RecordSchema:
				if sameKeys(b, v) {
					candidates = append(candidates, b)
				}
			}
		}
		sch = nil
		if len(candidates) == 1 {
			sch = candidates[0]
		}
	}
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
