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

package decimaltext

import (
	"errors"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/iskorotkov/avro/v2"
)

func rat(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("bad rat " + s)
	}
	return r
}

func TestFormat(t *testing.T) {
	tests := []struct {
		r     string
		scale int
		want  string
	}{
		{"-15432/125", 3, "-123.456"},
		{"3/2", 2, "1.50"},
		{"100", 2, "100.00"},
		{"7", 0, "7"},
		{"0", 3, "0.000"},
		{"-1/8", 3, "-0.125"},
		{"-1/8", 5, "-0.12500"},
		{"1/100", 2, "0.01"},
		{"-99999999999999999999999999999999999999", 0, "-99999999999999999999999999999999999999"},
		{"12345678901234567890123456789012345678/100000000000000000000", 20, "123456789012345678.90123456789012345678"},
	}
	for _, tt := range tests {
		t.Run(tt.r, func(t *testing.T) {
			got, err := Format(rat(tt.r), tt.scale)
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Format(%s, %d) = %q, want %q", tt.r, tt.scale, got, tt.want)
			}
		})
	}
}

func TestFormat_NotExact(t *testing.T) {
	for _, tt := range []struct {
		r     string
		scale int
	}{
		{"1/8", 2},  // needs 3 digits
		{"1/3", 10}, // never terminates
		{"-1/2", 0},
	} {
		if s, err := Format(rat(tt.r), tt.scale); !errors.Is(err, ErrNotExact) {
			t.Errorf("Format(%s, %d) = %q, %v; want ErrNotExact", tt.r, tt.scale, s, err)
		}
	}
	if _, err := Format(rat("1"), -1); err == nil {
		t.Error("negative scale accepted")
	}
}

func TestFormatExact(t *testing.T) {
	tests := map[string]string{
		"-15432/125": "-123.456",
		"3/2":        "1.5",
		"100":        "100",
		"0":          "0",
		"-1/8":       "-0.125",
		"1/1024":     "0.0009765625",
		"1/3125":     "0.00032",
		"19753/20":   "987.65",
		"-7":         "-7",
	}
	for in, want := range tests {
		got, err := FormatExact(rat(in))
		if err != nil {
			t.Fatalf("%s: %v", in, err)
		}
		if got != want {
			t.Errorf("FormatExact(%s) = %q, want %q", in, got, want)
		}
	}
	for _, in := range []string{"1/3", "2/7", "1/6"} {
		if s, err := FormatExact(rat(in)); !errors.Is(err, ErrNotExact) {
			t.Errorf("FormatExact(%s) = %q, %v; want ErrNotExact", in, s, err)
		}
	}
}

const (
	dec2 = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	fx3  = `{"type":"fixed","name":"fx3","size":8,"logicalType":"decimal","precision":12,"scale":3}`
)

func mustSchema(t *testing.T, s string) avro.Schema {
	t.Helper()
	sch, err := avro.ParseBytesWithCache([]byte(s), "", &avro.SchemaCache{})
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

func TestRender(t *testing.T) {
	tests := []struct {
		name   string
		schema string // empty: no schema
		in     any
		want   any
	}{
		{
			name:   "top-level field",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":` + dec2 + `},{"name":"s","type":"string"}]}`,
			in:     opencdc.StructuredData{"f": rat("3/2"), "s": "x"},
			want:   opencdc.StructuredData{"f": "1.50", "s": "x"},
		},
		{
			name:   "fixed",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":` + fx3 + `}]}`,
			in:     opencdc.StructuredData{"f": rat("2")},
			want:   opencdc.StructuredData{"f": "2.000"},
		},
		{
			name:   "nullable union",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":["null",` + dec2 + `]},{"name":"g","type":["null",` + dec2 + `]}]}`,
			in:     opencdc.StructuredData{"f": rat("5/4"), "g": nil},
			want:   opencdc.StructuredData{"f": "1.25", "g": nil},
		},
		{
			name:   "union with a string branch",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":["null","string",` + dec2 + `]}]}`,
			in:     opencdc.StructuredData{"f": rat("1/4")},
			want:   opencdc.StructuredData{"f": "0.25"},
		},
		{
			name:   "nested record",
			schema: `{"type":"record","name":"p","fields":[{"name":"r","type":{"type":"record","name":"in","fields":[{"name":"d","type":` + dec2 + `}]}}]}`,
			in:     opencdc.StructuredData{"r": map[string]any{"d": rat("10")}},
			want:   opencdc.StructuredData{"r": map[string]any{"d": "10.00"}},
		},
		{
			name: "nullable record holding a named fixed, then a reference to it",
			schema: `{"type":"record","name":"p","fields":[` +
				`{"name":"u","type":["null",{"type":"record","name":"in","fields":[{"name":"d","type":` + fx3 + `}]}]},` +
				`{"name":"ref","type":"fx3"}]}`,
			in:   opencdc.StructuredData{"u": map[string]any{"d": rat("-1/8")}, "ref": rat("7")},
			want: opencdc.StructuredData{"u": map[string]any{"d": "-0.125"}, "ref": "7.000"},
		},
		{
			name:   "array of nullable decimals",
			schema: `{"type":"record","name":"p","fields":[{"name":"a","type":{"type":"array","items":["null",` + dec2 + `]}}]}`,
			in:     opencdc.StructuredData{"a": []any{nil, rat("1/2"), rat("3")}},
			want:   opencdc.StructuredData{"a": []any{nil, "0.50", "3.00"}},
		},
		{
			name:   "nullable array",
			schema: `{"type":"record","name":"p","fields":[{"name":"a","type":["null",{"type":"array","items":` + dec2 + `}]}]}`,
			in:     opencdc.StructuredData{"a": []any{rat("1")}},
			want:   opencdc.StructuredData{"a": []any{"1.00"}},
		},
		{
			name:   "map values",
			schema: `{"type":"record","name":"p","fields":[{"name":"m","type":{"type":"map","values":` + dec2 + `}}]}`,
			in:     opencdc.StructuredData{"m": map[string]any{"a": rat("3"), "b": rat("-1/100")}},
			want:   opencdc.StructuredData{"m": map[string]any{"a": "3.00", "b": "-0.01"}},
		},
		{
			name:   "array of maps of records",
			schema: `{"type":"record","name":"p","fields":[{"name":"a","type":{"type":"array","items":{"type":"map","values":{"type":"record","name":"in","fields":[{"name":"d","type":` + fx3 + `}]}}}}]}`,
			in:     opencdc.StructuredData{"a": []any{map[string]any{"k": map[string]any{"d": rat("1/2")}}}},
			want:   opencdc.StructuredData{"a": []any{map[string]any{"k": map[string]any{"d": "0.500"}}}},
		},
		{
			name: "union of two records picks the one whose fields match",
			schema: `{"type":"record","name":"p","fields":[{"name":"u","type":["null",` +
				`{"type":"record","name":"a","fields":[{"name":"x","type":` + dec2 + `}]},` +
				`{"type":"record","name":"b","fields":[{"name":"x","type":` + fx3 + `},{"name":"y","type":"int"}]}]}]}`,
			in:   opencdc.StructuredData{"u": map[string]any{"x": rat("1"), "y": 1}},
			want: opencdc.StructuredData{"u": map[string]any{"x": "1.000", "y": 1}},
		},
		{
			name:   "union of decimals with different scales falls back to fewest digits",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":["null",` + dec2 + `,` + fx3 + `]}]}`,
			in:     opencdc.StructuredData{"f": rat("3/2")},
			want:   opencdc.StructuredData{"f": "1.5"},
		},
		{
			name:   "value that does not fit the scale keeps exact digits",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":` + dec2 + `}]}`,
			in:     opencdc.StructuredData{"f": rat("1/8")},
			want:   opencdc.StructuredData{"f": "0.125"},
		},
		{
			name:   "decimal where the schema has none",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":"string"}]}`,
			in:     opencdc.StructuredData{"f": rat("3/2")},
			want:   opencdc.StructuredData{"f": "1.5"},
		},
		{
			name: "no schema",
			in:   opencdc.StructuredData{"f": rat("3/2"), "a": []any{map[string]any{"d": rat("-1/8")}}},
			want: opencdc.StructuredData{"f": "1.5", "a": []any{map[string]any{"d": "-0.125"}}},
		},
		{
			name:   "no decimals",
			schema: `{"type":"record","name":"p","fields":[{"name":"f","type":"string"}]}`,
			in:     opencdc.StructuredData{"f": "x", "a": []any{1}},
			want:   opencdc.StructuredData{"f": "x", "a": []any{1}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sch avro.Schema
			if tt.schema != "" {
				sch = mustSchema(t, tt.schema)
			}
			before := deepCopy(tt.in)
			got, err := Render(tt.in, sch)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Render:\n got  %#v\n want %#v", got, tt.want)
			}
			if Contains(got) {
				t.Fatalf("a *big.Rat survived: %#v", got)
			}
			if !reflect.DeepEqual(tt.in, before) {
				t.Fatalf("Render mutated its input:\n now    %#v\n before %#v", tt.in, before)
			}
		})
	}
}

func TestRender_NotExact(t *testing.T) {
	_, err := Render(opencdc.StructuredData{"r": map[string]any{"f": rat("1/3")}}, nil)
	if !errors.Is(err, ErrNotExact) {
		t.Fatalf("got %v, want ErrNotExact", err)
	}
	if want := `field "r.f"`; err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q does not name the field %s", err, want)
	}
}

func TestContains(t *testing.T) {
	for _, v := range []any{
		rat("1"),
		opencdc.StructuredData{"a": rat("1")},
		map[string]any{"a": []any{map[string]any{"b": rat("1")}}},
		[]any{nil, rat("1")},
	} {
		if !Contains(v) {
			t.Errorf("Contains(%#v) = false", v)
		}
	}
	for _, v := range []any{nil, "1/2", opencdc.StructuredData{"a": 1.5}, []any{[]byte("x")}, opencdc.RawData("x")} {
		if Contains(v) {
			t.Errorf("Contains(%#v) = true", v)
		}
	}
}

func deepCopy(v any) any {
	switch v := v.(type) {
	case opencdc.StructuredData:
		return opencdc.StructuredData(deepCopy(map[string]any(v)).(map[string]any))
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = deepCopy(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopy(item)
		}
		return out
	case *big.Rat:
		return new(big.Rat).Set(v)
	default:
		return v
	}
}
