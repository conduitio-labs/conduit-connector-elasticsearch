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
	"encoding/json"
	"math/big"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/iskorotkov/avro/v2"
)

// TestRender_CodecUnions renders what the codec and commons actually decode,
// not hand-built values: some unions come back unwrapped, others wrapped as
// {"<branch full name>": value}. The wrapper shape is kept as it is (#173);
// what is checked is that every decimal gets its own branch's scale.
func TestRender_CodecUnions(t *testing.T) {
	const (
		d2  = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
		d6  = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":6}`
		fx4 = `{"type":"fixed","name":"fx","namespace":"n2","size":8,"logicalType":"decimal","precision":10,"scale":4}`
	)
	r := func() *big.Rat { return big.NewRat(3, 2) }
	tests := []struct {
		name string
		typ  string
		// value in the codec's encoding shape: unions as {"<branch>": value}.
		value any
		want  string // JSON of the rendered field
	}{
		{
			// The review probe: a decimal branch next to a map of decimals.
			name: "map or decimal, decimal branch", typ: `["null",{"type":"map","values":` + d6 + `},` + d2 + `]`,
			value: map[string]any{"bytes.decimal": r()}, want: `{"bytes.decimal":"1.50"}`,
		},
		{
			name: "map or decimal, map branch", typ: `["null",{"type":"map","values":` + d6 + `},` + d2 + `]`,
			value: map[string]any{"map": map[string]any{"k": r()}}, want: `{"map":{"k":"1.500000"}}`,
		},
		{
			name: "string or decimal", typ: `["null","string",` + d2 + `]`,
			value: map[string]any{"bytes.decimal": r()}, want: `"1.50"`,
		},
		{
			name: "two records, namespaced one", typ: `["null",{"type":"record","name":"a","namespace":"ns","fields":[{"name":"x","type":` + d2 + `}]},{"type":"record","name":"b","fields":[{"name":"x","type":` + d6 + `}]}]`,
			value: map[string]any{"ns.a": map[string]any{"x": r()}}, want: `{"ns.a":{"x":"1.50"}}`,
		},
		{
			name: "two records, other one", typ: `["null",{"type":"record","name":"a","namespace":"ns","fields":[{"name":"x","type":` + d2 + `}]},{"type":"record","name":"b","fields":[{"name":"x","type":` + d6 + `}]}]`,
			value: map[string]any{"b": map[string]any{"x": r()}}, want: `{"b":{"x":"1.500000"}}`,
		},
		{
			name: "nullable record", typ: `["null",{"type":"record","name":"c","fields":[{"name":"x","type":` + d2 + `}]}]`,
			value: map[string]any{"c": map[string]any{"x": r()}}, want: `{"x":"1.50"}`,
		},
		{
			name: "fixed or bytes decimal, fixed branch", typ: `["null",` + fx4 + `,` + d2 + `]`,
			value: map[string]any{"n2.fx": r()}, want: `{"n2.fx":"1.5000"}`,
		},
		{
			name: "fixed or bytes decimal, bytes branch", typ: `["null",` + fx4 + `,` + d2 + `]`,
			value: map[string]any{"bytes.decimal": r()}, want: `{"bytes.decimal":"1.50"}`,
		},
		{
			name: "array of string-or-decimal", typ: `{"type":"array","items":["null","string",` + d2 + `]}`,
			value: []any{map[string]any{"bytes.decimal": r()}, nil}, want: `["1.50",null]`,
		},
		{
			name: "array of nullable records", typ: `{"type":"array","items":["null",{"type":"record","name":"e","fields":[{"name":"x","type":` + d2 + `}]}]}`,
			value: []any{map[string]any{"e": map[string]any{"x": r()}}}, want: `[{"e":{"x":"1.50"}}]`,
		},
		{
			name: "map of int-or-decimal", typ: `{"type":"map","values":["null","int",` + d2 + `]}`,
			value: map[string]any{"k": map[string]any{"bytes.decimal": r()}}, want: `{"k":"1.50"}`,
		},
		{
			name: "array or decimal, array branch", typ: `["null",{"type":"array","items":` + d6 + `},` + d2 + `]`,
			value: map[string]any{"array": []any{r()}}, want: `{"array":["1.500000"]}`,
		},
		// The delta review's probes: inside containers commons has already
		// unwrapped, a user key that equals a branch name is data, not a
		// wrapper.
		{
			name: "review: array of union, map item with key amount", typ: `{"type":"array","items":["null",{"type":"fixed","name":"amount","size":8,"logicalType":"decimal","precision":10,"scale":2},{"type":"map","values":` + d6 + `}]}`,
			value: []any{map[string]any{"map": map[string]any{"amount": r()}}, map[string]any{"amount": r()}}, want: `[{"amount":"1.500000"},"1.50"]`,
		},
		{
			name: "review: map of union, map value with key bytes.decimal", typ: `{"type":"map","values":["null",{"type":"map","values":` + d6 + `},` + d2 + `]}`,
			value: map[string]any{"k": map[string]any{"map": map[string]any{"bytes.decimal": r()}}, "j": map[string]any{"bytes.decimal": r()}}, want: `{"j":"1.50","k":{"bytes.decimal":"1.500000"}}`,
		},
		{
			name: "review: nullable map with key map", typ: `["null",{"type":"map","values":` + d2 + `}]`,
			value: map[string]any{"map": map[string]any{"map": r()}}, want: `{"map":"1.50"}`,
		},
		{
			name: "nullable record in a nullable record", typ: `["null",{"type":"record","name":"o","fields":[{"name":"g","type":["null",{"type":"record","name":"in","fields":[{"name":"x","type":` + d2 + `}]}]}]}]`,
			value: map[string]any{"o": map[string]any{"g": map[string]any{"in": map[string]any{"x": r()}}}}, want: `{"g":{"in":{"x":"1.50"}}}`,
		},
		{
			name: "nullable record field inside an array-of-union item", typ: `{"type":"array","items":["null",{"type":"array","items":"int"},{"type":"record","name":"it","fields":[{"name":"g","type":["null",{"type":"record","name":"in2","fields":[{"name":"x","type":` + d2 + `}]}]}]}]}`,
			value: []any{map[string]any{"it": map[string]any{"g": map[string]any{"in2": map[string]any{"x": r()}}}}}, want: `[{"g":{"x":"1.50"}}]`,
		},
		{
			name: "nullable map as map values", typ: `{"type":"map","values":["null",{"type":"map","values":` + d2 + `}]}`,
			value: map[string]any{"k": map[string]any{"map": map[string]any{"map": r()}}}, want: `{"k":{"map":"1.50"}}`,
		},
		{
			name: "nullable record inside a multi-branch record union", typ: `["null",{"type":"record","name":"ra","fields":[{"name":"g","type":["null",{"type":"record","name":"in3","fields":[{"name":"x","type":` + d2 + `}]}]}]},{"type":"record","name":"rb","fields":[{"name":"y","type":"int"}]}]`,
			value: map[string]any{"ra": map[string]any{"g": map[string]any{"in3": map[string]any{"x": r()}}}}, want: `{"ra":{"g":{"in3":{"x":"1.50"}}}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			text := `{"type":"record","name":"p","fields":[{"name":"f","type":` + tt.typ + `}]}`
			sch := mustSchema(t, text)
			encoded, err := avro.Config{}.Freeze().Marshal(sch, map[string]any{"f": tt.value})
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			serde, err := cavro.Parse([]byte(text))
			if err != nil {
				t.Fatal(err)
			}
			var decoded opencdc.StructuredData
			if err := serde.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got, err := Render(decoded, sch)
			if err != nil {
				t.Fatal(err)
			}
			out, err := json.Marshal(got.(opencdc.StructuredData)["f"])
			if err != nil {
				t.Fatal(err)
			}
			if string(out) != tt.want {
				t.Fatalf("decoded %#v\nrendered %s, want %s", decoded["f"], out, tt.want)
			}
		})
	}
}
