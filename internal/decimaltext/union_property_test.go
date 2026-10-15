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
	"fmt"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/iskorotkov/avro/v2"
)

// schemaGen builds random nested schemas with unions, maps, arrays, records
// and decimals, and values for them in the codec's encoding shape. Every
// decimal of scale s (3 to 8) holds s + 0.1, so its rendered text says which
// scale it was given: "4.1000" at scale 4 is right, "4.1" is the
// fewest-digits fallback (exact, unpadded), and any other number of
// fractional digits is another branch's scale. Map keys are drawn from the
// branch names in the schema, so data that looks like a wrapper is common.
type schemaGen struct {
	rng   *rand.Rand
	id    string
	n     int
	names []string // full names of named types and other branch names
}

type genType struct {
	json  string
	kind  string // decimal, fixed, record, map, array, union
	name  string // branch name in a wrapper
	scale int
	elem  *genType   // map values, array items
	items []*genType // record fields, union branches
}

func (g *schemaGen) next() string {
	g.n++
	return fmt.Sprintf("%s_%d", g.id, g.n)
}

func (g *schemaGen) decimalType(allowBytes bool) *genType {
	scale := 3 + g.rng.IntN(6)
	if allowBytes && g.rng.IntN(2) == 0 {
		return &genType{
			kind: "decimal", name: bytesDecimal, scale: scale,
			json: fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":12,"scale":%d}`, scale),
		}
	}
	name := g.next()
	g.names = append(g.names, name)
	return &genType{
		kind: "fixed", name: name, scale: scale,
		json: fmt.Sprintf(`{"type":"fixed","name":%q,"size":8,"logicalType":"decimal","precision":12,"scale":%d}`, name, scale),
	}
}

func (g *schemaGen) typ(depth int) *genType {
	choice := g.rng.IntN(6)
	if depth >= 3 {
		choice = 0
	}
	switch choice {
	case 0:
		return g.decimalType(true)
	case 1:
		name := g.next()
		g.names = append(g.names, name)
		t := &genType{kind: "record", name: name}
		var fields []string
		for i := 0; i < 1+g.rng.IntN(2); i++ {
			f := g.typ(depth + 1)
			t.items = append(t.items, f)
			fields = append(fields, fmt.Sprintf(`{"name":"f%d","type":%s}`, i, f.json))
		}
		t.json = fmt.Sprintf(`{"type":"record","name":%q,"fields":[%s]}`, name, strings.Join(fields, ","))
		return t
	case 2:
		e := g.typ(depth + 1)
		return &genType{kind: "map", name: "map", elem: e, json: `{"type":"map","values":` + e.json + `}`}
	case 3:
		e := g.typ(depth + 1)
		return &genType{kind: "array", name: "array", elem: e, json: `{"type":"array","items":` + e.json + `}`}
	default:
		return g.union(depth)
	}
}

// union builds ["null", X] or ["null", X, Y] with at most one bytes, map and
// array branch, as Avro requires.
func (g *schemaGen) union(depth int) *genType {
	t := &genType{kind: "union"}
	used := map[string]bool{}
	branches := []string{`"null"`}
	for i := 0; i < 1+g.rng.IntN(2); i++ {
		var b *genType
		for {
			switch g.rng.IntN(4) {
			case 0:
				b = g.decimalType(!used[bytesDecimal])
			case 1:
				b = g.typ(depth + 1)
				if b.kind == "union" {
					continue
				}
			case 2:
				e := g.typ(depth + 1)
				b = &genType{kind: "map", name: "map", elem: e, json: `{"type":"map","values":` + e.json + `}`}
			default:
				e := g.typ(depth + 1)
				b = &genType{kind: "array", name: "array", elem: e, json: `{"type":"array","items":` + e.json + `}`}
			}
			if b.kind != "union" && !used[b.name] {
				break
			}
		}
		used[b.name] = true
		t.items = append(t.items, b)
		branches = append(branches, b.json)
	}
	t.json = "[" + strings.Join(branches, ",") + "]"
	return t
}

func (g *schemaGen) value(t *genType) any {
	switch t.kind {
	case "decimal", "fixed":
		return new(big.Rat).SetFrac64(int64(t.scale)*10+1, 10)
	case "record":
		m := map[string]any{}
		for i, f := range t.items {
			m[fmt.Sprintf("f%d", i)] = g.value(f)
		}
		return m
	case "map":
		m := map[string]any{}
		for i := 0; i < 1+g.rng.IntN(2); i++ {
			key := "k"
			if len(g.names) > 0 && g.rng.IntN(2) == 0 {
				key = g.names[g.rng.IntN(len(g.names))]
			} else if g.rng.IntN(2) == 0 {
				key = []string{bytesDecimal, "map", "array", "null"}[g.rng.IntN(4)]
			}
			m[key] = g.value(t.elem)
		}
		return m
	case "array":
		var a []any
		for i := 0; i < 1+g.rng.IntN(2); i++ {
			a = append(a, g.value(t.elem))
		}
		return a
	case "union":
		if g.rng.IntN(5) == 0 {
			return nil
		}
		b := t.items[g.rng.IntN(len(t.items))]
		return map[string]any{b.name: g.value(b)}
	}
	panic(t.kind)
}

// commonsMisaligns reports whether t (a map value or array item when
// underContainer) holds a union that the codec wraps, that is not a map value
// or array item, and that has another union below it. commons' unwrap walk
// steps through such a union with the wrapper still in place, and depending on
// the branches it then leaves unions wrapped, drops a map key, or rejects the
// record (ConduitIO/conduit-commons#312). The wrapped-but-unreached part is
// what unionshape.go models and is covered by TestRender_CodecUnions; the
// generator skips the whole class because the mangled data has no right
// answer.
func commonsMisaligns(t *genType, underContainer bool) bool {
	switch t.kind {
	case "record":
		for _, f := range t.items {
			if commonsMisaligns(f, false) {
				return true
			}
		}
	case "map", "array":
		return commonsMisaligns(t.elem, true)
	case "union":
		wrappedByCodec := false
		for _, b := range t.items {
			if b.kind != "decimal" {
				wrappedByCodec = true
			}
		}
		for _, b := range t.items {
			if !underContainer && wrappedByCodec && hasUnion(b) {
				return true
			}
			if commonsMisaligns(b, underContainer) {
				return true
			}
		}
	}
	return false
}

func hasUnion(t *genType) bool {
	if t.kind == "union" {
		return true
	}
	if t.elem != nil && hasUnion(t.elem) {
		return true
	}
	for _, i := range t.items {
		if hasUnion(i) {
			return true
		}
	}
	return false
}

// checkSelfDescribing fails on decimal text with another branch's scale, or
// on a *big.Rat left in v, and counts fewest-digits fallbacks.
func checkSelfDescribing(t *testing.T, v any, path string, fallbacks *int) {
	t.Helper()
	switch v := v.(type) {
	case string:
		intPart, frac, ok := strings.Cut(v, ".")
		s, err := strconv.Atoi(intPart)
		switch {
		case !ok || err != nil || frac == "" || frac[0] != '1' || strings.Trim(frac[1:], "0") != "":
			t.Errorf("%s: %q is not the generated value", path, v)
		case len(frac) == s:
		case len(frac) == 1:
			*fallbacks++
		default:
			t.Errorf("%s: %q has %d fractional digits, its scale is %d", path, v, len(frac), s)
		}
	case *big.Rat:
		t.Errorf("%s: *big.Rat survived", path)
	case opencdc.StructuredData:
		checkSelfDescribing(t, map[string]any(v), path, fallbacks)
	case map[string]any:
		for k, item := range v {
			checkSelfDescribing(t, item, path+"."+k, fallbacks)
		}
	case []any:
		for i, item := range v {
			checkSelfDescribing(t, item, fmt.Sprintf("%s[%d]", path, i), fallbacks)
		}
	}
}

// TestProperty_UnionShapes checks the union shape rules (unionshape.go)
// against what the codec and commons really decode, over random schemas: each
// decimal must get the scale of the branch it was encoded with.
func TestProperty_UnionShapes(t *testing.T) {
	rng := rand.New(rand.NewPCG(propertySeed(t), 1)) //nolint:gosec // deterministic, replayable test input
	iterations, checked, skipped, fallbacks := 1500, 0, 0, 0
	if testing.Short() {
		iterations = 150
	}
	for i := 0; i < iterations; i++ {
		g := &schemaGen{rng: rng, id: fmt.Sprintf("t%d", i)}
		f := g.union(0)
		if rng.IntN(3) == 0 {
			f = g.typ(0)
		}
		text := `{"type":"record","name":"` + g.next() + `","fields":[{"name":"f","type":` + f.json + `}]}`
		value := g.value(f)
		if commonsMisaligns(f, false) {
			skipped++
			continue
		}

		sch := mustSchema(t, text)
		encoded, err := avro.Config{}.Freeze().Marshal(sch, map[string]any{"f": value})
		if err != nil {
			t.Fatalf("iteration %d: encode %s: %v", i, text, err)
		}
		serde, err := cavro.Parse([]byte(text))
		if err != nil {
			t.Fatalf("iteration %d: parse: %v", i, err)
		}
		var decoded opencdc.StructuredData
		if err := serde.Unmarshal(encoded, &decoded); err != nil {
			// commons rejects some shapes outright; nothing reaches Write.
			continue
		}
		got, err := Render(decoded, sch)
		if err != nil {
			t.Fatalf("iteration %d: render: %v", i, err)
		}
		checked++
		before := t.Failed()
		checkSelfDescribing(t, got, "f", &fallbacks)
		if !before && t.Failed() {
			t.Fatalf("iteration %d\nschema  %s\ndecoded %#v", i, text, decoded)
		}
	}
	t.Logf("%d of %d schemas decoded and checked, %d skipped (commons misaligns), %d decimals fell back to fewest digits",
		checked, iterations, skipped, fallbacks)
	if checked < iterations/2 {
		t.Fatalf("only %d of %d schemas decoded; the generator drifted", checked, iterations)
	}
}
