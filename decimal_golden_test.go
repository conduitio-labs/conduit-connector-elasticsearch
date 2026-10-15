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

package elasticsearch_test

// Golden bulk bodies for Avro decimals (#171). Each case sends Avro-encoded
// records through the real destination with the SDK middleware, the SDK's
// in-memory schema service and a fake Elasticsearch, and compares the bulk
// request body with testdata/decimal/<case>.ndjson. Run with
// DECIMAL_GOLDEN_UPDATE=1 to rewrite the files, then review the diff: every
// decimal must be a JSON string of exact decimal text at the schema's scale.

import (
	"context"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/iskorotkov/avro/v2"
)

const (
	dec2  = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	dec3  = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}`
	dec0  = `{"type":"bytes","logicalType":"decimal","precision":20,"scale":0}`
	dec38 = `{"type":"bytes","logicalType":"decimal","precision":38,"scale":12}`
	fxd   = `{"type":"fixed","name":"money","size":16,"logicalType":"decimal","precision":30,"scale":4}`
)

func rat(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("bad decimal " + s)
	}
	return r
}

type decimalRecord struct {
	op     opencdc.Operation
	schema string
	// value is in the codec's own shape: unions as {"<branch>": value}.
	value map[string]any
	// keySchema and key, when keySchema is set, give an Avro key; otherwise
	// rawKey is used as is (empty: no key).
	keySchema string
	key       map[string]any
	rawKey    string
}

func TestDecimalBulkGolden(t *testing.T) {
	everywhere := rec(
		field("top", dec3),
		field("fixed", fxd),
		field("nullable", `["null",`+dec2+`]`),
		field("missing", `["null",`+dec2+`]`),
		field("arr", `{"type":"array","items":["null",`+dec2+`]}`),
		field("m", `{"type":"map","values":`+dec2+`}`),
		field("nested", `["null",{"type":"record","name":"inner","fields":[{"name":"price","type":"money"},{"name":"tags","type":{"type":"array","items":"money"}}]}]`),
		field("s", `"string"`),
	)
	everywhereValue := map[string]any{
		"top":      rat("-123.456"),
		"fixed":    rat("2.5"),
		"nullable": map[string]any{"bytes.decimal": rat("1.5")},
		"missing":  nil,
		"arr":      []any{nil, map[string]any{"bytes.decimal": rat("0.5")}, map[string]any{"bytes.decimal": rat("-3")}},
		"m":        map[string]any{"a": rat("100"), "b": rat("-0.01")},
		"nested": map[string]any{"inner": map[string]any{
			"price": rat("9.99"),
			"tags":  []any{rat("1"), rat("0.0001")},
		}},
		"s": "1.5 as a plain string",
	}

	cases := map[string][]decimalRecord{
		"top-level-negative": {{
			schema: rec(field("f", dec3)), value: map[string]any{"f": rat("-123.456")}, rawKey: "k1",
		}},
		"trailing-zeros": {{
			schema: rec(field("a", dec2), field("b", dec2), field("c", dec2)),
			value:  map[string]any{"a": rat("1.5"), "b": rat("100"), "c": rat("0")}, rawKey: "k1",
		}},
		"scale-zero": {{
			schema: rec(field("f", dec0)), value: map[string]any{"f": rat("-98765432109876543210")}, rawKey: "k1",
		}},
		"precision-38": {{
			schema: rec(field("f", dec38)),
			value:  map[string]any{"f": rat("-99999999999999999999999999.999999999999")}, rawKey: "k1",
		}},
		"fixed": {{
			schema: rec(field("f", fxd)), value: map[string]any{"f": rat("987.65")}, rawKey: "k1",
		}},
		"every-location": {{schema: everywhere, value: everywhereValue, rawKey: "k1"}},
		"keyless-create": {{
			op: opencdc.OperationCreate, schema: rec(field("f", dec2)), value: map[string]any{"f": rat("1.5")},
		}},
		"snapshot": {{
			op: opencdc.OperationSnapshot, schema: rec(field("f", dec2)), value: map[string]any{"f": rat("1.5")}, rawKey: "k1",
		}},
		// The key is the document ID and is deliberately left alone: changing
		// how a decimal key prints would move existing documents to new IDs.
		"decimal-key-unchanged": {{
			schema: rec(field("f", dec2)), value: map[string]any{"f": rat("1.5")},
			keySchema: rec(field("id", dec2)), key: map[string]any{"id": rat("1.5")},
		}},
		"batch-mixed": {
			{schema: rec(field("f", dec2)), value: map[string]any{"f": rat("1.5")}, rawKey: "k1"},
			{schema: rec(field("g", `"double"`)), value: map[string]any{"g": 1.5}, rawKey: "k2"},
			{schema: rec(field("f", dec3)), value: map[string]any{"f": rat("1.5")}, rawKey: "k3"},
			{op: opencdc.OperationDelete, rawKey: "k4"},
		},
	}

	update := os.Getenv("DECIMAL_GOLDEN_UPDATE") == "1"
	ctx := context.Background()
	for name, recs := range cases {
		t.Run(name, func(t *testing.T) {
			got := writeDecimalRecords(ctx, t, name, recs)
			path := filepath.Join("testdata", "decimal", name+".ndjson")
			if update {
				if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(got), 0o600); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if got != string(want) {
				t.Errorf("bulk body differs from %s\nwant:\n%s\ngot:\n%s", path, want, got)
			}
			if strings.Contains(stripKeys(got), "/") {
				t.Errorf("a ratio reached the document body:\n%s", got)
			}
		})
	}
}

// stripKeys drops the action lines, which carry document IDs (and the
// unchanged decimal key), so only document bodies are searched for ratios.
func stripKeys(bulk string) string {
	var docs []string
	for _, line := range strings.Split(bulk, "\n") {
		if strings.HasPrefix(line, `{"update"`) || strings.HasPrefix(line, `{"create"`) ||
			strings.HasPrefix(line, `{"delete"`) || strings.HasPrefix(line, `{"index"`) {
			continue
		}
		docs = append(docs, line)
	}
	return strings.Join(docs, "\n")
}

func writeDecimalRecords(ctx context.Context, t *testing.T, name string, recs []decimalRecord) string {
	t.Helper()
	fake := &fakeES{}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	dest := openDestination(ctx, t, map[string]string{"version": "8", "host": srv.URL, "index": "idx"})
	defer func() {
		if err := dest.Teardown(ctx); err != nil {
			t.Error(err)
		}
	}()

	records := make([]opencdc.Record, len(recs))
	for i, r := range recs {
		op := r.op
		if op == 0 {
			op = opencdc.OperationUpdate
		}
		out := opencdc.Record{
			Position:  opencdc.Position("p"),
			Operation: op,
			Metadata:  opencdc.Metadata{},
		}
		if r.schema != "" {
			sch := createSchema(ctx, t, name+".payload", r.schema)
			out.Payload.After = opencdc.RawData(encodeAvro(t, r.schema, r.value))
			out.Metadata.SetPayloadSchemaSubject(sch.Subject)
			out.Metadata.SetPayloadSchemaVersion(sch.Version)
		}
		switch {
		case r.keySchema != "":
			ksch := createSchema(ctx, t, name+".key", r.keySchema)
			out.Key = opencdc.RawData(encodeAvro(t, r.keySchema, r.key))
			out.Metadata.SetKeySchemaSubject(ksch.Subject)
			out.Metadata.SetKeySchemaVersion(ksch.Version)
		case r.rawKey != "":
			out.Key = opencdc.RawData(r.rawKey)
		}
		records[i] = out
	}

	n, err := dest.Write(ctx, records)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(records) {
		t.Fatalf("wrote %d of %d records", n, len(records))
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return strings.Join(fake.bulks, "\n---\n")
}

func createSchema(ctx context.Context, t *testing.T, subject, text string) schema.Schema {
	t.Helper()
	sch, err := schema.Create(ctx, schema.TypeAvro, "decimal-golden."+subject, []byte(text))
	if err != nil {
		t.Fatalf("create schema %s: %v", subject, err)
	}
	return sch
}

// encodeAvro encodes v with the codec directly, so unions can be given in the
// codec's {"<branch>": value} form. Each call gets a fresh codec API: the
// shared one can reuse an encoder built for another schema's named fixed at a
// different scale (ConduitIO/conduit-commons#305).
func encodeAvro(t *testing.T, text string, v map[string]any) []byte {
	t.Helper()
	sch, err := avro.ParseBytesWithCache([]byte(text), "", &avro.SchemaCache{})
	if err != nil {
		t.Fatalf("parse %s: %v", text, err)
	}
	b, err := avro.Config{}.Freeze().Marshal(sch, v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}
