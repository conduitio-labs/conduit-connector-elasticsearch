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

// Avro codec equivalence across the hamba/avro -> iskorotkov/avro move
// (ConduitIO/conduit#3012).
//
// This connector never imports an Avro library. The codec is reached through
// the SDK middleware, which uses conduit-commons:
//
//   - destination: sdk.schema.extract.{key,payload}.enabled (default true)
//     decodes Avro-encoded keys and payloads into opencdc.StructuredData before
//     Write sees them, and Write turns that into the Elasticsearch document;
//   - source: sdk.schema.extract.key.enabled (default true) derives an Avro
//     schema from the structured key {"id": <document id>}, registers it and
//     encodes the key with it. The payload is raw JSON and is not touched.
//
// The fixtures in testdata/avro_equivalence_fixtures.json were produced by
// this test on the previous main (SDK v0.12.0, conduit-commons v0.6.0, codec
// hamba/avro v2.31.0) with AVRO_FIXTURE_GEN=1. The test now runs the same
// cases through the current module and requires identical results. The
// fixtures are frozen: never regenerate them with the new codec. Invariant 6:
// schema handling never silently mangles data.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	elasticsearch "github.com/conduitio-labs/conduit-connector-elasticsearch"
	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/conduitio/conduit-connector-sdk/schema"
)

const avroFixturePath = "testdata/avro_equivalence_fixtures.json"

type avroFixtures struct {
	// Codec is the Avro module the fixtures were generated with.
	Codec       string               `json:"codec"`
	Destination []destinationFixture `json:"destination"`
	Source      []sourceFixture      `json:"source"`
}

type destinationFixture struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	// PayloadAvro is the payload encoded by the old codec (base64). When
	// encoding failed, EncodeError is set instead.
	PayloadAvro string `json:"payloadAvro"`
	EncodeError string `json:"encodeError,omitempty"`
	// KeyAvro is the key {"id": "key-<name>"} encoded by the old codec.
	KeyAvro string `json:"keyAvro"`
	// Decoded is the %#v of what PayloadAvro decodes into.
	Decoded     string `json:"decoded,omitempty"`
	DecodeError string `json:"decodeError,omitempty"`
	// Bulk is the bulk request body Write sent to Elasticsearch for a record
	// carrying PayloadAvro and an Avro-encoded key.
	Bulk       string `json:"bulk,omitempty"`
	Written    int    `json:"written"`
	WriteError string `json:"writeError,omitempty"`

	// FreshDecoded is the %#v of this run's own encoding decoded again. Only
	// compared for unordered cases; never stored.
	FreshDecoded string `json:"-"`
}

type sourceFixture struct {
	Name  string `json:"name"`
	DocID string `json:"docID"`
	// KeyType is the Go type of the key the source emitted.
	KeyType string `json:"keyType"`
	// KeyBytes is the key as the SDK hands it to Conduit (base64).
	KeyBytes string `json:"keyBytes"`
	// KeySubject, KeySchema and KeyDecoded are set when a key schema was
	// attached: subject:version, the registered schema text, and the %#v of
	// the key decoded with it.
	KeySubject string `json:"keySubject,omitempty"`
	KeySchema  string `json:"keySchema,omitempty"`
	KeyDecoded string `json:"keyDecoded,omitempty"`
	Payload    string `json:"payload"`
	// PayloadSchema is set when the payload carried a schema (it should not:
	// the payload is raw JSON).
	PayloadSchema string `json:"payloadSchema,omitempty"`
}

// keySchema is the shape the source emits for its key, so the destination
// cases also cover an Elasticsearch-to-Elasticsearch pipeline.
const keySchema = `{"type":"record","name":"key","fields":[{"name":"id","type":"string"}]}`

type destinationCase struct {
	name   string
	schema string
	value  opencdc.StructuredData
}

// unorderedCases hold a multi-entry Avro map. Map entries are encoded in Go
// map iteration order, so their bytes differ between runs on either codec;
// their fresh encoding is checked by decoding it instead. The frozen bytes are
// still what the destination decodes.
var unorderedCases = map[string]bool{"map-long": true}

func ts(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func rec(fields ...string) string {
	return `{"type":"record","name":"payload","fields":[` + strings.Join(fields, ",") + `]}`
}

func field(name, typ string) string {
	return fmt.Sprintf(`{"name":%q,"type":%s}`, name, typ)
}

// destinationCases covers every Avro type the commons codec maps, with the Go
// value shapes Conduit records carry.
func destinationCases() []destinationCase {
	return []destinationCase{
		{"boolean", rec(field("f", `"boolean"`)), opencdc.StructuredData{"f": true}},
		{"int", rec(field("f", `"int"`)), opencdc.StructuredData{"f": int32(-2147483648)}},
		{"int-from-int", rec(field("f", `"int"`)), opencdc.StructuredData{"f": 42}},
		{"long", rec(field("f", `"long"`)), opencdc.StructuredData{"f": int64(9007199254740993)}},
		{"long-from-int", rec(field("f", `"long"`)), opencdc.StructuredData{"f": 7}},
		{"float", rec(field("f", `"float"`)), opencdc.StructuredData{"f": float32(3.14159)}},
		{"double", rec(field("f", `"double"`)), opencdc.StructuredData{"f": 1.0e-300}},
		{"double-negative-zero", rec(field("f", `"double"`)), opencdc.StructuredData{"f": negZero()}},
		{"string", rec(field("f", `"string"`)), opencdc.StructuredData{"f": "héllo \"wörld\" <tag> &  "}},
		{"string-empty", rec(field("f", `"string"`)), opencdc.StructuredData{"f": ""}},
		{"bytes", rec(field("f", `"bytes"`)), opencdc.StructuredData{"f": []byte{0, 1, 2, 0xff}}},
		{"bytes-empty", rec(field("f", `"bytes"`)), opencdc.StructuredData{"f": []byte{}}},
		{"null", rec(field("f", `"null"`)), opencdc.StructuredData{"f": nil}},
		{"nullable-string-null", rec(field("f", `["null","string"]`)), opencdc.StructuredData{"f": nil}},
		{"nullable-string-value", rec(field("f", `["null","string"]`)), opencdc.StructuredData{"f": "x"}},
		{"nullable-long-value", rec(field("f", `["null","long"]`)), opencdc.StructuredData{"f": int64(5)}},
		{"nullable-double-value", rec(field("f", `["null","double"]`)), opencdc.StructuredData{"f": 2.5}},
		{"nullable-record", rec(field("f", `["null",{"type":"record","name":"inner","fields":[{"name":"a","type":"int"}]}]`)), opencdc.StructuredData{"f": map[string]any{"a": 1}}},
		{"enum", rec(field("f", `{"type":"enum","name":"color","symbols":["RED","GREEN"]}`)), opencdc.StructuredData{"f": "GREEN"}},
		{"fixed", rec(field("f", `{"type":"fixed","name":"four","size":4}`)), opencdc.StructuredData{"f": [4]byte{1, 2, 3, 4}}},
		{"array-int", rec(field("f", `{"type":"array","items":"int"}`)), opencdc.StructuredData{"f": []any{1, 2, 3}}},
		{"array-empty", rec(field("f", `{"type":"array","items":"string"}`)), opencdc.StructuredData{"f": []any{}}},
		{"array-empty-in-record", rec(field("f", `{"type":"record","name":"inner","fields":[{"name":"a","type":{"type":"array","items":"int"}}]}`)), opencdc.StructuredData{"f": map[string]any{"a": []any{}}}},
		{"array-empty-in-union", rec(field("f", `["null",{"type":"array","items":"int"}]`)), opencdc.StructuredData{"f": []any{}}},
		{"array-of-empty-arrays", rec(field("f", `{"type":{"type":"array","items":{"type":"array","items":"int"}}}`)), opencdc.StructuredData{"f": []any{[]any{}, []any{1}}}},
		{"map-of-empty-array", rec(field("f", `{"type":"map","values":{"type":"array","items":"int"}}`)), opencdc.StructuredData{"f": map[string]any{"k": []any{}}}},
		{"array-nullable", rec(field("f", `{"type":"array","items":["null","string"]}`)), opencdc.StructuredData{"f": []any{"a", nil}}},
		{"map-long", rec(field("f", `{"type":"map","values":"long"}`)), opencdc.StructuredData{"f": map[string]any{"b": int64(2), "a": int64(1)}}},
		{"map-single", rec(field("f", `{"type":"map","values":"string"}`)), opencdc.StructuredData{"f": map[string]any{"k": "v"}}},
		{"map-empty", rec(field("f", `{"type":"map","values":"string"}`)), opencdc.StructuredData{"f": map[string]any{}}},
		{"record-nested", rec(field("f", `{"type":"record","name":"inner","fields":[{"name":"a","type":"string"},{"name":"b","type":{"type":"array","items":"double"}}]}`)), opencdc.StructuredData{"f": map[string]any{"a": "x", "b": []any{1.5, 2.5}}}},
		{"date", rec(field("f", `{"type":"int","logicalType":"date"}`)), opencdc.StructuredData{"f": ts("2024-02-29T00:00:00Z")}},
		{"time-millis", rec(field("f", `{"type":"int","logicalType":"time-millis"}`)), opencdc.StructuredData{"f": 13*time.Hour + 14*time.Minute + 15*time.Second + 16*time.Millisecond}},
		{"time-micros", rec(field("f", `{"type":"long","logicalType":"time-micros"}`)), opencdc.StructuredData{"f": 13*time.Hour + 17*time.Microsecond}},
		{"timestamp-millis", rec(field("f", `{"type":"long","logicalType":"timestamp-millis"}`)), opencdc.StructuredData{"f": ts("2024-05-06T07:08:09.123Z")}},
		{"timestamp-micros", rec(field("f", `{"type":"long","logicalType":"timestamp-micros"}`)), opencdc.StructuredData{"f": ts("2024-05-06T07:08:09.123456Z")}},
		{"timestamp-pre-epoch", rec(field("f", `{"type":"long","logicalType":"timestamp-micros"}`)), opencdc.StructuredData{"f": ts("1969-12-31T23:59:59.999999Z")}},
		{"local-timestamp-millis", rec(field("f", `{"type":"long","logicalType":"local-timestamp-millis"}`)), opencdc.StructuredData{"f": ts("2024-05-06T07:08:09.123Z")}},
		{"local-timestamp-micros", rec(field("f", `{"type":"long","logicalType":"local-timestamp-micros"}`)), opencdc.StructuredData{"f": ts("2024-05-06T07:08:09.123456Z")}},
		{"decimal-bytes", rec(field("f", `{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}`)), opencdc.StructuredData{"f": big.NewRat(-123456, 1000)}},
		{"decimal-fixed", rec(field("f", `{"type":"fixed","name":"dec","size":8,"logicalType":"decimal","precision":12,"scale":2}`)), opencdc.StructuredData{"f": big.NewRat(98765, 100)}},
		{"nullable-decimal", rec(field("f", `["null",{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}]`)), opencdc.StructuredData{"f": big.NewRat(5, 4)}},
		{"uuid", rec(field("f", `{"type":"string","logicalType":"uuid"}`)), opencdc.StructuredData{"f": "c0ffee00-1234-5678-9abc-def012345678"}},
		{"field-default", rec(field("f", `"string"`), `{"name":"g","type":"int","default":7}`), opencdc.StructuredData{"f": "x", "g": 8}},
		{"field-order", rec(field("z", `"int"`), field("a", `"int"`), field("m", `"string"`)), opencdc.StructuredData{"a": 1, "m": "m", "z": 26}},
		{"extra-map-key-ignored", rec(field("f", `"string"`)), opencdc.StructuredData{"f": "x", "notInSchema": "y"}},
		{"missing-field", rec(field("f", `"string"`), field("g", `"string"`)), opencdc.StructuredData{"f": "x"}},
		{"type-mismatch", rec(field("f", `"int"`)), opencdc.StructuredData{"f": "not an int"}},
		{"every-type", rec(
			field("b", `"boolean"`),
			field("i", `"int"`),
			field("l", `"long"`),
			field("fl", `"float"`),
			field("d", `"double"`),
			field("s", `"string"`),
			field("by", `"bytes"`),
			field("n", `["null","string"]`),
			field("e", `{"type":"enum","name":"e","symbols":["A","B"]}`),
			field("fx", `{"type":"fixed","name":"fx","size":2}`),
			field("a", `{"type":"array","items":"long"}`),
			field("m", `{"type":"map","values":"boolean"}`),
			field("r", `{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}`),
			field("dt", `{"type":"int","logicalType":"date"}`),
			field("tsm", `{"type":"long","logicalType":"timestamp-millis"}`),
			field("dec", `{"type":"bytes","logicalType":"decimal","precision":6,"scale":2}`),
			field("u", `{"type":"string","logicalType":"uuid"}`),
		), opencdc.StructuredData{
			"b": false, "i": 1, "l": int64(2), "fl": float32(0.5), "d": 0.25, "s": "s",
			"by": []byte("by"), "n": "n", "e": "B", "fx": [2]byte{9, 8},
			"a": []any{int64(1), int64(2)}, "m": map[string]any{"t": true},
			"r": map[string]any{"x": 3}, "dt": ts("2000-01-01T00:00:00Z"),
			"tsm": ts("2000-01-01T00:00:00.001Z"), "dec": big.NewRat(31415, 100),
			"u": "00000000-0000-0000-0000-000000000000",
		}},
	}
}

func negZero() float64 {
	z := 0.0
	return -z
}

// emptyArrayChange lists the cases where the move changes a result, and the
// only kind of change it makes: an empty Avro array used to decode to a nil
// slice, which the destination wrote to Elasticsearch as null; it now decodes
// to an empty slice and is written as []. The cause is hamba/avro itself: this
// module was on v2.28.0 (through conduit-commons v0.6.0), v2.29.0 started
// returning empty slices, and the fork carries that. It is not a commons or
// SDK change. Each entry pins the old and the new value, so the change cannot
// spread to other shapes without failing the test.
//
// The new value is the faithful one: null is also what the "null" branch of
// ["null", {"type":"array"}] produces, so the old output could not tell an
// empty array from a missing one.
var emptyArrayChange = map[string]struct{ from, to, docFrom, docTo string }{
	"array-empty": {
		`map{"f": []any(nil)}`, `map{"f": []any{}}`,
		`{"doc":{"f":null},"doc_as_upsert":true}`, `{"doc":{"f":[]},"doc_as_upsert":true}`,
	},
	"array-empty-in-record": {
		`map{"f": map{"a": []any(nil)}}`, `map{"f": map{"a": []any{}}}`,
		`{"doc":{"f":{"a":null}},"doc_as_upsert":true}`, `{"doc":{"f":{"a":[]}},"doc_as_upsert":true}`,
	},
	"array-empty-in-union": {
		`map{"f": []any(nil)}`, `map{"f": []any{}}`,
		`{"doc":{"f":null},"doc_as_upsert":true}`, `{"doc":{"f":[]},"doc_as_upsert":true}`,
	},
	"map-of-empty-array": {
		`map{"f": map{"k": []any(nil)}}`, `map{"f": map{"k": []any{}}}`,
		`{"doc":{"f":{"k":null}},"doc_as_upsert":true}`, `{"doc":{"f":{"k":[]}},"doc_as_upsert":true}`,
	},
}

// sourceDocIDs are the Elasticsearch document IDs the source turns into keys.
var sourceDocIDs = []string{"doc-1", "", "ünïcødé", strings.Repeat("x", 512), "with\"quote\\and\nnewline"}

// --- fake Elasticsearch -----------------------------------------------------

type fakeES struct {
	mu     sync.Mutex
	bulks  []string
	hits   []map[string]any
	served bool
}

func (f *fakeES) handler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/" && (r.Method == http.MethodHead || r.Method == http.MethodGet):
		_, _ = io.WriteString(w, `{"version":{"number":"8.19.3"},"tagline":"You Know, for Search"}`)
	case r.URL.Path == "/_bulk":
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.bulks = append(f.bulks, string(body))
		f.mu.Unlock()
		resp, err := bulkSuccess(body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(resp)
	case strings.HasSuffix(r.URL.Path, "/_search"):
		f.mu.Lock()
		hits := []map[string]any{}
		if !f.served {
			hits = f.hits
			f.served = true
		}
		f.mu.Unlock()
		resp, err := json.Marshal(map[string]any{"hits": map[string]any{"hits": hits}})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(resp)
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"type":"not_found","reason":"fake"}}`)
	}
}

// bulkSuccess answers every action of a bulk body with status 200.
func bulkSuccess(body []byte) ([]byte, error) {
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	var items []map[string]any
	for i := 0; i < len(lines); i++ {
		var action map[string]json.RawMessage
		if err := json.Unmarshal([]byte(lines[i]), &action); err != nil {
			continue
		}
		for op := range action {
			items = append(items, map[string]any{op: map[string]any{"_id": "id", "status": 200}})
			if op != "delete" {
				i++ // skip the document line
			}
		}
	}
	return json.Marshal(map[string]any{"took": 1, "errors": false, "items": items})
}

// --- test -------------------------------------------------------------------

func TestAvroEquivalence(t *testing.T) {
	generate := os.Getenv("AVRO_FIXTURE_GEN") == "1"
	codec := avroCodec(t)
	if generate && codec != "github.com/hamba/avro/v2" {
		t.Fatalf("AVRO_FIXTURE_GEN=1 must run on the old codec (hamba/avro), this build uses %q", codec)
	}

	// local-timestamp-* decode into time.Local, so pin it to a fixed zone that
	// is not UTC: the fixtures must not depend on the host, and a codec that
	// confused local and UTC time would show up.
	local := time.Local
	time.Local = time.FixedZone("UTC-7", -7*60*60)
	t.Cleanup(func() { time.Local = local })

	var want avroFixtures
	wantDst := map[string]destinationFixture{}
	if !generate {
		raw, err := os.ReadFile(avroFixturePath)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &want); err != nil {
			t.Fatal(err)
		}
		if want.Codec != "github.com/hamba/avro/v2" {
			t.Fatalf("fixtures must come from hamba/avro, got %q", want.Codec)
		}
		for _, f := range want.Destination {
			wantDst[f.Name] = f
		}
	}

	got := avroFixtures{Codec: codec}
	ctx := context.Background()

	cases := destinationCases()
	for _, tc := range cases {
		var frozen *destinationFixture
		if f, ok := wantDst[tc.name]; ok {
			frozen = &f
		}
		got.Destination = append(got.Destination, runDestinationCase(ctx, t, tc, frozen))
	}
	for i, id := range sourceDocIDs {
		got.Source = append(got.Source, runSourceCase(ctx, t, fmt.Sprintf("source-%d", i), id))
	}

	if generate {
		out, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(avroFixturePath, append(out, '\n'), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s with %d destination and %d source cases", avroFixturePath, len(got.Destination), len(got.Source))
		return
	}

	compareDestination(t, cases, wantDst, got.Destination)
	compareSource(t, want.Source, got.Source)
}

// compareDestination checks every destination result against its frozen
// fixture. wantDst is consumed.
func compareDestination(t *testing.T, cases []destinationCase, wantDst map[string]destinationFixture, got []destinationFixture) {
	t.Helper()
	for _, g := range got {
		w, ok := wantDst[g.Name]
		if !ok {
			t.Errorf("%s: no frozen fixture; add the case on the old codec, never regenerate with the new one", g.Name)
			continue
		}
		delete(wantDst, g.Name)
		compareField(t, g.Name, "schema", w.Schema, g.Schema)
		if unorderedCases[g.Name] {
			compareField(t, g.Name, "decoded fresh encoding", w.Decoded, g.FreshDecoded)
		} else {
			compareField(t, g.Name, "payload Avro bytes", w.PayloadAvro, g.PayloadAvro)
		}
		compareField(t, g.Name, "key Avro bytes", w.KeyAvro, g.KeyAvro)
		compareField(t, g.Name, "encode error", w.EncodeError, g.EncodeError)
		if c, ok := emptyArrayChange[g.Name]; ok {
			// Pin both sides: the frozen hamba/avro output and what the
			// fork produces from the same bytes.
			compareField(t, g.Name, "frozen decoded value", c.from, w.Decoded)
			compareField(t, g.Name, "decoded value", c.to, g.Decoded)
			if !strings.Contains(w.Bulk, c.docFrom) {
				t.Errorf("%s: frozen bulk body does not contain %s:\n%s", g.Name, c.docFrom, w.Bulk)
			}
			w.Decoded = c.to
			w.Bulk = strings.Replace(w.Bulk, c.docFrom, c.docTo, 1)
		}
		compareField(t, g.Name, "decoded value", w.Decoded, g.Decoded)
		compareField(t, g.Name, "decode error", w.DecodeError, g.DecodeError)
		compareField(t, g.Name, "bulk body", w.Bulk, g.Bulk)
		compareField(t, g.Name, "written", fmt.Sprint(w.Written), fmt.Sprint(g.Written))
		compareField(t, g.Name, "write error", w.WriteError, g.WriteError)
	}
	for name := range wantDst {
		t.Errorf("%s: fixture has no case", name)
	}

	caseNames := map[string]bool{}
	for _, tc := range cases {
		caseNames[tc.name] = true
	}
	for name := range emptyArrayChange {
		if !caseNames[name] {
			t.Errorf("%s: listed in emptyArrayChange but not a case", name)
		}
	}
}

// compareSource checks the source results against the frozen fixtures.
func compareSource(t *testing.T, want, got []sourceFixture) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("source cases: want %d, got %d", len(want), len(got))
	}
	for i, g := range got {
		w := want[i]
		compareField(t, g.Name, "doc id", w.DocID, g.DocID)
		compareField(t, g.Name, "key type", w.KeyType, g.KeyType)
		compareField(t, g.Name, "key bytes", w.KeyBytes, g.KeyBytes)
		compareField(t, g.Name, "key schema subject", w.KeySubject, g.KeySubject)
		compareField(t, g.Name, "key schema", w.KeySchema, g.KeySchema)
		compareField(t, g.Name, "key decoded", w.KeyDecoded, g.KeyDecoded)
		compareField(t, g.Name, "payload", w.Payload, g.Payload)
		compareField(t, g.Name, "payload schema", w.PayloadSchema, g.PayloadSchema)
	}
}

func compareField(t *testing.T, name, what, want, got string) {
	t.Helper()
	if want != got {
		t.Errorf("%s: %s differs from the hamba/avro fixture\nwant: %s\ngot:  %s", name, what, want, got)
	}
}

// avroCodec returns the Avro module behind conduit-commons' Avro serde. Test
// binaries carry no module list in their build info, so it is read from the
// type of the serde's schema field.
func avroCodec(t *testing.T) string {
	t.Helper()
	f, ok := reflect.TypeOf(cavro.Serde{}).FieldByName("schema")
	if !ok {
		t.Fatal("conduit-commons avro.Serde has no schema field")
	}
	return f.Type.PkgPath()
}

// runDestinationCase encodes tc.value with the codec in this build and sends a
// record through the destination. With frozen set (compare mode) the record
// carries the frozen hamba/avro bytes instead of this run's encoding, so what
// is checked is that records written before the move decode, and land in
// Elasticsearch, exactly as they did.
func runDestinationCase(ctx context.Context, t *testing.T, tc destinationCase, frozen *destinationFixture) destinationFixture {
	t.Helper()
	out := destinationFixture{Name: tc.name, Schema: tc.schema}

	subject := "equivalence." + tc.name
	sch, err := schema.Create(ctx, schema.TypeAvro, subject+".payload", []byte(tc.schema))
	if err != nil {
		t.Fatalf("%s: create payload schema: %v", tc.name, err)
	}
	ksch, err := schema.Create(ctx, schema.TypeAvro, subject+".key", []byte(keySchema))
	if err != nil {
		t.Fatalf("%s: create key schema: %v", tc.name, err)
	}

	key, err := ksch.Marshal(opencdc.StructuredData{"id": "key-" + tc.name})
	if err != nil {
		t.Fatalf("%s: encode key: %v", tc.name, err)
	}
	out.KeyAvro = base64.StdEncoding.EncodeToString(key)

	payload, err := sch.Marshal(tc.value)
	if err != nil {
		out.EncodeError = err.Error()
	} else {
		out.PayloadAvro = base64.StdEncoding.EncodeToString(payload)
		var fresh opencdc.StructuredData
		if err := sch.Unmarshal(payload, &fresh); err == nil {
			out.FreshDecoded = goString(fresh)
		}
	}

	if frozen != nil {
		if frozen.EncodeError != "" {
			return out // the old codec could not encode it either; compared above
		}
		if payload, err = base64.StdEncoding.DecodeString(frozen.PayloadAvro); err != nil {
			t.Fatalf("%s: fixture payload: %v", tc.name, err)
		}
		if key, err = base64.StdEncoding.DecodeString(frozen.KeyAvro); err != nil {
			t.Fatalf("%s: fixture key: %v", tc.name, err)
		}
	} else if out.EncodeError != "" {
		return out
	}

	var decoded opencdc.StructuredData
	if err := sch.Unmarshal(payload, &decoded); err != nil {
		out.DecodeError = err.Error()
	} else {
		out.Decoded = goString(decoded)
	}

	fake := &fakeES{}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	dest := openDestination(ctx, t, map[string]string{
		"version": "8",
		"host":    srv.URL,
		"index":   "idx",
	})
	defer func() {
		if err := dest.Teardown(ctx); err != nil {
			t.Errorf("%s: teardown: %v", tc.name, err)
		}
	}()

	r := opencdc.Record{
		Position:  opencdc.Position("p"),
		Operation: opencdc.OperationCreate,
		Metadata:  opencdc.Metadata{},
		Key:       opencdc.RawData(key),
		Payload:   opencdc.Change{After: opencdc.RawData(payload)},
	}
	r.Metadata.SetKeySchemaSubject(ksch.Subject)
	r.Metadata.SetKeySchemaVersion(ksch.Version)
	r.Metadata.SetPayloadSchemaSubject(sch.Subject)
	r.Metadata.SetPayloadSchemaVersion(sch.Version)

	n, err := dest.Write(ctx, []opencdc.Record{r})
	out.Written = n
	if err != nil {
		out.WriteError = err.Error()
	}
	fake.mu.Lock()
	out.Bulk = strings.Join(fake.bulks, "\n---\n")
	fake.mu.Unlock()
	return out
}

func runSourceCase(ctx context.Context, t *testing.T, name, docID string) sourceFixture {
	t.Helper()
	out := sourceFixture{Name: name, DocID: docID}

	fake := &fakeES{hits: []map[string]any{{
		"_index":  "idx",
		"_id":     docID,
		"_source": map[string]any{"title": "t", "n": 1},
		"sort":    []int64{1},
	}}}
	srv := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer srv.Close()

	// The source workers run until the context passed to Open is canceled.
	openCtx, openCancel := context.WithCancel(ctx)
	defer openCancel()
	src := openSource(openCtx, t, map[string]string{
		"version":            "8",
		"host":               srv.URL,
		"indexes.idx.sortBy": "_seq_no",
		"pollingPeriod":      "10ms",
		"batchSize":          "10",
	})

	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	r := readLikePlugin(readCtx, t, name, src)
	cancel()
	openCancel()
	if err := src.Teardown(ctx); err != nil {
		t.Errorf("%s: teardown: %v", name, err)
	}

	out.Payload = string(r.Payload.After.Bytes())
	if subject, err := r.Metadata.GetPayloadSchemaSubject(); err == nil {
		version, _ := r.Metadata.GetPayloadSchemaVersion()
		out.PayloadSchema = fmt.Sprintf("%s:%d", subject, version)
	}

	out.KeyType = fmt.Sprintf("%T", r.Key)
	out.KeyBytes = base64.StdEncoding.EncodeToString(r.Key.Bytes())
	subject, errSubject := r.Metadata.GetKeySchemaSubject()
	version, errVersion := r.Metadata.GetKeySchemaVersion()
	if errSubject != nil || errVersion != nil {
		// No key schema attached: the key is handed over as is.
		return out
	}
	out.KeySubject = fmt.Sprintf("%s:%d", subject, version)
	ksch, err := schema.Get(ctx, subject, version)
	if err != nil {
		t.Fatalf("%s: get key schema: %v", name, err)
	}
	out.KeySchema = string(ksch.Bytes)
	var decoded opencdc.StructuredData
	if err := ksch.Unmarshal(r.Key.Bytes(), &decoded); err != nil {
		t.Fatalf("%s: decode key: %v", name, err)
	}
	out.KeyDecoded = goString(decoded)
	return out
}

// readLikePlugin reads one record the way the SDK's source plugin does: ReadN
// first, falling back to Read when the connector does not implement ReadN.
// Which middleware runs depends on that path, so the test must not shortcut it.
func readLikePlugin(ctx context.Context, t *testing.T, name string, src sdk.Source) opencdc.Record {
	t.Helper()
	for {
		recs, err := src.ReadN(ctx, 1)
		if errors.Is(err, sdk.ErrUnimplemented) {
			r, err := src.Read(ctx)
			if err != nil {
				t.Fatalf("%s: read: %v", name, err)
			}
			return r
		}
		if err != nil {
			t.Fatalf("%s: readN: %v", name, err)
		}
		if len(recs) > 0 {
			return recs[0]
		}
	}
}

// goString prints v with %#v, with map keys in a stable order.
func goString(v any) string {
	switch v := v.(type) {
	case opencdc.StructuredData:
		return goString(map[string]any(v))
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var b bytes.Buffer
		b.WriteString("map{")
		for i, k := range keys {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%q: %s", k, goString(v[k]))
		}
		b.WriteString("}")
		return b.String()
	case []any:
		if v == nil {
			return "[]any(nil)" // nil and empty land differently in JSON
		}
		parts := make([]string, len(v))
		for i := range v {
			parts[i] = goString(v[i])
		}
		return "[]any{" + strings.Join(parts, ", ") + "}"
	case *big.Rat:
		return fmt.Sprintf("*big.Rat(%s)", v.RatString())
	default:
		return fmt.Sprintf("%#v", v)
	}
}

func connector() sdk.Connector { return elasticsearch.Connector }

// --- SDK adapter --------------------------------------------------------------
// These two functions are the only code that differs from the generator run on
// the old SDK (v0.12.0), where they called Configure(ctx, cfg). Here the SDK
// parses the config into Config() with the parameters from connector.yaml,
// which is what the plugin adapter does before Open.

func openDestination(ctx context.Context, t *testing.T, cfg map[string]string) sdk.Destination {
	t.Helper()
	dest := connector().NewDestination()
	if err := sdk.Util.ParseConfig(ctx, cfg, dest.Config(), connector().NewSpecification().DestinationParams); err != nil {
		t.Fatalf("configure destination: %v", err)
	}
	if err := dest.Open(ctx); err != nil {
		t.Fatalf("open destination: %v", err)
	}
	return dest
}

func openSource(ctx context.Context, t *testing.T, cfg map[string]string) sdk.Source {
	t.Helper()
	src := connector().NewSource()
	if err := sdk.Util.ParseConfig(ctx, cfg, src.Config(), connector().NewSpecification().SourceParams); err != nil {
		t.Fatalf("configure source: %v", err)
	}
	if err := src.Open(ctx, nil); err != nil {
		t.Fatalf("open source: %v", err)
	}
	return src
}
