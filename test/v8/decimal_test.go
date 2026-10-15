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

package v8

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	v8 "github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/v8"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/test"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	esV8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/iskorotkov/avro/v2"
	"github.com/stretchr/testify/require"
)

const decimalIndex = "decimal_171"

// TestWrite_AvroDecimalsAreExactText is the end-to-end regression test for
// #171. An Avro-encoded record goes through the destination with the SDK
// middleware into a real Elasticsearch. Every decimal must be stored in
// _source as exact text at the schema's scale; before the fix it was the
// ratio of a *big.Rat ("-15432/125"), which the scaled_float field rejects.
//
// It also records how the mappings a user would choose treat that text:
// keyword matches it exactly, scaled_float and double coerce it to a number,
// and dynamic mapping makes it text with a keyword subfield.
func TestWrite_AvroDecimalsAreExactText(t *testing.T) {
	ctx := context.Background()
	const index = decimalIndex

	cfgRaw := map[string]string{
		test.ParamVersion: elasticsearch.Version8,
		test.ParamHost:    test.Host(),
		test.ParamIndex:   index,
		test.ParamType:    "doc",
	}
	dest := destination.NewDestination()
	require.NoError(t, test.ConfigureDestination(ctx, dest, cfgRaw))
	require.NoError(t, dest.Open(ctx))
	t.Cleanup(func() { require.NoError(t, dest.Teardown(ctx)) })

	client, err := test.GetClient(cfgRaw)
	require.NoError(t, err)
	es := client.(*v8.Client).GetClient()

	assertIndexIsDeleted(es, index)
	t.Cleanup(func() { assertIndexIsDeleted(es, index) })
	res, err := es.Indices.Create(index, es.Indices.Create.WithBody(strings.NewReader(`{"mappings":{"properties":{
		"exact":  {"type":"keyword"},
		"amount": {"type":"scaled_float","scaling_factor":100},
		"ratio":  {"type":"double"}
	}}}`)))
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())
	require.NoError(t, res.Body.Close())

	const dec2 = `{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}`
	text := `{"type":"record","name":"payload","fields":[` +
		`{"name":"exact","type":` + dec2 + `},` +
		`{"name":"amount","type":` + dec2 + `},` +
		`{"name":"ratio","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":3}},` +
		`{"name":"dynamic","type":["null",` + dec2 + `]},` +
		`{"name":"items","type":{"type":"array","items":{"type":"record","name":"item","fields":[{"name":"price","type":` + dec2 + `}]}}}` +
		`]}`
	sch, err := schema.Create(ctx, schema.TypeAvro, "decimal_171.payload", []byte(text))
	require.NoError(t, err)
	parsed, err := avro.ParseBytesWithCache([]byte(text), "", &avro.SchemaCache{})
	require.NoError(t, err)
	payload, err := avro.Config{}.Freeze().Marshal(parsed, map[string]any{
		"exact":   big.NewRat(3, 2),
		"amount":  big.NewRat(123456, 100),
		"ratio":   big.NewRat(-15432, 125),
		"dynamic": map[string]any{"bytes.decimal": big.NewRat(100, 1)},
		"items":   []any{map[string]any{"price": big.NewRat(999, 100)}},
	})
	require.NoError(t, err)

	r := opencdc.Record{
		Position:  opencdc.Position("p1"),
		Operation: opencdc.OperationUpdate,
		Metadata:  opencdc.Metadata{},
		Key:       opencdc.RawData("doc-1"),
		Payload:   opencdc.Change{After: opencdc.RawData(payload)},
	}
	r.Metadata.SetPayloadSchemaSubject(sch.Subject)
	r.Metadata.SetPayloadSchemaVersion(sch.Version)

	n, err := dest.Write(ctx, []opencdc.Record{r})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	res, err = es.Indices.Refresh(es.Indices.Refresh.WithIndex(index))
	require.NoError(t, err)
	require.NoError(t, res.Body.Close())

	var doc struct {
		Source map[string]any `json:"_source"`
	}
	decode(t, es.Get, index, "doc-1", &doc)
	require.Equal(t, map[string]any{
		"exact":   "1.50",
		"amount":  "1234.56",
		"ratio":   "-123.456",
		"dynamic": "100.00",
		"items":   []any{map[string]any{"price": "9.99"}},
	}, doc.Source)

	// keyword: an exact term match on the scale-padded text.
	require.Equal(t, 1, hits(t, es, `{"query":{"term":{"exact":"1.50"}}}`))
	require.Equal(t, 0, hits(t, es, `{"query":{"term":{"exact":"1.5"}}}`))
	// scaled_float and double: the text is coerced to a number.
	require.Equal(t, 1, hits(t, es, `{"query":{"range":{"amount":{"gte":1234.56,"lte":1234.56}}}}`))
	require.Equal(t, 1, hits(t, es, `{"query":{"range":{"ratio":{"lt":-123.4}}}}`))
	// dynamic mapping: text plus a keyword subfield, never a float.
	require.Equal(t, 1, hits(t, es, `{"query":{"term":{"dynamic.keyword":"100.00"}}}`))
	res, err = es.Indices.GetMapping(es.Indices.GetMapping.WithIndex(index))
	require.NoError(t, err)
	defer res.Body.Close()
	var mapping map[string]struct {
		Mappings struct {
			Properties map[string]struct {
				Type string `json:"type"`
			} `json:"properties"`
		} `json:"mappings"`
	}
	require.NoError(t, json.NewDecoder(res.Body).Decode(&mapping))
	require.Equal(t, "text", mapping[index].Mappings.Properties["dynamic"].Type)
}

func decode(t *testing.T, get esapi.Get, index, id string, v any) {
	t.Helper()
	res, err := get(index, id)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.False(t, res.IsError(), string(body))
	require.NoError(t, json.Unmarshal(body, v))
}

func hits(t *testing.T, es *esV8.Client, query string) int {
	t.Helper()
	res, err := es.Search(es.Search.WithIndex(decimalIndex), es.Search.WithBody(strings.NewReader(query)))
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.False(t, res.IsError(), string(body))
	var out struct {
		Hits struct {
			Hits []json.RawMessage `json:"hits"`
		} `json:"hits"`
	}
	require.NoError(t, json.Unmarshal(body, &out))
	return len(out.Hits.Hits)
}
