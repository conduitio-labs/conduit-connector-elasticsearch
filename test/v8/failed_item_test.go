// Copyright © 2022 Meroxa, Inc. and Miquido
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
	"net/http"
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	v8 "github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/v8"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/stretchr/testify/require"
)

// TestWrite_FailedItemIsNotReportedWritten is the end-to-end regression test
// for #153. Elasticsearch itself rejects the middle record (mapping conflict),
// and Write must report only the records before it as written, so the SDK
// nacks the rejected record instead of acking it.
func TestWrite_FailedItemIsNotReportedWritten(t *testing.T) {
	ctx := context.Background()
	const index = "failed_item"

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
	esClient := client.(*v8.Client).GetClient()

	assertIndexIsDeleted(esClient, index)
	t.Cleanup(func() { assertIndexIsDeleted(esClient, index) })

	// Field "x" is an integer; a non-numeric string is rejected per item.
	res, err := esClient.Indices.Create(
		index,
		esClient.Indices.Create.WithBody(strings.NewReader(`{"mappings":{"properties":{"x":{"type":"integer"}}}}`)),
	)
	require.NoError(t, err)
	require.False(t, res.IsError(), res.String())

	record := func(key string, x interface{}) opencdc.Record {
		return sdk.SourceUtil{}.NewRecordUpdate(
			opencdc.Position(key), nil, opencdc.RawData(key), nil, opencdc.StructuredData{"x": x},
		)
	}
	exists := func(key string) bool {
		r, err := esClient.Exists(index, key)
		require.NoError(t, err)

		return r.StatusCode == http.StatusOK
	}
	count := func() int {
		r, err := esClient.Indices.Refresh(esClient.Indices.Refresh.WithIndex(index))
		require.NoError(t, err)
		require.False(t, r.IsError())
		c, err := esClient.Count(esClient.Count.WithIndex(index))
		require.NoError(t, err)
		require.False(t, c.IsError(), c.String())

		var body struct {
			Count int `json:"count"`
		}
		require.NoError(t, json.NewDecoder(c.Body).Decode(&body))

		return body.Count
	}

	batch := []opencdc.Record{record("k0", 1), record("k1", "not-a-number"), record("k2", 3)}

	n, err := dest.Write(ctx, batch)
	require.Error(t, err)
	require.Equal(t, 1, n, "only the record before the rejected one is confirmed")
	require.Contains(t, err.Error(), "key=k1 ")

	require.True(t, exists("k0"))
	require.False(t, exists("k1"), "the rejected record must not be reported as stored")
	// Bulk items are independent, so k2 was stored although it is nacked
	// and will be sent again.
	require.True(t, exists("k2"))

	// The SDK nacks batch[n:], so Conduit re-sends batch[n:] (here: after the
	// operator fixed the record). Keyed records are upserts, so re-sending k2
	// must not duplicate it.
	n, err = dest.Write(ctx, []opencdc.Record{record("k1", 2), record("k2", 3)})
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 3, count(), "re-sent keyed records are overwritten, not duplicated")
}
