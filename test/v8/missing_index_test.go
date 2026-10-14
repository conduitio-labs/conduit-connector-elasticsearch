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

// TestWrite_MissingIndexIsNotReportedWritten is the end-to-end regression test
// for #154. With the index missing and auto-creation disabled, Elasticsearch
// answers every non-delete item with 404. Those records were not stored and
// must not be reported as written; a delete on the missing index is fine.
func TestWrite_MissingIndexIsNotReportedWritten(t *testing.T) {
	ctx := context.Background()
	const index = "probe154_missing"

	cfgRaw := map[string]string{
		destination.ConfigVersion: elasticsearch.Version8,
		destination.ConfigHost:    "http://127.0.0.1:9200",
		destination.ConfigIndex:   index,
		destination.ConfigType:    "doc",
	}

	dest := destination.NewDestination()
	require.NoError(t, dest.Configure(ctx, cfgRaw))
	require.NoError(t, dest.Open(ctx))
	t.Cleanup(func() { require.NoError(t, dest.Teardown(ctx)) })

	client, err := test.GetClient(cfgRaw)
	require.NoError(t, err)
	esClient := client.(*v8.Client).GetClient()

	assertIndexIsDeleted(esClient, index)

	setAutoCreate := func(value string) {
		res, err := esClient.Cluster.PutSettings(strings.NewReader(
			`{"transient":{"action.auto_create_index":` + value + `}}`,
		))
		require.NoError(t, err)
		require.False(t, res.IsError(), res.String())
	}
	setAutoCreate(`"-probe154*,+*"`)
	t.Cleanup(func() { setAutoCreate(`null`) })

	keyed := sdk.SourceUtil{}.NewRecordUpdate(
		opencdc.Position("p1"), nil, opencdc.RawData("k1"), nil, opencdc.StructuredData{"x": 1},
	)
	keyless := sdk.SourceUtil{}.NewRecordCreate(
		opencdc.Position("p2"), nil, nil, opencdc.StructuredData{"x": 2},
	)
	deleted := sdk.SourceUtil{}.NewRecordDelete(
		opencdc.Position("p3"), nil, opencdc.RawData("k3"), nil,
	)

	t.Run("keyed update", func(t *testing.T) {
		n, err := dest.Write(ctx, []opencdc.Record{keyed})
		require.Error(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("keyless create", func(t *testing.T) {
		n, err := dest.Write(ctx, []opencdc.Record{keyless})
		require.Error(t, err)
		require.Equal(t, 0, n)
	})
	t.Run("delete stays a success", func(t *testing.T) {
		n, err := dest.Write(ctx, []opencdc.Record{deleted})
		require.NoError(t, err)
		require.Equal(t, 1, n)
	})
	t.Run("middle of a batch", func(t *testing.T) {
		n, err := dest.Write(ctx, []opencdc.Record{deleted, keyed, deleted})
		require.Error(t, err)
		require.Equal(t, 1, n)
	})
}
