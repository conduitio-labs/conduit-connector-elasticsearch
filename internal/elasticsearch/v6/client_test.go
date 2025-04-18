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

package v6

import (
	"encoding/json"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/elastic/go-elasticsearch/v6"
	"github.com/stretchr/testify/require"
)

const (
	indexName = "someIndexName"
	indexType = "someIndexType"
)

func TestNewClient(t *testing.T) {
	t.Run("Fails when provided config object is invalid", func(t *testing.T) {
		client, err := NewClient("invalid config object")

		require.Nil(t, client)
		require.EqualError(t, err, "provided config object is invalid")
	})
}

func TestClient_GetClient(t *testing.T) {
	esClient := &elasticsearch.Client{}

	client := Client{
		es: esClient,
	}

	require.Same(t, esClient, client.GetClient())
}

func TestClient_PrepareCreateOperation(t *testing.T) {
	t.Run("Fails when payload could not be prepared", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetTypeFunc: func() string {
					return indexType
				},
			},
		}

		metadata, payload, err := client.PrepareCreateOperation(sdk.SourceUtil{}.NewRecordCreate(
			nil,
			nil,
			nil,
			opencdc.StructuredData{
				"foo": complex64(1 + 2i),
			},
		), indexName)

		require.Nil(t, metadata)
		require.Nil(t, payload)
		require.EqualError(t, err, "json: unsupported type: complex64")
	})

	t.Run("Successfully prepares create operation", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetTypeFunc: func() string {
					return indexType
				},
			},
		}

		metadata, payload, err := client.PrepareCreateOperation(sdk.SourceUtil{}.NewRecordCreate(
			nil,
			nil,
			nil,
			opencdc.StructuredData{
				"foo": "bar",
			},
		), indexName)

		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.NotNil(t, payload)

		expectedMetadata := bulkRequestActionAndMetadata{
			Index: &bulkRequestIndexAction{
				Index: indexName,
				Type:  indexType,
			},
		}

		expectedPayload := bulkRequestCreateSource([]byte(`{"foo":"bar"}`))

		require.Equal(t, expectedMetadata, metadata)
		require.Equal(t, expectedPayload, payload)
	})
}

func TestClient_PrepareUpsertOperation(t *testing.T) {
	t.Run("Fails when payload could not be prepared", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetTypeFunc: func() string {
					return indexType
				},
			},
		}

		metadata, payload, err := client.PrepareUpsertOperation(
			"key",
			sdk.SourceUtil{}.NewRecordUpdate(
				nil,
				nil,
				nil,
				opencdc.StructuredData{
					"foo": complex64(12 + 2i),
				},
				opencdc.StructuredData{
					"foo": complex64(1 + 2i),
				},
			),
			indexName,
		)

		require.Nil(t, metadata)
		require.Nil(t, payload)
		require.EqualError(t, err, "json: unsupported type: complex64")
	})

	t.Run("Successfully prepares upsert operation", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetTypeFunc: func() string {
					return indexType
				},
			},
		}

		metadata, payload, err := client.PrepareUpsertOperation(
			"key",
			sdk.SourceUtil{}.NewRecordUpdate(
				nil,
				nil,
				nil,
				opencdc.StructuredData{
					"foo": "bar",
				},
				opencdc.StructuredData{
					"foo": "baz",
				},
			),
			indexName,
		)

		require.NoError(t, err)
		require.NotNil(t, metadata)
		require.NotNil(t, payload)

		expectedMetadata := bulkRequestActionAndMetadata{
			Update: &bulkRequestUpdateAction{
				ID:              "key",
				Index:           indexName,
				Type:            indexType,
				RetryOnConflict: 3,
			},
		}

		expectedPayload := bulkRequestOptionalSource{
			Doc:         json.RawMessage(`{"foo":"baz"}`),
			DocAsUpsert: true,
		}

		require.Equal(t, expectedMetadata, metadata)
		require.Equal(t, expectedPayload, payload)
	})
}

func TestClient_PrepareDeleteOperation(t *testing.T) {
	t.Run("Successfully prepares delete operation", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetTypeFunc: func() string {
					return indexType
				},
			},
		}

		metadata, err := client.PrepareDeleteOperation(
			"key",
			indexName,
		)

		require.NoError(t, err)
		require.NotNil(t, metadata)

		expectedMetadata := bulkRequestActionAndMetadata{
			Delete: &bulkRequestDeleteAction{
				ID:    "key",
				Index: indexName,
				Type:  indexType,
			},
		}

		require.Equal(t, expectedMetadata, metadata)
	})
}
