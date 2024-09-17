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

package v5

import (
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/elastic/go-elasticsearch/v5"
	"github.com/stretchr/testify/require"
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
				GetIndexFunc: func() string {
					return "someIndexName"
				},
				GetTypeFunc: func() string {
					return "someIndexType"
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
		))

		require.Nil(t, metadata)
		require.Nil(t, payload)
		require.EqualError(t, err, "json: unsupported type: complex64")
	})
}

func TestClient_PrepareUpsertOperation(t *testing.T) {
	t.Run("Fails when payload could not be prepared", func(t *testing.T) {
		client := Client{
			cfg: &configMock{
				GetIndexFunc: func() string {
					return "someIndexName"
				},
				GetTypeFunc: func() string {
					return "someIndexType"
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
		)

		require.Nil(t, metadata)
		require.Nil(t, payload)
		require.EqualError(t, err, "json: unsupported type: complex64")
	})
}
