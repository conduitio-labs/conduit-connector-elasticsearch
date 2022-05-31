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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	esV5 "github.com/elastic/go-elasticsearch/v5"
	"github.com/jaswdr/faker"
	"github.com/miquido/conduit-connector-elasticsearch/destination"
	"github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch"
	v5 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v5"
	"github.com/stretchr/testify/require"
)

func TestOperationsWithSmallestBulkSize(t *testing.T) {
	fakerInstance := faker.New()
	dest := destination.NewDestination().(*destination.Destination)

	cfgRaw := map[string]string{
		destination.ConfigKeyVersion:       elasticsearch.Version5,
		destination.ConfigKeyConnectionURI: "http://127.0.0.1:9200/users/user",
		destination.ConfigKeyBulkSize:      "1",
	}

	require.NoError(t, dest.Configure(context.Background(), cfgRaw))
	require.NoError(t, dest.Open(context.Background()))

	esClient := dest.GetClient().(*v5.Client).GetClient()

	require.True(t, assertIndexIsDeleted(esClient, "users"))

	t.Cleanup(func() {
		assertIndexIsDeleted(esClient, "users")

		require.NoError(t, dest.Teardown(context.Background()))
	})

	var (
		user1 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(100, 200)),
			"email": fakerInstance.Internet().Email(),
		}
		user2 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(201, 300)),
			"email": fakerInstance.Internet().Email(),
		}
	)

	t.Run("records can be upserted", func(t *testing.T) {
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "updated",
			},
			Payload:   sdk.StructuredData(user1),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user1["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "created",
			},
			Payload:   sdk.StructuredData(user2),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user2["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))

		// Give Elasticsearch enough time to persist operations
		time.Sleep(time.Second)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user2,
		}))
	})

	t.Run("record can be deleted", func(t *testing.T) {
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "deleted",
			},
			Payload:   nil,
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user1["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))

		// Give Elasticsearch enough time to persist operations
		time.Sleep(time.Second)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user2,
		}))
	})
}

func TestOperationsWithBiggerBulkSize(t *testing.T) {
	fakerInstance := faker.New()
	dest := destination.NewDestination().(*destination.Destination)

	cfgRaw := map[string]string{
		destination.ConfigKeyVersion:       elasticsearch.Version5,
		destination.ConfigKeyConnectionURI: "http://127.0.0.1:9200/users/user",
		destination.ConfigKeyBulkSize:      "3",
	}

	require.NoError(t, dest.Configure(context.Background(), cfgRaw))
	require.NoError(t, dest.Open(context.Background()))

	esClient := dest.GetClient().(*v5.Client).GetClient()

	require.True(t, assertIndexIsDeleted(esClient, "users"))

	t.Cleanup(func() {
		assertIndexIsDeleted(esClient, "users")

		require.NoError(t, dest.Teardown(context.Background()))
	})

	var (
		user1 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(100, 199)),
			"email": fakerInstance.Internet().Email(),
		}
		user2 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(200, 299)),
			"email": fakerInstance.Internet().Email(),
		}
		user3 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(300, 399)),
			"email": fakerInstance.Internet().Email(),
		}
		user4 = map[string]interface{}{
			"id":    user2["id"],
			"email": fakerInstance.Internet().Email(),
		}
		user5 = map[string]interface{}{
			"id":    float64(fakerInstance.Int32Between(500, 599)),
			"email": fakerInstance.Internet().Email(),
		}
	)

	t.Run("writing first 3 records does persists them", func(t *testing.T) {
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "updated",
			},
			Payload:   sdk.StructuredData(user1),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user1["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "created",
			},
			Payload:   sdk.StructuredData(user2),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user2["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "created",
			},
			Payload:   sdk.StructuredData(user3),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user3["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))

		// Give Elasticsearch enough time to persist operations
		time.Sleep(time.Second)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user2,
			user3,
		}))
	})

	t.Run("writing next 2 records does not persist them", func(t *testing.T) {
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "updated",
			},
			Payload:   sdk.StructuredData(user4),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user4["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "created",
			},
			Payload:   sdk.StructuredData(user5),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user5["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))

		// Give Elasticsearch enough time to persist operations
		time.Sleep(time.Second)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user2,
			user3,
		}))
	})

	t.Run("writing 1 more record fills the buffer and performs actions", func(t *testing.T) {
		require.NoError(t, dest.WriteAsync(context.Background(), sdk.Record{
			Metadata: map[string]string{
				"action": "deleted",
			},
			Payload:   sdk.StructuredData(user3),
			Key:       sdk.RawData(fmt.Sprintf("%.0f", user3["id"])),
			CreatedAt: time.Now(),
		}, ackFunc(t)))

		// Give Elasticsearch enough time to persist operations
		time.Sleep(time.Second)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user4, // Overrides user2
			// user3, // Deleted
			user5,
		}))
	})
}

func ackFunc(t *testing.T) sdk.AckFunc {
	return func(err error) error {
		require.NoError(t, err)

		return nil
	}
}

func assertIndexIsDeleted(esClient *esV5.Client, index string) bool {
	existsResponse, err := esClient.Indices.Exists([]string{index})
	if err != nil {
		log.Fatalf("Cannot check if index %q exists: %s", index, err)

		return false
	}

	if existsResponse.StatusCode == http.StatusNotFound {
		return true
	}

	res, err := esClient.Indices.Delete([]string{index})
	if err != nil || res.IsError() {
		log.Fatalf("Cannot delete index %q: %s", index, err)

		return false
	}

	return true
}

func assertIndexContainsDocuments(t *testing.T, esClient *esV5.Client, documents []map[string]interface{}) error {
	// Build the request body.
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"sort": []map[string]interface{}{
			{
				"id": map[string]string{
					"order": "asc",
				},
			},
		},
	}

	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		return fmt.Errorf("error encoding query: %s", err)
	}

	// Search
	response, err := esClient.Search(
		esClient.Search.WithIndex("users"),
		esClient.Search.WithDocumentType("user"),
		esClient.Search.WithBody(&buf),
	)
	if err != nil {
		return fmt.Errorf("error getting response: %s", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		var e map[string]interface{}

		if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
			return fmt.Errorf("error parsing the response body: %s", err)
		}

		// Print the response status and error information.
		return fmt.Errorf("[%s] %s: %s",
			response.Status(),
			e["error"].(map[string]interface{})["type"],
			e["error"].(map[string]interface{})["reason"],
		)
	}

	var r map[string]interface{}

	if err := json.NewDecoder(response.Body).Decode(&r); err != nil {
		return fmt.Errorf("error parsing the response body: %s", err)
	}

	hitsMetadata := r["hits"].(map[string]interface{})

	require.Equal(t, len(documents), int(hitsMetadata["total"].(float64)))

	hits := hitsMetadata["hits"].([]interface{})

	for i, document := range documents {
		hit := hits[i].(map[string]interface{})
		source := hit["_source"].(map[string]interface{})

		require.EqualValues(t, document, source)
	}

	return nil
}
