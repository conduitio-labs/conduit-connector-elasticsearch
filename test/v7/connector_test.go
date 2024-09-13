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

package v7

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	v7 "github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/v7"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	esV7 "github.com/elastic/go-elasticsearch/v7"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestOperationsWithSmallestBulkSize(t *testing.T) {
	fakerInstance := faker.New()
	dest := destination.NewDestination().(*destination.Destination)

	cfgRaw := map[string]string{
		destination.ConfigKeyVersion:  elasticsearch.Version7,
		destination.ConfigKeyHost:     "http://127.0.0.1:9200",
		destination.ConfigKeyIndex:    "users",
		destination.ConfigKeyType:     "user",
		destination.ConfigKeyBulkSize: "1",
	}

	require.NoError(t, dest.Configure(context.Background(), cfgRaw))
	require.NoError(t, dest.Open(context.Background()))

	esClient := dest.GetClient().(*v7.Client).GetClient()

	require.True(t, assertIndexIsDeleted(esClient, "users"))

	t.Cleanup(func() {
		require.NoError(t, dest.Teardown(context.Background()))
	})

	t.Run("StructuredData record", func(t *testing.T) {
		t.Cleanup(func() {
			assertIndexIsDeleted(esClient, "users")
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

		t.Run("can be upserted", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user1["id"])),
						nil,
						opencdc.StructuredData(user1),
					),
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user2["id"])),
						nil,
						opencdc.StructuredData(user2),
					),
				})
			require.NoError(t, err)
			require.Equal(t, 2, n)

			require.NoError(
				t,
				assertIndexContainsDocuments(t, esClient, []map[string]interface{}{user1, user2}),
			)
		})

		t.Run("can be deleted", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordDelete(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user1["id"])),
						opencdc.StructuredData(user1),
					),
				})
			require.NoError(t, err)
			require.Equal(t, 1, n)

			require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
				user2,
			}))
		})

		t.Run("can be created", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordCreate(
						nil,
						nil,
						nil,
						opencdc.StructuredData(user1),
					),
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						nil,
						nil,
						opencdc.StructuredData(user2),
					),
				},
			)
			require.NoError(t, err)
			require.Equal(t, 2, n)

			require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
				user1,
				user2,
				user2,
			}))
		})
	})

	t.Run("RawData record", func(t *testing.T) {
		t.Cleanup(func() {
			assertIndexIsDeleted(esClient, "users")
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

		t.Run("can be upserted", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user1["id"])),
						nil,
						opencdc.RawData(fmt.Sprintf(
							`{"id":%.f,"email":%q}`,
							user1["id"],
							user1["email"],
						)),
					),
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user2["id"])),
						nil,
						opencdc.RawData(fmt.Sprintf(
							`{"id":%.f,"email":%q}`,
							user2["id"],
							user2["email"],
						)),
					),
				},
			)

			require.NoError(t, err)
			require.Equal(t, 2, n)
			require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
				user1,
				user2,
			}))
		})

		t.Run("can be deleted", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordDelete(
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf("%.0f", user1["id"])),
						opencdc.StructuredData(user1),
					),
				},
			)

			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
				user2,
			}))
		})

		t.Run("can be created", func(t *testing.T) {
			n, err := dest.Write(
				context.Background(),
				[]opencdc.Record{
					sdk.SourceUtil{}.NewRecordCreate(
						nil,
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf(
							`{"id":%.f,"email":%q}`,
							user1["id"],
							user1["email"],
						)),
					),
					sdk.SourceUtil{}.NewRecordUpdate(
						nil,
						nil,
						nil,
						nil,
						opencdc.RawData(fmt.Sprintf(
							`{"id":%.f,"email":%q}`,
							user2["id"],
							user2["email"],
						)),
					),
				},
			)
			require.Equal(t, 2, n)
			require.NoError(t, err)

			require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
				user1,
				user2,
				user2,
			}))
		})
	})
}

func TestOperationsWithBiggerBulkSize(t *testing.T) {
	fakerInstance := faker.New()
	dest := destination.NewDestination().(*destination.Destination)

	cfgRaw := map[string]string{
		destination.ConfigKeyVersion:  elasticsearch.Version7,
		destination.ConfigKeyHost:     "http://127.0.0.1:9200",
		destination.ConfigKeyIndex:    "users",
		destination.ConfigKeyType:     "user",
		destination.ConfigKeyBulkSize: "3",
	}

	require.NoError(t, dest.Configure(context.Background(), cfgRaw))
	require.NoError(t, dest.Open(context.Background()))

	esClient := dest.GetClient().(*v7.Client).GetClient()

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

	t.Run("one create operation and two upsert operations", func(t *testing.T) {
		n, err := dest.Write(
			context.Background(),
			[]opencdc.Record{
				sdk.SourceUtil{}.NewRecordCreate(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user1["id"])),
					opencdc.StructuredData(user1),
				),
				sdk.SourceUtil{}.NewRecordUpdate(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user2["id"])),
					nil,
					opencdc.StructuredData(user2),
				),
				sdk.SourceUtil{}.NewRecordUpdate(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user3["id"])),
					nil,
					opencdc.StructuredData(user3),
				),
			},
		)

		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user2,
			user3,
		}))
	})

	t.Run("create new, update existing", func(t *testing.T) {
		n, err := dest.Write(
			context.Background(),
			[]opencdc.Record{
				sdk.SourceUtil{}.NewRecordUpdate(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user4["id"])),
					nil,
					opencdc.StructuredData(user4),
				),
				sdk.SourceUtil{}.NewRecordCreate(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user5["id"])),
					opencdc.StructuredData(user5),
				),
			},
		)

		require.NoError(t, err)
		require.Equal(t, 2, n)

		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user4, // Overrides user2
			user3,
			user5,
		}))
	})

	t.Run("writing 1 more record fills the buffer and performs actions", func(t *testing.T) {
		n, err := dest.Write(
			context.Background(),
			[]opencdc.Record{
				sdk.SourceUtil{}.NewRecordDelete(
					nil,
					nil,
					opencdc.RawData(fmt.Sprintf("%.0f", user3["id"])),
					opencdc.StructuredData(user3),
				),
			},
		)

		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.NoError(t, assertIndexContainsDocuments(t, esClient, []map[string]interface{}{
			user1,
			user4, // Overrides user2
			// user3, // Deleted
			user5,
		}))
	})
}

func assertIndexIsDeleted(esClient *esV7.Client, index string) bool {
	res, err := esClient.Indices.Delete([]string{index}, esClient.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil || res.IsError() {
		log.Fatalf("Cannot delete index %q: %s", index, err)

		return false
	}

	return true
}

func assertIndexContainsDocuments(t *testing.T, esClient *esV7.Client, documents []map[string]interface{}) error {
	refresh, err := esClient.Indices.Refresh(
		esClient.Indices.Refresh.WithIndex("users"),
	)
	if err != nil {
		return fmt.Errorf("error refreshing index: %w", err)
	}
	if refresh.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"error refreshing index, status code %v, status %v",
			refresh.StatusCode,
			refresh.Status(),
		)
	}
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
		return fmt.Errorf("error encoding query: %w", err)
	}

	// Search
	response, err := esClient.Search(
		esClient.Search.WithIndex("users"),
		esClient.Search.WithBody(&buf),
	)
	if err != nil {
		return fmt.Errorf("error getting response: %w", err)
	}

	defer response.Body.Close()

	if response.IsError() {
		var e map[string]interface{}

		if err := json.NewDecoder(response.Body).Decode(&e); err != nil {
			return fmt.Errorf("error parsing the response body: %w", err)
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
		return fmt.Errorf("error parsing the response body: %w", err)
	}

	hitsMetadata := r["hits"].(map[string]interface{})
	totalMetadata := hitsMetadata["total"].(map[string]interface{})

	require.Equal(t, len(documents), int(totalMetadata["value"].(float64)))

	hits := hitsMetadata["hits"].([]interface{})

	for i, document := range documents {
		hit := hits[i].(map[string]interface{})
		source := hit["_source"].(map[string]interface{})

		require.EqualValues(t, document, source)
	}

	return nil
}
