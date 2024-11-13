// Copyright © 2024 Meroxa, Inc. and Miquido
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

package api

import (
	"encoding/json"
	"log"
)

// SearchRequest is the request for calling ElasticSearch api.
type SearchRequest struct {
	Index       string  `json:"index"`
	Size        *int    `json:"size:"`
	SearchAfter []int64 `json:"searchAfter"`
	SortBy      string  `json:"sortBy"`
	Order       string  `json:"order"`
}

// SearchResponse is the JSON response from Elasticsearch search query.
type SearchResponse struct {
	Hits struct {
		Hits []struct {
			Index  string         `json:"_index"`
			ID     string         `json:"_id"`
			Source map[string]any `json:"_source"`
			Sort   []int64        `json:"sort"` // used for search_after
		} `json:"hits"`
	} `json:"hits"`
}

// CreateSearchBody creates search request body for search api.
func CreateSearchBody(searchAfter []int64, sortBy, order string) string {
	body := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": struct{}{},
		},
	}

	if sortBy == "_seq_no" {
		body["sort"] = []map[string]interface{}{
			{
				"_seq_no": map[string]string{
					"order": order,
				},
			},
		}
	} else {
		body["sort"] = []map[string]string{
			{
				sortBy: order,
			},
		}
	}

	if len(searchAfter) == 1 {
		body["search_after"] = searchAfter
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		log.Printf("error marshaling the search request body: %s", err)
	}

	return string(jsonBody)
}
