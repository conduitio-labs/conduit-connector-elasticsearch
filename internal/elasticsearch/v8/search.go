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
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/esapi"
)

const (
	// metadataFieldIndex is a name of a record metadata field that stores a ElasticSearch Index name.
	metadataFieldIndex = "elasticsearch.index"
)

// SearchReponse is the JSON response from Elasticsearch search query.
type SearchReponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Index  string         `json:"index"`
			ID     string         `json:"_id"`
			Score  float64        `json:"_score"`
			Source map[string]any `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// Search calls the elasticsearch search api and retuns a list of opencdc.Record read from an index.
func (c *Client) Search(ctx context.Context, index string, offset, size *int) (interface{}, error) {
	// Create the search request
	req := esapi.SearchRequest{
		Index: []string{index},
		Body: strings.NewReader(fmt.Sprintf(`{
			"query": {
				"match_all": {}
			}
		}`)),
		From: offset,
		Size: size,
	}

	// Perform the request
	res, err := req.Do(ctx, c.es)
	if err != nil {
		return nil, fmt.Errorf("error getting search response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error search response: %s", res.String())
	}

	var response SearchReponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error parsing the search response body: %s", err)
	}

	return response, nil
}
