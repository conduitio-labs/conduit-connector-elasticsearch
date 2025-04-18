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
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/api"

	"github.com/elastic/go-elasticsearch/v5/esapi"
)

// Search calls the elasticsearch search api and retuns SearchResponse read from an index.
func (c *Client) Search(ctx context.Context, request *api.SearchRequest) (*api.SearchResponse, error) {
	if request.SortBy == "_seq_no" {
		return nil, fmt.Errorf("v5 does not support sorting using _seq_no")
	}

	// Create the search request
	req := esapi.SearchRequest{
		Index: []string{request.Index},
		Body:  strings.NewReader(api.CreateSearchBody(request.SearchAfter, request.SortBy, request.Order)),
		Size:  request.Size,
	}

	// Perform the request
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res, err := req.Do(ctx, c.es)
	if err != nil {
		return nil, fmt.Errorf("error getting search response: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("error search response: %s", res.String())
	}

	var response *api.SearchResponse
	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("error parsing the search response body: %w", err)
	}

	return response, nil
}
