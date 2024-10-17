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

package elasticsearch

import (
	"context"
	"io"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/api"
	"github.com/conduitio/conduit-commons/opencdc"
)

// Client describes Elasticsearch client interface.
type Client interface {
	// Ping executes Elasticsearch ping request.
	// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html
	Ping(ctx context.Context) error

	// Bulk executes Elasticsearch Bulk API request.
	// See: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	Bulk(ctx context.Context, reader io.Reader) (io.ReadCloser, error)

	// PrepareCreateOperation prepares insert operation definition for Bulk API query.
	PrepareCreateOperation(item opencdc.Record) (metadata interface{}, payload interface{}, err error)

	// PrepareUpsertOperation prepares upsert operation definition for Bulk API query.
	PrepareUpsertOperation(key string, item opencdc.Record) (metadata interface{}, payload interface{}, err error)

	// PrepareDeleteOperation prepares delete operation definition for Bulk API query.
	PrepareDeleteOperation(key string) (metadata interface{}, err error)

	// Search calls the elasticsearch search api and retuns SearchResponse read from an index.
	Search(ctx context.Context, request *api.SearchRequest) (*api.SearchResponse, error)
}
