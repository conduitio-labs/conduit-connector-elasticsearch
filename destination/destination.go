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

package destination

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"io"
)

func NewDestination() sdk.Destination {
	return &Destination{}
}

type Destination struct {
	sdk.UnimplementedDestination

	config Config
	client client
}

//go:generate moq -out client_moq_test.go . client
type client = elasticsearch.Client

// GetClient returns the current Elasticsearch client
func (d *Destination) GetClient() elasticsearch.Client {
	return d.client
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		ConfigKeyVersion: {
			Default:  "",
			Required: true,
			Description: fmt.Sprintf(
				"The version of the Elasticsearch service. One of: %s, %s, %s, %s",
				elasticsearch.Version5,
				elasticsearch.Version6,
				elasticsearch.Version7,
				elasticsearch.Version8,
			),
		},
		ConfigKeyHost: {
			Default:     "",
			Required:    true,
			Description: "The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).",
		},
		ConfigKeyUsername: {
			Default:     "",
			Required:    false,
			Description: "The username for HTTP Basic Authentication.",
		},
		ConfigKeyPassword: {
			Default:     "",
			Required:    false,
			Description: "The password for HTTP Basic Authentication.",
		},
		ConfigKeyCloudID: {
			Default:     "",
			Required:    false,
			Description: "Endpoint for the Elastic Service (https://elastic.co/cloud).",
		},
		ConfigKeyAPIKey: {
			Default:     "",
			Required:    false,
			Description: "Base64-encoded token for authorization; if set, overrides username/password and service token.",
		},
		ConfigKeyServiceToken: {
			Default:     "",
			Required:    false,
			Description: "Service token for authorization; if set, overrides username/password.",
		},
		ConfigKeyCertificateFingerprint: {
			Default:     "",
			Required:    false,
			Description: "SHA256 hex fingerprint given by Elasticsearch on first launch.",
		},
		ConfigKeyIndex: {
			Default:     "",
			Required:    true,
			Description: "The name of the index to write the data to.",
		},
		ConfigKeyType: {
			Default:     "",
			Required:    false,
			Description: "The name of the index's type to write the data to.",
		},
		ConfigKeyBulkSize: {
			Default:     "1000",
			Required:    true,
			Description: "The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10 000`.",
		},
		ConfigKeyRetries: {
			Default:     "0",
			Required:    false,
			Description: "The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255.",
		},
	}
}

func (d *Destination) Configure(_ context.Context, cfgRaw map[string]string) (err error) {
	d.config, err = ParseConfig(cfgRaw)

	return
}

func (d *Destination) Open(ctx context.Context) (err error) {
	// Initialize Elasticsearch client
	d.client, err = elasticsearch.NewClient(d.config.Version, d.config)
	if err != nil {
		return fmt.Errorf("failed creating client: %w", err)
	}

	// Check the connection
	if err := d.client.Ping(ctx); err != nil {
		return fmt.Errorf("server cannot be pinged: %w", err)
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// Execute operations
	// todo return retries

	// Prepare request payload
	data, err := d.prepareBulkRequestPayload(records)
	if err != nil {
		return 0, err
	}

	// Send the bulk request
	response, err := d.executeBulkRequest(ctx, data)
	if err != nil {
		return 0, err
	}

	// NB: The order of responses is the same as the order of requests
	// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
	for n, item := range response.Items {
		// Detect operation result
		var itemResponse bulkResponseItem
		var operationType string

		switch {
		case item.Index != nil:
			itemResponse = *item.Index
			operationType = "index"

		case item.Create != nil:
			itemResponse = *item.Create
			operationType = "create"

		case item.Update != nil:
			itemResponse = *item.Update
			operationType = "update"

		case item.Delete != nil:
			itemResponse = *item.Delete
			operationType = "delete"

		default:
			sdk.Logger(ctx).Warn().Msg("no index, create, update or delete details were found in Elasticsearch response")

			continue
		}

		if itemResponse.Status >= 200 && itemResponse.Status < 300 {
			continue
		}

		if itemResponse.Error == nil {
			return n + 1, fmt.Errorf(
				"item with key=%s %s failure: unknown error",
				itemResponse.ID,
				operationType,
			)
		} else {
			return n + 1, fmt.Errorf(
				"item with key=%s %s failure: [%s] %s: %s",
				itemResponse.ID,
				operationType,
				itemResponse.Error.Type,
				itemResponse.Error.Reason,
				itemResponse.Error.CausedBy,
			)
		}

	}

	return len(records), nil
}

func (d *Destination) Teardown(context.Context) error {
	return nil // No close routine needed
}

// prepareBulkRequestPayload converts all pending operations into a valid Elasticsearch Bulk API request.
func (d *Destination) prepareBulkRequestPayload(records []sdk.Record) (*bytes.Buffer, error) {
	data := &bytes.Buffer{}

	for _, record := range records {
		var key string
		if record.Key != nil {
			key = string(record.Key.Bytes())
		}

		switch record.Operation {
		case sdk.OperationCreate, sdk.OperationSnapshot:
			if err := d.writeInsertOperation(data, record); err != nil {
				return nil, err
			}

		case sdk.OperationUpdate:
			if err := d.writeUpsertOperation(key, data, record); err != nil {
				return nil, err
			}

		case sdk.OperationDelete:
			if err := d.writeDeleteOperation(key, data); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("operation %v on record %v not supported", record.Operation, record.Key)
		}
	}

	return data, nil
}

// writeInsertOperation adds create new Document without ID request into Bulk API request
func (d *Destination) writeInsertOperation(data *bytes.Buffer, item sdk.Record) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, payload, err := d.client.PrepareCreateOperation(item)
	if err != nil {
		return fmt.Errorf("failed to prepare metadata: %w", err)
	}

	// Write metadata
	if err := jsonEncoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to prepare metadata: %w", err)
	}

	// Write payload
	if err := jsonEncoder.Encode(payload); err != nil {
		return fmt.Errorf("failed to prepare data: %w", err)
	}

	return nil
}

// writeUpsertOperation adds upsert a Document with ID request into Bulk API request
func (d *Destination) writeUpsertOperation(key string, data *bytes.Buffer, item sdk.Record) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, payload, err := d.client.PrepareUpsertOperation(key, item)
	if err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	// Write metadata
	if err := jsonEncoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	// Write payload
	if err := jsonEncoder.Encode(payload); err != nil {
		return fmt.Errorf("failed to prepare data with key=%s: %w", key, err)
	}

	return nil
}

// writeDeleteOperation adds delete a Document by ID request into Bulk API request
func (d *Destination) writeDeleteOperation(key string, data *bytes.Buffer) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, err := d.client.PrepareDeleteOperation(key)
	if err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	// Write metadata
	if err := jsonEncoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	return nil
}

// executeBulkRequest executes Bulk API request and parses the response
func (d *Destination) executeBulkRequest(ctx context.Context, data *bytes.Buffer) (bulkResponse, error) {
	// Check if there is any job to do
	if data.Len() < 1 {
		sdk.Logger(ctx).Info().Msg("no operations to execute in bulk, skipping")

		return bulkResponse{}, nil
	}

	defer data.Reset()

	// Execute the request
	responseBody, err := d.client.Bulk(ctx, bytes.NewReader(data.Bytes()))
	if err != nil {
		return bulkResponse{}, fmt.Errorf("bulk request failure: %w", err)
	}

	// Get the response
	bodyContents, err := io.ReadAll(responseBody)
	if err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: failed to read the result: %w", err)
	}

	if err := responseBody.Close(); err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: failed to read the result: %w", err)
	}

	// Read individual errors
	var response bulkResponse
	if err := json.Unmarshal(bodyContents, &response); err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: could not read the response: %w", err)
	}

	return response, nil
}
