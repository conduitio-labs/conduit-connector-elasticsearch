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
	"io"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

type Destination struct {
	sdk.UnimplementedDestination

	config       Config
	getIndexName IndexFn

	client client
}

//go:generate moq -out client_moq_test.go . client
type client = elasticsearch.Client

// GetClient returns the current Elasticsearch client.
func (d *Destination) GetClient() elasticsearch.Client {
	return d.client
}

func (d *Destination) Parameters() config.Parameters {
	return d.config.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg config.Config) (err error) {
	err = sdk.Util.ParseConfig(ctx, cfg, &d.config, NewDestination().Parameters())
	if err != nil {
		return err
	}

	d.getIndexName, err = d.config.IndexFunction()
	if err != nil {
		return fmt.Errorf("invalid index name or index function: %w", err)
	}

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

func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
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

		if (itemResponse.Status >= 200 && itemResponse.Status < 300) || itemResponse.Status == 404 {
			continue
		}

		if itemResponse.Error == nil {
			return n + 1, fmt.Errorf(
				"item with key=%s %s failure: unknown error status: %d",
				itemResponse.ID,
				operationType,
				itemResponse.Status,
			)
		}

		return n + 1, fmt.Errorf(
			"item with key=%s %s failure: [%s] %s: %s",
			itemResponse.ID,
			operationType,
			itemResponse.Error.Type,
			itemResponse.Error.Reason,
			itemResponse.Error.CausedBy,
		)
	}

	return len(records), nil
}

func (d *Destination) Teardown(context.Context) error {
	return nil // No close routine needed
}

// prepareBulkRequestPayload converts all pending operations into a valid Elasticsearch Bulk API request.
func (d *Destination) prepareBulkRequestPayload(records []opencdc.Record) (*bytes.Buffer, error) {
	data := &bytes.Buffer{}

	for _, record := range records {
		index, err := d.getIndexName(record)
		if err != nil {
			return nil, err
		}

		var key string
		if record.Key != nil {
			key = string(record.Key.Bytes())
		}

		op := record.Operation
		if key == "" {
			op = opencdc.OperationCreate
		}
		switch {
		case key == "":
			if err := d.writeInsertOperation(data, record, index); err != nil {
				return nil, err
			}

		case op == opencdc.OperationSnapshot || op == opencdc.OperationCreate || op == opencdc.OperationUpdate:
			if err := d.writeUpsertOperation(key, data, record, index); err != nil {
				return nil, err
			}

		case op == opencdc.OperationDelete:
			if err := d.writeDeleteOperation(key, data, index); err != nil {
				return nil, err
			}

		default:
			return nil, fmt.Errorf("operation %v on record %v not supported", record.Operation, record.Key)
		}
	}

	return data, nil
}

// writeInsertOperation adds create new Document without ID request into Bulk API request.
func (d *Destination) writeInsertOperation(data *bytes.Buffer, item opencdc.Record, index string) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, payload, err := d.client.PrepareCreateOperation(item, index)
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

// writeUpsertOperation adds upsert a Document with ID request into Bulk API request.
func (d *Destination) writeUpsertOperation(key string, data *bytes.Buffer, item opencdc.Record, index string) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, payload, err := d.client.PrepareUpsertOperation(key, item, index)
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

// writeDeleteOperation adds delete a Document by ID request into Bulk API request.
func (d *Destination) writeDeleteOperation(key string, data *bytes.Buffer, index string) error {
	jsonEncoder := json.NewEncoder(data)

	// Prepare data
	metadata, err := d.client.PrepareDeleteOperation(key, index)
	if err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	// Write metadata
	if err := jsonEncoder.Encode(metadata); err != nil {
		return fmt.Errorf("failed to prepare metadata with key=%s: %w", key, err)
	}

	return nil
}

// executeBulkRequest executes Bulk API request and parses the response.
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
