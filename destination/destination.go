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
	"sync"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch"
)

func NewDestination() sdk.Destination {
	return &Destination{}
}

type Destination struct {
	sdk.UnimplementedDestination

	config          Config
	client          client
	mutex           sync.Mutex
	operationsQueue BufferQueue
}

//go:generate moq -out client_moq_test.go . client
type client = elasticsearch.Client

func (d *Destination) GetClient() elasticsearch.Client {
	return d.client
}

func (d *Destination) Configure(_ context.Context, cfgRaw map[string]string) (err error) {
	d.config, err = ParseConfig(cfgRaw)

	return
}

func (d *Destination) Open(ctx context.Context) (err error) {
	// Initialize Elasticsearch client
	d.client, err = elasticsearch.NewClient(d.config.Version, d.config)
	if err != nil {
		return fmt.Errorf("connection could not be established: %w", err)
	}

	// Check the connection
	if err := d.client.Ping(ctx); err != nil {
		return fmt.Errorf("connection could not be established: %w", err)
	}

	// Initialize the buffer
	d.mutex = sync.Mutex{}
	d.operationsQueue = make(BufferQueue, 0, d.config.BulkSize)

	return nil
}

func (d *Destination) WriteAsync(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.operationsQueue.Enqueue(&operation{
		CreatedAt: record.CreatedAt,
		Record:    record,
		AckFunc:   ackFunc,
	})

	if uint64(d.operationsQueue.Len()) >= d.config.BulkSize {
		if err := d.Flush(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) Flush(ctx context.Context) error {
	// check if there are operations in the buffer
	if d.operationsQueue.Empty() {
		return nil
	}

	// Sort operations to ensure the order
	d.operationsQueue.Sort()

	// Execute operations
	retriesLeft := d.config.Retries

	for {
		// Set up the buffer for failed operations
		failedOperations := make(BufferQueue, 0, d.operationsQueue.Len())

		// Prepare request payload
		data, err := d.prepareBulkRequestPayload(ctx)
		if err != nil {
			return err
		}

		// Send the bulk request
		response, err := d.executeBulkRequest(ctx, data)
		if err != nil {
			return err
		}

		// Ack results
		for n, item := range response.Items {
			// Detect operation result
			var itemResponse bulkResponseItem

			switch {
			case item.Create != nil:
				itemResponse = *item.Create

			case item.Update != nil:
				itemResponse = *item.Update

			case item.Delete != nil:
				itemResponse = *item.Delete

			default:
				sdk.Logger(ctx).Warn().Msg("no update or delete details were found in Elasticsearch response")

				continue
			}

			// ACK
			// The order of responses is the same as the order of requests
			// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html#bulk-api-response-body
			ackFunc := d.operationsQueue[n].AckFunc

			if itemResponse.Status >= 200 && itemResponse.Status < 300 {
				if err := ackFunc(nil); err != nil {
					return err
				}

				continue
			}

			if itemResponse.Error == nil {
				d.operationsQueue[n].err = fmt.Errorf(
					"item with key=%s create/upsert/delete failure: unknown error",
					itemResponse.ID,
				)
			} else {
				d.operationsQueue[n].err = fmt.Errorf(
					"item with key=%s create/upsert/delete failure: [%s] %s: %s",
					itemResponse.ID,
					itemResponse.Error.Type,
					itemResponse.Error.Reason,
					itemResponse.Error.CausedBy,
				)
			}

			failedOperations.Enqueue(d.operationsQueue[n])
		}

		// Fail pending operations when retries limit is reached
		if retriesLeft == 0 {
			for _, failedOperation := range failedOperations {
				if err := failedOperation.AckFunc(failedOperation.err); err != nil {
					return err
				}
			}

			break
		}

		// Check if there are operations to retry
		if failedOperations.Len() == 0 {
			break
		}

		// Set up for retry
		retriesLeft--

		d.operationsQueue = failedOperations // No need to sort since d.operationsQueue is already sorted
	}

	// Reset buffer
	d.operationsQueue = make(BufferQueue, 0, d.config.BulkSize)

	return nil
}

func (d *Destination) Teardown(context.Context) error {
	return nil // No close routine needed
}

func (d *Destination) prepareBulkRequestPayload(ctx context.Context) (*bytes.Buffer, error) {
	data := &bytes.Buffer{}

	for _, item := range d.operationsQueue {
		record := item.Record
		action := record.Metadata["action"]

		var key string
		if record.Key != nil {
			key = string(record.Key.Bytes())
		}

		if key == "" {
			action = "insert"
		} else if action == "" {
			action = "create"
		}

		switch action {
		case "insert":
			if err := d.writeInsertOperation(data, record); err != nil {
				return nil, err
			}

		case "create", "created",
			"update", "updated":
			if err := d.writeUpsertOperation(key, data, record); err != nil {
				return nil, err
			}

		case "delete", "deleted":
			if err := d.writeDeleteOperation(key, data); err != nil {
				return nil, err
			}

		default:
			sdk.Logger(ctx).Warn().Msgf("unsupported action: %+v", action)

			continue
		}
	}

	return data, nil
}

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
	defer responseBody.Close()

	// Read individual errors
	var response bulkResponse
	if err := json.Unmarshal(bodyContents, &response); err != nil {
		return bulkResponse{}, fmt.Errorf("bulk response failure: could not read the response: %w", err)
	}

	return response, nil
}
