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
	"errors"
	"fmt"
	"io"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/elastic/go-elasticsearch/v5"
)

func NewClient(cfg interface{}) (*Client, error) {
	configTyped, ok := cfg.(config)
	if !ok {
		return nil, errors.New("provided config object is invalid")
	}

	esClient, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{configTyped.GetHost()},
		Username:  configTyped.GetUsername(),
		Password:  configTyped.GetPassword(),
	})
	if err != nil {
		return nil, err
	}

	return &Client{
		es:  esClient,
		cfg: configTyped,
	}, nil
}

type Client struct {
	es  *elasticsearch.Client
	cfg config
}

// GetClient returns Elasticsearch v5 client.
func (c *Client) GetClient() *elasticsearch.Client {
	return c.es
}

func (c *Client) Ping(ctx context.Context) error {
	ping, err := c.es.Ping(c.es.Ping.WithContext(ctx))
	if err != nil {
		return err
	}
	if ping.IsError() {
		return fmt.Errorf("host ping failed: %s", ping.Status())
	}

	return nil
}

func (c *Client) Bulk(ctx context.Context, reader io.Reader) (io.ReadCloser, error) {
	result, err := c.es.Bulk(reader, c.es.Bulk.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if result.IsError() {
		bodyContents, err := io.ReadAll(result.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read the result: %w", err)
		}

		if err := result.Body.Close(); err != nil {
			return nil, fmt.Errorf("failed to read the result: %w", err)
		}

		var errorDetails ErrorResponse
		if err := json.Unmarshal(bodyContents, &errorDetails); err != nil {
			return nil, errors.New(result.Status())
		}

		return nil, fmt.Errorf("[%s] %s", errorDetails.Error.Type, errorDetails.Error.Reason)
	}

	return result.Body, nil
}

func (c *Client) PrepareCreateOperation(item opencdc.Record) (interface{}, interface{}, error) {
	// Prepare metadata
	metadata := bulkRequestActionAndMetadata{
		Index: &bulkRequestIndexAction{
			Index: c.cfg.GetIndex(),
			Type:  c.cfg.GetType(),
		},
	}

	// Prepare payload
	payload, err := preparePayload(&item)
	if err != nil {
		return nil, nil, err
	}

	return metadata, bulkRequestCreateSource(payload), nil
}

func (c *Client) PrepareUpsertOperation(key string, item opencdc.Record) (interface{}, interface{}, error) {
	// Prepare metadata
	metadata := bulkRequestActionAndMetadata{
		Update: &bulkRequestUpdateAction{
			ID:    key,
			Index: c.cfg.GetIndex(),
			Type:  c.cfg.GetType(),
		},
	}

	// Prepare payload
	var err error

	payload := bulkRequestUpdateSource{
		Doc:         nil,
		DocAsUpsert: true,
	}

	payload.Doc, err = preparePayload(&item)
	if err != nil {
		return nil, nil, err
	}

	return metadata, payload, nil
}

func (c *Client) PrepareDeleteOperation(key string, item opencdc.Record) (interface{}, error) {
	return bulkRequestActionAndMetadata{
		Delete: &bulkRequestDeleteAction{
			ID:    key,
			Index: c.cfg.GetIndex(),
			Type:  c.cfg.GetType(),
		},
	}, nil
}

// preparePayload encodes Record's payload as JSON.
func preparePayload(item *opencdc.Record) (json.RawMessage, error) {
	switch itemPayload := item.Payload.After.(type) {
	case opencdc.StructuredData:
		return json.Marshal(itemPayload)

	default:
		// Nothing more can be done, we can trust the source to provide valid JSON
		return itemPayload.Bytes(), nil
	}
}
