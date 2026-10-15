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

package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config   Config
	client   elasticsearch.Client
	position *Position
	ch       chan opencdc.Record
	wg       *sync.WaitGroup
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	// A new variable per source: the SDK parses the configuration into the
	// value this points to.
	keyEnabled := false
	return sdk.SourceWithMiddleware(&Source{
		config: Config{
			DefaultSourceMiddleware: sdk.DefaultSourceMiddleware{
				SourceWithSchemaExtraction: sdk.SourceWithSchemaExtraction{
					// Invariant 6: keep the key exactly as earlier releases
					// emitted it. Up to SDK v0.12 the key schema extraction
					// never ran for this source (it implements Read, not
					// ReadN, and the middleware only hooked ReadN), so the key
					// left as structured data with no schema even though the
					// parameter said "true". SDK v0.15 runs it on Read too,
					// which would switch every key to Avro bytes with a
					// "<index>.key" schema. Default to what users actually
					// got; setting it to true opts in to the SDK behavior.
					KeyEnabled: &keyEnabled,
				},
			},
		},
	})
}

// Config returns the source configuration. The SDK parses the connector
// configuration into it and validates it before calling Open.
func (s *Source) Config() sdk.SourceConfig {
	return &s.config
}

// Open parses the position and initializes the iterator.
func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an ElasticSearch Source...")

	var err error
	s.position, err = ParseSDKPosition(position)
	if err != nil {
		return err
	}

	// Initialize Elasticsearch client
	s.client, err = elasticsearch.NewClient(s.config.Version, s.config)
	if err != nil {
		return fmt.Errorf("failed creating client: %w", err)
	}

	// Check the connection
	if err := s.client.Ping(ctx); err != nil {
		return fmt.Errorf("server cannot be pinged: %w", err)
	}

	s.ch = make(chan opencdc.Record, s.config.BatchSize)
	s.wg = &sync.WaitGroup{}

	for index, sort := range s.config.Indexes {
		s.wg.Add(1)
		var init bool
		lastRecordSortID, ok := s.position.IndexPositions[index]
		if !ok {
			// read from scratch
			init = true
		}

		// a new worker for a new index
		NewWorker(ctx, s.client, index, lastRecordSortID, init, s.config.PollingPeriod, s.config.BatchSize, s.wg, s.ch, s.position, sort, s.config.Retries)
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from ElasticSearch Source...")

	if s == nil || s.ch == nil {
		return opencdc.Record{}, fmt.Errorf("error source not opened for reading")
	}

	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case record, ok := <-s.ch:
		if !ok {
			return opencdc.Record{}, fmt.Errorf("error reading data")
		}
		return record, nil
	}
}

// Ack logs the debug event with the position.
func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Trace().Str("position", string(position)).Msg("got ack")
	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the ElasticSearch Source")
	// wait for goroutines to finish
	s.wg.Wait()
	// close the read channel for write
	close(s.ch)
	// reset read channel to nil, to avoid reading buffered records
	s.ch = nil
	return nil
}
