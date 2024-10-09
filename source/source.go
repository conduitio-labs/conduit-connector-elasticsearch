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
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config Config
	client elasticsearch.Client
	// holds the last position of indexes returned by Read() method
	offsets map[string]int
	// hold the initial sdk position of indexes, used for Ack() method
	positions []Position
	ch        chan opencdc.Record
	shutdown  chan struct{}
	wg        *sync.WaitGroup
}

// NewSource initialises a new source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters returns a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() config.Parameters {
	return s.config.Parameters()
}

// Configure parses and stores configurations,
// returns an error in case of invalid configuration.
func (s *Source) Configure(ctx context.Context, cfgRaw config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring ElasticSearch Source...")

	err := sdk.Util.ParseConfig(ctx, cfgRaw, &s.config, NewSource().Parameters())
	if err != nil {
		return err
	}

	return nil
}

// Open parses the position and initializes the iterator.
func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening an ElasticSearch Source...")

	var err error
	s.positions, err = ParseSDKPosition(position)
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
	s.shutdown = make(chan struct{})
	s.offsets = make(map[string]int)
	s.wg = &sync.WaitGroup{}

	for _, index := range s.config.Indexes {
		s.wg.Add(1)

		offset := 0
		for _, position := range s.positions {
			if index == position.Index {
				offset = position.Pos
			}
		}

		s.offsets[index] = offset

		// a new worker for a new index
		NewWorker(s, index, offset)
	}

	return nil
}

// Read returns the next record.
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	sdk.Logger(ctx).Debug().Msg("Reading a record from ElasticSearch Source...")

	if s == nil || s.ch == nil {
		return opencdc.Record{}, fmt.Errorf("error source not opened for reading")
	}

	record, ok := <-s.ch
	if !ok {
		return opencdc.Record{}, fmt.Errorf("error reading data")
	}

	index, ok := record.Metadata["metadataFieldIndex"]
	if !ok {
		// this should never happen
		return opencdc.Record{}, fmt.Errorf("error index not found in data header")
	}

	offset, ok := s.offsets[index]
	if !ok {
		// this should never happen
		return opencdc.Record{}, fmt.Errorf("error offset index not found")
	}

	s.offsets[index] = offset + 1
	return record, nil
}

// Ack logs the debug event with the position.
func (s *Source) Ack(_ context.Context, position opencdc.Position) error {
	pos := Position{}
	err := pos.unmarshal(position)
	if err != nil {
		return fmt.Errorf("error unmarshaling opencdc position: %w", err)
	}

	last := s.offsets[pos.Index]

	for _, p := range s.positions {
		if p.Index == pos.Index && p.Pos > pos.Pos {
			return fmt.Errorf("error acknowledging: position less than initial sdk position")
		}
	}

	if last < pos.Pos {
		return fmt.Errorf("error acknowledging: record not read")
	}

	return nil
}

// Teardown gracefully shutdown connector.
func (s *Source) Teardown(ctx context.Context) error {
	sdk.Logger(ctx).Info().Msg("Tearing down the ElasticSearch Source")

	if s == nil || s.ch == nil {
		return fmt.Errorf("error source not opened for teardown")
	}

	close(s.shutdown)

	// wait for goroutines to finish
	s.wg.Wait()
	// close the read channel for write
	close(s.ch)
	// reset read channel to nil, to avoid reading buffered records
	s.ch = nil
	return nil
}
