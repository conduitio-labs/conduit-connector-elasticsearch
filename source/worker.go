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
	"encoding/json"
	"log"
	"time"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/api"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Worker struct {
	source           *Source
	index            string
	sortByField      string
	orderBy          string
	lastRecordSortID int64
}

// NewWorker create a new worker goroutine and starts polling elasticsearch for new records.
func NewWorker(
	ctx context.Context,
	source *Source,
	index string,
	sortByField string,
	orderBy string,
	lastRecordSortID int64,
) {
	worker := &Worker{
		source:           source,
		index:            index,
		sortByField:      sortByField,
		orderBy:          orderBy,
		lastRecordSortID: lastRecordSortID,
	}

	go worker.start(ctx)
}

// start polls elasticsearch for new records and writes it into the source channel.
func (w *Worker) start(ctx context.Context) {
	defer w.source.wg.Done()

	for {
		request := &api.SearchRequest{
			Index:       w.index,
			Size:        &w.source.config.BatchSize,
			SearchAfter: w.lastRecordSortID,
			SortBy:      w.sortByField,
			Order:       w.orderBy,
		}

		response, err := w.source.client.Search(ctx, request)
		if err != nil || len(response.Hits.Hits) == 0 {
			if err != nil {
				log.Println("search() err:", err)
			}

			select {
			case <-w.source.shutdown:
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return

			case <-time.After(w.source.config.PollingPeriod):
				continue
			}
		}

		for _, hit := range response.Hits.Hits {
			metadata := opencdc.Metadata{
				opencdc.MetadataCollection: hit.Index,
			}
			metadata.SetCreatedAt(time.Now().UTC())

			payload, err := json.Marshal(hit.Source)
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("error marshal payload")
				continue
			}

			if len(hit.Sort) == 0 {
				// this should never happen
				sdk.Logger(ctx).Err(err).Msg("error hit.Sort is empty")
				continue
			}

			w.source.position.update(hit.Index, hit.Sort[0])

			sdkPosition, err := w.source.position.marshal()
			if err != nil {
				sdk.Logger(ctx).Err(err).Msg("error marshal position")
				continue
			}

			key := make(opencdc.StructuredData)
			key["id"] = hit.ID

			record := sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(payload))

			select {
			case w.source.ch <- record:
				w.lastRecordSortID = hit.Sort[0]

			case <-w.source.shutdown:
				sdk.Logger(ctx).Debug().Msg("worker shutting down...")
				return
			}
		}
	}
}
