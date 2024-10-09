package source

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	// metadataFieldIndex is a name of a record metadata field that stores a ElasticSearch Index name.
	metadataFieldIndex = "opencdc.collection"
)

type Worker struct {
	source *Source
	index  string
	offset int
}

func NewWorker(source *Source, index string, offset int) {
	worker := &Worker{
		source: source,
		index:  index,
		offset: offset,
	}

	go worker.start()
}

func (w *Worker) start() {
	defer w.source.wg.Done()

	for {
		response, err := w.source.client.Search(context.Background(), w.index, &w.offset, &w.source.config.BatchSize)
		if err != nil || len(response.Hits.Hits) == 0 {
			if err != nil {
				log.Println("search() err:", err)
			}

			select {
			case <-w.source.shutdown:
				log.Println("worker shutting down...")
				return

			case <-time.After(w.source.config.PollingPeriod):
				continue
			}
		}

		for _, hit := range response.Hits.Hits {
			metadata := opencdc.Metadata{
				metadataFieldIndex: hit.Index,
			}
			metadata.SetCreatedAt(time.Now().UTC())

			payload, err := json.Marshal(hit.Source)
			if err != nil {
				// log
				continue
			}

			position := Position{
				ID:    hit.ID,
				Index: hit.Index,
				Pos:   w.offset + 1,
			}
			sdkPosition, err := position.marshal()
			if err != nil {
				// handle
				continue
			}

			key := make(opencdc.StructuredData)
			key["id"] = hit.ID

			record := sdk.Util.Source.NewRecordCreate(sdkPosition, metadata, key, opencdc.RawData(payload))

			select {
			case w.source.ch <- record:
				w.offset++

			case <-w.source.shutdown:
				log.Println("worker shutting down...")
				return
			}
		}
	}
}
