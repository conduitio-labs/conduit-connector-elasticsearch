// Copyright © 2026 Meroxa, Inc. and Miquido
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
	"errors"
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/decimaltext"
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/conduitio/conduit-connector-sdk/schema"
	"github.com/iskorotkov/avro/v2"
)

// maxParsedSchemas bounds the parsed-schema memo. A pipeline writes a handful
// of payload schemas; when there are more, the memo starts over.
const maxParsedSchemas = 64

// renderDecimals returns records with every Avro decimal in Payload.After
// written as exact decimal text (#171). Records without a *big.Rat are
// returned unchanged, and so is the slice when no record has one; otherwise a
// new slice holds the rewritten copies, and the records passed in are not
// modified.
//
// The key is left as it is: Key.Bytes() is the Elasticsearch document ID, and
// changing how a decimal key prints would move existing documents to new IDs.
func (d *Destination) renderDecimals(ctx context.Context, records []opencdc.Record) ([]opencdc.Record, error) {
	var out []opencdc.Record
	for i, rec := range records {
		after, ok := rec.Payload.After.(opencdc.StructuredData)
		if !ok || !decimaltext.Contains(after) {
			continue
		}
		sch, err := d.payloadSchema(ctx, rec)
		if err != nil {
			return nil, fmt.Errorf("record %d: %w", i, err)
		}
		rendered, err := decimaltext.Render(after, sch)
		if err != nil {
			// Invariant 6: never write a decimal we cannot write exactly.
			return nil, fmt.Errorf("record %d: payload: %w", i, err)
		}
		if out == nil {
			out = make([]opencdc.Record, len(records))
			copy(out, records)
		}
		out[i].Payload.After = rendered.(opencdc.StructuredData)
	}
	if out == nil {
		return records, nil
	}
	return out, nil
}

// payloadSchema returns the Avro schema the SDK middleware decoded the payload
// with, taken from the record metadata like the middleware does, or nil when
// the record carries no payload schema. Only the decimals' scales come from
// it; without it they are written with the fewest exact digits.
func (d *Destination) payloadSchema(ctx context.Context, rec opencdc.Record) (avro.Schema, error) {
	subject, errSubject := rec.Metadata.GetPayloadSchemaSubject()
	version, errVersion := rec.Metadata.GetPayloadSchemaVersion()
	if errors.Is(errSubject, opencdc.ErrMetadataFieldNotFound) ||
		errors.Is(errVersion, opencdc.ErrMetadataFieldNotFound) {
		return nil, nil //nolint:nilnil // no schema is a valid answer: Render then uses the fewest exact digits
	}
	if errSubject != nil {
		return nil, fmt.Errorf("payload schema subject: %w", errSubject)
	}
	if errVersion != nil {
		return nil, fmt.Errorf("payload schema version: %w", errVersion)
	}

	// The SDK caches the schema service response, so this is the schema the
	// middleware has just fetched.
	sch, err := schema.Get(ctx, subject, version)
	if err != nil {
		return nil, fmt.Errorf("get payload schema %s:%d to read decimal scales: %w", subject, version, err)
	}
	if sch.Type != schema.TypeAvro {
		return nil, nil //nolint:nilnil // only Avro schemas carry decimal scales
	}
	return d.schemas.parse(sch)
}

// parsedSchemas memoizes parsed Avro schemas by subject and version, keeping
// the schema text to detect a subject:version that now holds another schema.
type parsedSchemas struct {
	mu sync.Mutex
	m  map[parsedSchemaKey]parsedSchema
}

type parsedSchemaKey struct {
	subject string
	version int
}

type parsedSchema struct {
	text   []byte
	schema avro.Schema
}

func (p *parsedSchemas) parse(sch schema.Schema) (avro.Schema, error) {
	key := parsedSchemaKey{subject: sch.Subject, version: sch.Version}
	p.mu.Lock()
	defer p.mu.Unlock()
	if e, ok := p.m[key]; ok && bytes.Equal(e.text, sch.Bytes) {
		return e.schema, nil
	}
	// A private cache: the package-level default would let two schemas that
	// declare the same named type with different definitions share one.
	// conduit-commons parses with that default, so the middleware can decode
	// with a schema that refers to a named type only another schema defines;
	// such a schema fails here, on every attempt.
	parsed, err := avro.ParseBytesWithCache(sch.Bytes, "", &avro.SchemaCache{})
	if err != nil {
		return nil, fmt.Errorf(
			"parse payload schema %s:%d on its own to read decimal scales (permanent: "+
				"this record fails until the schema is self-contained): %w",
			sch.Subject, sch.Version, err)
	}
	if p.m == nil || len(p.m) >= maxParsedSchemas {
		p.m = make(map[parsedSchemaKey]parsedSchema)
	}
	p.m[key] = parsedSchema{text: bytes.Clone(sch.Bytes), schema: parsed}
	return parsed, nil
}
