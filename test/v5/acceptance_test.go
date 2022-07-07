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
	"fmt"
	"strconv"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	es "github.com/miquido/conduit-connector-elasticsearch"
	esDestination "github.com/miquido/conduit-connector-elasticsearch/destination"
	"github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch"
	v5 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v5"
	"go.uber.org/goleak"
)

type WithCustomRecordGeneratorDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d *WithCustomRecordGeneratorDriver) GenerateRecord(t *testing.T) sdk.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t)

	// Override Key
	record.Key = sdk.RawData(strconv.FormatInt(time.Now().UnixMicro(), 10))

	// Override Payload
	payload := sdk.StructuredData{}

	for _, v := range record.Payload.(sdk.StructuredData) {
		payload[fmt.Sprintf(
			"f%s",
			strconv.FormatInt(time.Now().UnixMicro(), 10),
		)] = v
	}

	record.Payload = payload

	return record
}

func (d *WithCustomRecordGeneratorDriver) ReadFromDestination(_ *testing.T, records []sdk.Record) []sdk.Record {
	// No source connector, return wanted records
	return records
}

func TestAcceptance(t *testing.T) {
	var dest *esDestination.Destination

	destinationConfig := map[string]string{
		esDestination.ConfigKeyVersion:  elasticsearch.Version5,
		esDestination.ConfigKeyHost:     "http://127.0.0.1:9200",
		esDestination.ConfigKeyIndex:    "acceptance_idx",
		esDestination.ConfigKeyType:     "acceptance_type",
		esDestination.ConfigKeyBulkSize: "100",
	}

	sdk.AcceptanceTest(t, &WithCustomRecordGeneratorDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: es.Specification,

					NewSource: nil,

					NewDestination: func() sdk.Destination {
						dest = esDestination.NewDestination().(*esDestination.Destination)

						return dest
					},
				},

				DestinationConfig: destinationConfig,

				AfterTest: func(t *testing.T) {
					if client := dest.GetClient(); client != nil {
						assertIndexIsDeleted(
							client.(*v5.Client).GetClient(),
							destinationConfig[esDestination.ConfigKeyIndex],
						)
					}
				},

				GenerateDataType: sdk.GenerateStructuredData,

				GoleakOptions: []goleak.Option{
					// Routines created by Elasticsearch client
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
					goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
				},
			},
		},
	})
}
