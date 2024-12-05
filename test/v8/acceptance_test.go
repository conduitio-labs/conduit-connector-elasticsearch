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

package v8

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	es "github.com/conduitio-labs/conduit-connector-elasticsearch"
	esDestination "github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	v8 "github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch/v8"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"go.uber.org/goleak"
)

type CustomConfigurableAcceptanceTestDriver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d *CustomConfigurableAcceptanceTestDriver) GenerateRecord(t *testing.T, op opencdc.Operation) opencdc.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t, op)

	// Override Key
	record.Key = opencdc.RawData(strconv.FormatInt(time.Now().UnixMicro(), 10))

	// Override Payload
	after := opencdc.StructuredData{}

	for _, v := range record.Payload.After.(opencdc.StructuredData) {
		after[fmt.Sprintf(
			"f%s",
			strconv.FormatInt(time.Now().UnixMicro(), 10),
		)] = v
	}

	record.Payload.After = after

	return record
}

func (d *CustomConfigurableAcceptanceTestDriver) ReadFromDestination(_ *testing.T, records []opencdc.Record) []opencdc.Record {
	// No source connector, return wanted records
	return records
}

func TestAcceptance(t *testing.T) {
	destinationConfig := map[string]string{
		esDestination.ConfigVersion:  elasticsearch.Version8,
		esDestination.ConfigHost:     "http://127.0.0.1:9200",
		esDestination.ConfigIndex:    "acceptance_idx",
		esDestination.ConfigBulkSize: "100",
	}

	client, err := test.GetClient(destinationConfig)
	if err != nil {
		t.Logf("failed to create elasticsearch client: %v", err)
	}

	sdk.AcceptanceTest(t, &CustomConfigurableAcceptanceTestDriver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector: sdk.Connector{
					NewSpecification: es.Specification,

					NewSource: nil,

					NewDestination: esDestination.NewDestination,
				},

				DestinationConfig: destinationConfig,

				AfterTest: func(_ *testing.T) {
					assertIndexIsDeleted(
						client.(*v8.Client).GetClient(),
						destinationConfig[esDestination.ConfigIndex],
					)
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
