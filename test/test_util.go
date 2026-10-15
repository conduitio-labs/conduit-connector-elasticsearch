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

package test

import (
	"context"
	"fmt"
	"os"

	es "github.com/conduitio-labs/conduit-connector-elasticsearch"
	esDestination "github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// Destination parameter names used by the integration tests.
const (
	ParamVersion  = "version"
	ParamHost     = "host"
	ParamIndex    = "index"
	ParamType     = "type"
	ParamBulkSize = "bulkSize"
)

// Host is the Elasticsearch address the integration tests use: the
// ELASTICSEARCH_HOST environment variable, or http://127.0.0.1:9200 (the port
// the compose files publish by default). Setting it lets the suite run against
// a container on an ephemeral port.
func Host() string {
	if h := os.Getenv("ELASTICSEARCH_HOST"); h != "" {
		return h
	}
	return "http://127.0.0.1:9200"
}

// ConfigureDestination parses cfg into the destination the way the SDK does
// before Open: defaults and validations come from the connector specification,
// including the middleware parameters.
func ConfigureDestination(ctx context.Context, dest sdk.Destination, cfg map[string]string) error {
	return sdk.Util.ParseConfig(ctx, cfg, dest.Config(), es.Connector.NewSpecification().DestinationParams)
}

func GetClient(cfgMap map[string]string) (elasticsearch.Client, error) {
	ctx := context.Background()
	cfg := esDestination.Config{}

	err := sdk.Util.ParseConfig(ctx, cfgMap, &cfg, es.Connector.NewSpecification().DestinationParams)
	if err != nil {
		return nil, fmt.Errorf("failed parsing config: %w", err)
	}

	client, err := elasticsearch.NewClient(cfg.Version, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed creating client: %w", err)
	}

	return client, nil
}
