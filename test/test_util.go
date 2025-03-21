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

	esDestination "github.com/conduitio-labs/conduit-connector-elasticsearch/destination"
	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func GetClient(cfgMap map[string]string) (elasticsearch.Client, error) {
	ctx := context.Background()
	cfg := esDestination.Config{}

	err := sdk.Util.ParseConfig(ctx, cfgMap, &cfg, esDestination.NewDestination().Parameters())
	if err != nil {
		return nil, fmt.Errorf("failed parsing config: %w", err)
	}

	client, err := elasticsearch.NewClient(cfg.Version, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed creating client: %w", err)
	}

	return client, nil
}
