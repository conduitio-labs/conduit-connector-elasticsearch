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

//go:generate paramgen -output=paramgen.go Config

package source

import (
	"time"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
)

type Config struct {
	// The version of the Elasticsearch service. One of: 5, 6, 7, 8.
	Version elasticsearch.Version `json:"version" validate:"required"`
	// The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).
	Host string `json:"host" validate:"required"`
	// The username for HTTP Basic Authentication.
	Username string `json:"username"`
	// The password for HTTP Basic Authentication.
	Password string `json:"password"`
	// Endpoint for the Elastic Service (https://elastic.co/cloud).
	CloudID string `json:"cloudID"`
	// Base64-encoded token for authorization; if set, overrides username/password and service token.
	APIKey string `json:"APIKey"`
	// Service token for authorization; if set, overrides username/password.
	ServiceToken string `json:"serviceToken"`
	// SHA256 hex fingerprint given by Elasticsearch on first launch.
	CertificateFingerprint string `json:"certificateFingerprint"`
	// The name of the indexes to read data from.
	Indexes []string `json:"indexes" validate:"required"`
	// The sortbyField for each index to be used by elasticsearch search api.
	IndexSortFields []string `json:"indexSortFields" validate:"required"`
	// The sortOrders for each index to be used by elasticsearch search api.
	SortOrders []string `json:"sortOrders" validate:"required"`
	// The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10000`.
	BatchSize int `json:"batchSize" default:"1000"`
	// This period is used by workers to poll for new data at regular intervals.
	PollingPeriod time.Duration `json:"pollingPeriod" default:"5s"`
}

func (c Config) GetHost() string {
	return c.Host
}

func (c Config) GetUsername() string {
	return c.Username
}

func (c Config) GetPassword() string {
	return c.Password
}

func (c Config) GetCloudID() string {
	return c.CloudID
}

func (c Config) GetAPIKey() string {
	return c.APIKey
}

func (c Config) GetServiceToken() string {
	return c.ServiceToken
}

func (c Config) GetCertificateFingerprint() string {
	return c.CertificateFingerprint
}

func (c Config) GetIndex() string {
	return "" // Only for Config to implement the elasticsearch/internal/config
}
