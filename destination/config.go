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

//go:generate paramgen -output=paramgen.go Config

package destination

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/conduitio-labs/conduit-connector-elasticsearch/internal/elasticsearch"
)

type Config struct {
	// The version of the Elasticsearch service. One of: 5, 6, 7, 8.
	Version elasticsearch.Version `json:"keyVersion" validate:"required"`
	// The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).
	Host string `json:"keyHost" validate:"required"`
	// The username for HTTP Basic Authentication.
	Username string `json:"keyUsername"`
	// The password for HTTP Basic Authentication.
	Password string `json:"keyPassword"`
	// Endpoint for the Elastic Service (https://elastic.co/cloud).
	CloudID string `json:"keyCloudID"`
	// Base64-encoded token for authorization; if set, overrides username/password and service token.
	APIKey string `json:"keyAPIKey"`
	// Service token for authorization; if set, overrides username/password.
	ServiceToken string `json:"keyServiceToken"`
	// SHA256 hex fingerprint given by Elasticsearch on first launch.
	CertificateFingerprint string `json:"keyCertificateFingerprint"`
	// The name of the index to write the data to.
	Index string `json:"keyIndex"`
	// The name of the index's type to write the data to.
	Type string `json:"keyType"`
	// The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10 000`.
	BulkSize uint64 `json:"keyBulkSize" default:"1000"`
	// The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255.
	Retries uint8 `json:"keyRetries" default:"0"`
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
	return c.Index
}

func (c Config) GetType() string {
	return c.Type
}

func ParseConfig(cfgRaw map[string]string) (_ Config, err error) {
	cfg := Config{
		Version:                cfgRaw[ConfigKeyVersion],
		Host:                   cfgRaw[ConfigKeyHost],
		Username:               cfgRaw[ConfigKeyUsername],
		Password:               cfgRaw[ConfigKeyPassword],
		CloudID:                cfgRaw[ConfigKeyCloudID],
		APIKey:                 cfgRaw[ConfigKeyAPIKey],
		ServiceToken:           cfgRaw[ConfigKeyServiceToken],
		CertificateFingerprint: cfgRaw[ConfigKeyCertificateFingerprint],
		Index:                  cfgRaw[ConfigKeyIndex],
		Type:                   cfgRaw[ConfigKeyType],
	}

	if cfg.Version == "" {
		return Config{}, requiredConfigErr(ConfigKeyVersion)
	}
	if cfg.Version != elasticsearch.Version5 &&
		cfg.Version != elasticsearch.Version6 &&
		cfg.Version != elasticsearch.Version7 &&
		cfg.Version != elasticsearch.Version8 {
		return Config{}, fmt.Errorf(
			"%q config value must be one of [%s], %s provided",
			ConfigKeyVersion,
			strings.Join([]elasticsearch.Version{
				elasticsearch.Version5,
				elasticsearch.Version6,
				elasticsearch.Version7,
				elasticsearch.Version8,
			}, ", "),
			cfg.Version,
		)
	}

	if cfg.Host == "" {
		return Config{}, requiredConfigErr(ConfigKeyHost)
	}

	if cfg.Username == "" && cfg.Password != "" {
		return Config{}, fmt.Errorf("%q config value must be set when %q is provided", ConfigKeyUsername, ConfigKeyPassword)
	}

	if cfg.Index == "" {
		return Config{}, requiredConfigErr(ConfigKeyIndex)
	}

	if (cfg.Version == elasticsearch.Version5 || cfg.Version == elasticsearch.Version6) && cfg.Type == "" {
		return Config{}, requiredConfigErr(ConfigKeyType)
	}

	// Bulk size
	if cfg.BulkSize, err = parseBulkSizeConfigValue(cfgRaw); err != nil {
		return Config{}, err
	}

	// Retries
	if cfg.Retries, err = parseRetriesConfigValue(cfgRaw); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func requiredConfigErr(name string) error {
	return fmt.Errorf("%q config value must be set", name)
}

func parseBulkSizeConfigValue(cfgRaw map[string]string) (uint64, error) {
	bulkSize, ok := cfgRaw[ConfigKeyBulkSize]
	if !ok {
		return 0, requiredConfigErr(ConfigKeyBulkSize)
	}

	bulkSizeParsed, err := strconv.ParseUint(bulkSize, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %q config value: %w", ConfigKeyBulkSize, err)
	}
	if bulkSizeParsed <= 0 {
		return 0, fmt.Errorf("failed to parse %q config value: value must be greater than 0", ConfigKeyBulkSize)
	}
	if bulkSizeParsed > 10_000 {
		return 0, fmt.Errorf("failed to parse %q config value: value must not be grater than 10 000", ConfigKeyBulkSize)
	}

	return bulkSizeParsed, nil
}

func parseRetriesConfigValue(cfgRaw map[string]string) (uint8, error) {
	retries, ok := cfgRaw[ConfigKeyRetries]
	if !ok || retries == "" {
		return 0, nil
	}

	retriesParsed, err := strconv.ParseUint(retries, 10, 8)
	if err != nil {
		return 0, fmt.Errorf("failed to parse %q config value: %w", ConfigKeyRetries, err)
	}

	return uint8(retriesParsed), nil
}
