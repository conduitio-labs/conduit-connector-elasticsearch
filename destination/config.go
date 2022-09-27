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

package destination

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch"
)

const (
	ConfigKeyVersion                = "version"
	ConfigKeyHost                   = "host"
	ConfigKeyUsername               = "username"
	ConfigKeyPassword               = "password"
	ConfigKeyCloudID                = "cloudId"
	ConfigKeyAPIKey                 = "apiKey"
	ConfigKeyServiceToken           = "serviceToken"
	ConfigKeyCertificateFingerprint = "certificateFingerprint"
	ConfigKeyIndex                  = "index"
	ConfigKeyType                   = "type"
	ConfigKeyBulkSize               = "bulkSize"
	ConfigKeyRetries                = "retries"
)

type Config struct {
	Version                elasticsearch.Version
	Host                   string
	Username               string
	Password               string
	CloudID                string
	APIKey                 string
	ServiceToken           string
	CertificateFingerprint string
	Index                  string
	Type                   string
	BulkSize               uint64
	Retries                uint8
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
