// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package destination

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigKeyAPIKey                 = "keyAPIKey"
	ConfigKeyBulkSize               = "keyBulkSize"
	ConfigKeyCertificateFingerprint = "keyCertificateFingerprint"
	ConfigKeyCloudID                = "keyCloudID"
	ConfigKeyHost                   = "keyHost"
	ConfigKeyIndex                  = "keyIndex"
	ConfigKeyPassword               = "keyPassword"
	ConfigKeyRetries                = "keyRetries"
	ConfigKeyServiceToken           = "keyServiceToken"
	ConfigKeyType                   = "keyType"
	ConfigKeyUsername               = "keyUsername"
	ConfigKeyVersion                = "keyVersion"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigKeyAPIKey: {
			Default:     "",
			Description: "Base64-encoded token for authorization; if set, overrides username/password and service token.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyBulkSize: {
			Default:     "1000",
			Description: "The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10 000`.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigKeyCertificateFingerprint: {
			Default:     "",
			Description: "SHA256 hex fingerprint given by Elasticsearch on first launch.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyCloudID: {
			Default:     "",
			Description: "Endpoint for the Elastic Service (https://elastic.co/cloud).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyHost: {
			Default:     "",
			Description: "The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigKeyIndex: {
			Default:     "",
			Description: "The name of the index to write the data to.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyPassword: {
			Default:     "",
			Description: "The password for HTTP Basic Authentication.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyRetries: {
			Default:     "0",
			Description: "The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigKeyServiceToken: {
			Default:     "",
			Description: "Service token for authorization; if set, overrides username/password.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyType: {
			Default:     "",
			Description: "The name of the index's type to write the data to.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyUsername: {
			Default:     "",
			Description: "The username for HTTP Basic Authentication.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigKeyVersion: {
			Default:     "",
			Description: "The version of the Elasticsearch service. One of: 5, 6, 7, 8.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}