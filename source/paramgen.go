// Code generated by paramgen. DO NOT EDIT.
// Source: github.com/ConduitIO/conduit-commons/tree/main/paramgen

package source

import (
	"github.com/conduitio/conduit-commons/config"
)

const (
	ConfigAPIKey                 = "APIKey"
	ConfigBatchSize              = "batchSize"
	ConfigCertificateFingerprint = "certificateFingerprint"
	ConfigCloudID                = "cloudID"
	ConfigHost                   = "host"
	ConfigIndexesSortBy          = "indexes.*.sortBy"
	ConfigIndexesSortOrder       = "indexes.*.sortOrder"
	ConfigPassword               = "password"
	ConfigPollingPeriod          = "pollingPeriod"
	ConfigRetries                = "retries"
	ConfigServiceToken           = "serviceToken"
	ConfigUsername               = "username"
	ConfigVersion                = "version"
)

func (Config) Parameters() map[string]config.Parameter {
	return map[string]config.Parameter{
		ConfigAPIKey: {
			Default:     "",
			Description: "Base64-encoded token for authorization; if set, overrides username/password and service token.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigBatchSize: {
			Default:     "1000",
			Description: "The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10000`.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigCertificateFingerprint: {
			Default:     "",
			Description: "SHA256 hex fingerprint given by Elasticsearch on first launch.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigCloudID: {
			Default:     "",
			Description: "Endpoint for the Elastic Service (https://elastic.co/cloud).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigHost: {
			Default:     "",
			Description: "The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
		ConfigIndexesSortBy: {
			Default:     "_seq_no",
			Description: "The sortby field for each index to be used by elasticsearch search api.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigIndexesSortOrder: {
			Default:     "asc",
			Description: "The sortOrder(asc or desc) for each index to be used by elasticsearch search api.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigPassword: {
			Default:     "",
			Description: "The password for HTTP Basic Authentication.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigPollingPeriod: {
			Default:     "5s",
			Description: "This period is used by workers to poll for new data at regular intervals.",
			Type:        config.ParameterTypeDuration,
			Validations: []config.Validation{},
		},
		ConfigRetries: {
			Default:     "0",
			Description: "The maximum number of retries of failed operations.",
			Type:        config.ParameterTypeInt,
			Validations: []config.Validation{},
		},
		ConfigServiceToken: {
			Default:     "",
			Description: "Service token for authorization; if set, overrides username/password.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigUsername: {
			Default:     "",
			Description: "The username for HTTP Basic Authentication.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{},
		},
		ConfigVersion: {
			Default:     "",
			Description: "The version of the Elasticsearch service. One of: 5, 6, 7, 8.",
			Type:        config.ParameterTypeString,
			Validations: []config.Validation{
				config.ValidationRequired{},
			},
		},
	}
}
