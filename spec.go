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

package elasticsearch

import (
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/miquido/conduit-connector-elasticsearch/destination"
	"github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch"
)

// version is set during the build process (i.e. the Makefile).
// It follows Go's convention for module version, where the version
// starts with the letter v, followed by a semantic version.
var version = "v0.0.0-dev"

func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "elasticsearch",
		Summary:     "An Elasticsearch destination plugin for Conduit.",
		Description: "The Conduit plugin supporting Elasticsearch destination.",
		Version:     version,
		Author:      "Miquido",
		DestinationParams: map[string]sdk.Parameter{
			destination.ConfigKeyVersion: {
				Default:  "",
				Required: true,
				Description: fmt.Sprintf(
					"The version of the Elasticsearch service. One of: %s, %s, %s, %s",
					elasticsearch.Version5,
					elasticsearch.Version6,
					elasticsearch.Version7,
					elasticsearch.Version8,
				),
			},
			destination.ConfigKeyHost: {
				Default:     "",
				Required:    true,
				Description: "The Elasticsearch host and port (e.g.: http://127.0.0.1:9200).",
			},
			destination.ConfigKeyUsername: {
				Default:     "",
				Required:    false,
				Description: "The username for HTTP Basic Authentication.",
			},
			destination.ConfigKeyPassword: {
				Default:     "",
				Required:    false,
				Description: "The password for HTTP Basic Authentication.",
			},
			destination.ConfigKeyCloudID: {
				Default:     "",
				Required:    false,
				Description: "Endpoint for the Elastic Service (https://elastic.co/cloud).",
			},
			destination.ConfigKeyAPIKey: {
				Default:     "",
				Required:    false,
				Description: "Base64-encoded token for authorization; if set, overrides username/password and service token.",
			},
			destination.ConfigKeyServiceToken: {
				Default:     "",
				Required:    false,
				Description: "Service token for authorization; if set, overrides username/password.",
			},
			destination.ConfigKeyCertificateFingerprint: {
				Default:     "",
				Required:    false,
				Description: "SHA256 hex fingerprint given by Elasticsearch on first launch.",
			},
			destination.ConfigKeyIndex: {
				Default:     "",
				Required:    true,
				Description: "The name of the index to write the data to.",
			},
			destination.ConfigKeyType: {
				Default:     "",
				Required:    false,
				Description: "The name of the index's type to write the data to.",
			},
			destination.ConfigKeyBulkSize: {
				Default:     "1000",
				Required:    true,
				Description: "The number of items stored in bulk in the index. The minimum value is `1`, maximum value is `10 000`.",
			},
			destination.ConfigKeyRetries: {
				Default:     "0",
				Required:    false,
				Description: "The maximum number of retries of failed operations. The minimum value is `0` which disabled retry logic. The maximum value is `255.",
			},
		},
		SourceParams: map[string]sdk.Parameter{
			//
		},
	}
}
