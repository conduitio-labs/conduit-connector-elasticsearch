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
	"testing"

	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestConfig_Getters(t *testing.T) {
	fakerInstance := faker.New()

	var (
		host                   = fakerInstance.Internet().URL()
		username               = fakerInstance.Internet().Email()
		password               = fakerInstance.Internet().Password()
		cloudID                = fakerInstance.RandomStringWithLength(32)
		apiKey                 = fakerInstance.RandomStringWithLength(32)
		serviceToken           = fakerInstance.RandomStringWithLength(32)
		certificateFingerprint = fakerInstance.Hash().SHA256()
		indexName              = fakerInstance.Lorem().Word()
		indexType              = fakerInstance.Lorem().Word()
	)

	config := Config{
		Host:                   host,
		Username:               username,
		Password:               password,
		CloudID:                cloudID,
		APIKey:                 apiKey,
		ServiceToken:           serviceToken,
		CertificateFingerprint: certificateFingerprint,
		Index:                  indexName,
		Type:                   indexType,
	}

	require.Equal(t, host, config.GetHost())
	require.Equal(t, username, config.GetUsername())
	require.Equal(t, password, config.GetPassword())
	require.Equal(t, cloudID, config.GetCloudID())
	require.Equal(t, apiKey, config.GetAPIKey())
	require.Equal(t, serviceToken, config.GetServiceToken())
	require.Equal(t, certificateFingerprint, config.GetCertificateFingerprint())
	require.Equal(t, indexName, config.GetIndex())
	require.Equal(t, indexType, config.GetType())
}
