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
	"testing"

	"github.com/jaswdr/faker"
	v5 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v5"
	v6 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v6"
	v7 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v7"
	v8 "github.com/miquido/conduit-connector-elasticsearch/internal/elasticsearch/v8"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Fails for unsupported version", func(t *testing.T) {
		var (
			config = map[string]interface{}{
				fakerInstance.Lorem().Word(): fakerInstance.Int(),
			}
			version = fakerInstance.Lorem().Sentence(3)
		)

		_, err := NewClient(version, config)

		require.EqualError(t, err, fmt.Sprintf("unsupported version: %s", version))
	})

	t.Run(fmt.Sprintf("Client for version %s can be created", Version5), func(t *testing.T) {
		var (
			config = map[string]interface{}{
				fakerInstance.Lorem().Word(): fakerInstance.Int(),
			}
			clientMock = new(v5.Client)
		)

		v5ClientBuilder = func(cfg interface{}) (*v5.Client, error) {
			require.Equal(t, config, cfg)

			return clientMock, nil
		}

		client, err := NewClient(Version5, config)

		require.NoError(t, err)
		require.Same(t, clientMock, client)
	})

	t.Run(fmt.Sprintf("Client for version %s can be created", Version6), func(t *testing.T) {
		var (
			config = map[string]interface{}{
				fakerInstance.Lorem().Word(): fakerInstance.Int(),
			}
			clientMock = new(v6.Client)
		)

		v6ClientBuilder = func(cfg interface{}) (*v6.Client, error) {
			require.Equal(t, config, cfg)

			return clientMock, nil
		}

		client, err := NewClient(Version6, config)

		require.NoError(t, err)
		require.Same(t, clientMock, client)
	})

	t.Run(fmt.Sprintf("Client for version %s can be created", Version7), func(t *testing.T) {
		var (
			config = map[string]interface{}{
				fakerInstance.Lorem().Word(): fakerInstance.Int(),
			}
			clientMock = new(v7.Client)
		)

		v7ClientBuilder = func(cfg interface{}) (*v7.Client, error) {
			require.Equal(t, config, cfg)

			return clientMock, nil
		}

		client, err := NewClient(Version7, config)

		require.NoError(t, err)
		require.Same(t, clientMock, client)
	})

	t.Run(fmt.Sprintf("Client for version %s can be created", Version8), func(t *testing.T) {
		var (
			config = map[string]interface{}{
				fakerInstance.Lorem().Word(): fakerInstance.Int(),
			}
			clientMock = new(v8.Client)
		)

		v8ClientBuilder = func(cfg interface{}) (*v8.Client, error) {
			require.Equal(t, config, cfg)

			return clientMock, nil
		}

		client, err := NewClient(Version8, config)

		require.NoError(t, err)
		require.Same(t, clientMock, client)
	})
}
