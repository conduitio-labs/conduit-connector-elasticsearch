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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/jaswdr/faker"
	"github.com/stretchr/testify/require"
)

func TestNewDestination(t *testing.T) {
	t.Run("New Destination can be created", func(t *testing.T) {
		require.IsType(t, &Destination{}, NewDestination())
	})
}

func TestDestination_GetClient(t *testing.T) {
	clientMock := &clientMock{}

	destination := Destination{
		client: clientMock,
	}

	require.Same(t, clientMock, destination.GetClient())
}

func TestDestination_Write(t *testing.T) {
	fakerInstance := faker.New()

	t.Run("Does not perform any action when queue is empty", func(t *testing.T) {
		esClientMock := clientMock{}

		destination := Destination{
			config: Config{},
			client: &esClientMock,
		}

		n, err := destination.Write(context.Background(), nil)
		require.Equal(t, 0, n)
		require.NoError(t, err)
		require.Len(t, esClientMock.PrepareCreateOperationCalls(), 0)
		require.Len(t, esClientMock.PrepareUpsertOperationCalls(), 0)
		require.Len(t, esClientMock.PrepareDeleteOperationCalls(), 0)
		require.Len(t, esClientMock.BulkCalls(), 0)
	})

	t.Run("Does not retry when there were no failures", func(t *testing.T) {
		var (
			operationMetadata = fakerInstance.Lorem().Sentence(6)
			operationPayload  = fakerInstance.Lorem().Sentence(6)
		)

		esClientMock := clientMock{
			PrepareCreateOperationFunc: func(item sdk.Record) (interface{}, interface{}, error) {
				return operationMetadata, operationPayload, nil
			},

			BulkFunc: func(ctx context.Context, reader io.Reader) (io.ReadCloser, error) {
				bulkRequest, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%q\n%q\n", operationMetadata, operationPayload), string(bulkRequest))

				data, err := json.Marshal(bulkResponse{
					Took:   0,
					Errors: false,
					Items: []bulkResponseItems{
						{
							Create: &bulkResponseItem{
								Status: http.StatusOK,
							},
						},
					},
				})
				require.NoError(t, err)

				return io.NopCloser(bytes.NewReader(data)), nil
			},
		}

		destination := Destination{
			config: Config{
				BulkSize: 1,
				Retries:  2,
			},
			client: &esClientMock,
		}

		n, err := destination.Write(
			context.Background(),
			[]sdk.Record{
				sdk.SourceUtil{}.NewRecordCreate(
					nil,
					nil,
					nil,
					sdk.StructuredData{
						"id": fakerInstance.Int32(),
					},
				),
			},
		)
		require.Equal(t, 1, n)
		require.NoError(t, err)
		require.Len(t, esClientMock.PrepareCreateOperationCalls(), 1)
		require.Len(t, esClientMock.PrepareUpsertOperationCalls(), 0)
		require.Len(t, esClientMock.PrepareDeleteOperationCalls(), 0)
		require.Len(t, esClientMock.BulkCalls(), 1)
	})
}
