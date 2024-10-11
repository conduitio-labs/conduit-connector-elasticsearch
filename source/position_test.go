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

package source

import (
	"errors"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestNewPosition(t *testing.T) {
	is := is.New(t)
	pos := NewPosition()
	is.True(pos != nil)
	is.Equal(len(pos.IndexPositions), 0)
}

func TestParseSDKPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      opencdc.Position
		wantPos *Position
		wantErr error
	}{
		{
			name: "failure_unmarshal_error",
			in:   opencdc.Position("invalid"),
			wantErr: errors.New("unmarshal opencdc.Position into Position: " +
				"invalid character 'i' looking for beginning of value"),
		},
		{
			name: "success_no_error",
			in: opencdc.Position(`{
					"indexPositions": {
						"index": 10
					}
				}`),
			wantPos: &Position{IndexPositions: map[string]int{"index": 10}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := ParseSDKPosition(tt.in)
			if tt.wantErr == nil {
				is.NoErr(err)
				is.Equal(got, tt.wantPos)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.wantErr.Error())
			}
		})
	}
}

func TestMarshal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   *Position
		want opencdc.Position
	}{
		{
			name: "successful marshal",
			in:   &Position{IndexPositions: map[string]int{"a": 1, "b": 2}},
			want: []byte(`{"indexPositions":{"a":1,"b":2}}`),
		},
		{
			name: "marshal empty map",
			in:   &Position{IndexPositions: map[string]int{}},
			want: []byte(`{"indexPositions":{}}`),
		},
		{
			name: "marshal with nil map",
			in:   &Position{IndexPositions: nil},
			want: []byte(`{"indexPositions":null}`),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			got, err := tt.in.marshal()
			is.NoErr(err)
			is.Equal(got, tt.want)
		})
	}
}

func TestUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		in          *Position
		updateIndex string
		updatePos   int
		want        *Position
	}{
		{
			name:        "update existing index",
			in:          &Position{IndexPositions: map[string]int{"a": 1, "b": 2}},
			updateIndex: "a",
			updatePos:   10,
			want:        &Position{IndexPositions: map[string]int{"a": 10, "b": 2}},
		},
		{
			name:        "update new index",
			in:          &Position{IndexPositions: map[string]int{"a": 1}},
			updateIndex: "b",
			updatePos:   5,
			want:        &Position{IndexPositions: map[string]int{"a": 1, "b": 5}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			is := is.New(t)

			tt.in.update(tt.updateIndex, tt.updatePos)
			is.Equal(tt.in, tt.want)
		})
	}
}
