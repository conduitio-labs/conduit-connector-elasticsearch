// Copyright © 2026 Meroxa, Inc. and Miquido
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

package elasticsearch_test

import (
	"context"
	"strings"
	"testing"

	elasticsearch "github.com/conduitio-labs/conduit-connector-elasticsearch"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// TestDestinationConfig_RejectedAtConfigure checks that an invalid
// configuration fails when the SDK parses it, before Open, as it did on SDK
// v0.12. With v0.15 an invalid sdk.record.format slipped through:
// DefaultDestinationMiddleware.Validate skips DestinationWithRecordFormat,
// whose Validate has a pointer receiver, and Open drops the error.
func TestDestinationConfig_RejectedAtConfigure(t *testing.T) {
	base := map[string]string{"version": "8", "host": "http://127.0.0.1:1", "index": "idx"}
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string // empty: valid
	}{
		{"valid", "", "", ""},
		{"record format", "sdk.record.format", "nope", `invalid sdk.record.format: "nope" not found`},
		{"index template", "index", "{{ .Broken", "invalid index name or index function"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := map[string]string{}
			for k, v := range base {
				cfg[k] = v
			}
			if tc.key != "" {
				cfg[tc.key] = tc.value
			}
			dest := elasticsearch.Connector.NewDestination()
			err := sdk.Util.ParseConfig(context.Background(), cfg, dest.Config(), elasticsearch.Connector.NewSpecification().DestinationParams)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}
