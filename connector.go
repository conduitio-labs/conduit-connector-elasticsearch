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

//go:generate conn-sdk-cli specgen

package elasticsearch

import (
	_ "embed"

	"github.com/conduitio/conduit-connector-elasticsearch/destination"
	"github.com/conduitio/conduit-connector-elasticsearch/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

//go:embed connector.yaml
var specs string

// version is set at build time (see Makefile and .goreleaser.yml).
var version = "v0.0.0-dev"

var Connector = sdk.Connector{
	NewSpecification: sdk.YAMLSpecification(specs, version),
	NewSource:        source.NewSource,
	NewDestination:   destination.NewDestination,
}
