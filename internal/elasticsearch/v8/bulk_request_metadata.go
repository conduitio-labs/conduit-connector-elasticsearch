// Copyright © 2022 Meroxa, Inc.
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

package v8

// See: https://www.elastic.co/guide/en/elasticsearch/reference/8.2/docs-bulk.html
type bulkRequestActionAndMetadata struct {
	Create *bulkRequestCreateAction `json:"create,omitempty"`
	Update *bulkRequestUpdateAction `json:"update,omitempty"`
	Delete *bulkRequestDeleteAction `json:"delete,omitempty"`
}

type bulkRequestCreateAction struct {
	Index string `json:"_index"`
}

type bulkRequestUpdateAction struct {
	ID              string `json:"_id"`
	Index           string `json:"_index"`
	RetryOnConflict int    `json:"retry_on_conflict"`
}

type bulkRequestDeleteAction struct {
	ID    string `json:"_id"`
	Index string `json:"_index"`
}
