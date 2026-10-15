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

package destination

import "encoding/json"

type bulkResponse struct {
	Took   int                 `json:"took"`
	Errors bool                `json:"errors"`
	Items  []bulkResponseItems `json:"items"`
}

type bulkResponseItems struct {
	Index  *bulkResponseItem `json:"index,omitempty"`
	Create *bulkResponseItem `json:"create,omitempty"`
	Update *bulkResponseItem `json:"update,omitempty"`
	Delete *bulkResponseItem `json:"delete,omitempty"`
}

type bulkResponseItem struct {
	ID     string                 `json:"_id"`
	Status int                    `json:"status"`
	Error  *bulkResponseItemError `json:"error,omitempty"`
}

type bulkResponseItemError struct {
	Type     string          `json:"type"`
	Reason   string          `json:"reason"`
	CausedBy json.RawMessage `json:"caused_by"`
}
