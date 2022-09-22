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
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

type operation struct {
	CreatedAt time.Time
	Record    sdk.Record
	err       error
}

type BufferQueue []*operation

// Empty returns whether queue is empty or not.
func (bq BufferQueue) Empty() bool {
	return len(bq) == 0
}

// Len returns the current number of elements in queue.
func (bq BufferQueue) Len() int {
	return len(bq)
}

// Enqueue adds registers a new operation in queue.
func (bq *BufferQueue) Enqueue(item *operation) {
	*bq = append(*bq, item)
}
