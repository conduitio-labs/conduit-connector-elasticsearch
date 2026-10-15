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

package decimaltext

// Which union values reach the destination wrapped as
// {"<branch full name>": value} and which come back plain is decided by two
// layers, mirrored here from github.com/iskorotkov/avro/v2 v2.34.0 and
// github.com/conduitio/conduit-commons v0.8.0 (schema/avro/union.go):
//
//  1. The codec decodes a union into any as a plain value when every branch
//     resolves to a registered Go type (null, the primitives and their
//     registered logical types, such as bytes.decimal). If any branch does
//     not (a record, enum, fixed, map or array), every non-null value is
//     wrapped.
//  2. commons' AfterUnmarshal then unwraps, from wrappers collected before
//     any is removed:
//     - the values of a map whose value union has a map or array branch
//       (isMapUnion), and the items of an array whose item union does
//       (isArrayUnion);
//     - a union of null and one other type (isNullUnion) as a record field
//       or as map values.
//     It finds those positions by walking the decoded value along the
//     schema. The walk steps through a union that is a map value or array
//     item by taking the wrapper's single entry, and passes any other
//     union's value through unchanged, so it cannot see inside a value that
//     was still wrapped there: positions below such a union keep their
//     wrappers.
//
// Getting this wrong only costs padding or picks another branch's scale; the
// text stays exact. The tests in codec_union_test.go decode real codec output
// for every rule, so a change in either module shows up there.

import "github.com/iskorotkov/avro/v2"

type positionKind int

const (
	top positionKind = iota
	recordField
	mapValue
	arrayItem
)

// position is where a value sits: what kind of container holds it, and
// whether commons' walk reaches it.
type position struct {
	kind positionKind
	// reached is false below a union whose value the walk cannot see into.
	reached bool
}

var root = position{kind: top, reached: true}

// child is the position of the entries of a container at p.
func (p position) child(kind positionKind) position {
	return position{kind: kind, reached: p.reached}
}

// throughUnion is the position of a union's branch value, as seen by the
// containers inside it.
func (p position) throughUnion(u *avro.UnionSchema) position {
	if p.kind != mapValue && p.kind != arrayItem && !resolvable(u) {
		// commons' walk passed the wrapper through as the branch value.
		p.reached = false
	}
	return p
}

// wrapped reports whether a non-null value of u at p is still a wrapper.
func wrapped(u *avro.UnionSchema, at position) bool {
	if resolvable(u) {
		return false // the codec returned it plain
	}
	if !at.reached {
		return true
	}
	switch at.kind {
	case mapValue:
		return !hasContainerBranch(u) && !isNullUnion(u)
	case arrayItem:
		return !hasContainerBranch(u)
	case recordField:
		return !isNullUnion(u)
	case top:
		return true
	}
	return true
}

// resolvable mirrors the codec: every branch's name is one its default
// TypeResolver registers (resolver.go, NewTypeResolver). Named types, maps and
// arrays never are.
func resolvable(u *avro.UnionSchema) bool {
	for _, b := range u.Types() {
		if !registeredNames[branchName(b)] {
			return false
		}
	}
	return true
}

// bytesDecimal is the codec's name for a bytes decimal branch.
const bytesDecimal = "bytes.decimal"

var registeredNames = map[string]bool{
	"null": true, "int": true, "long": true, "float": true, "double": true,
	"string": true, "bytes": true, "boolean": true,
	"int.date":                    true,
	"int.time-millis":             true,
	"long.time-micros":            true,
	"long.timestamp-millis":       true,
	"long.timestamp-micros":       true,
	"long.timestamp-nanos":        true,
	"long.local-timestamp-millis": true,
	"long.local-timestamp-micros": true,
	"long.local-timestamp-nanos":  true,
	bytesDecimal:                  true,
	"string.uuid":                 true,
}

// hasContainerBranch mirrors isMapUnion/isArrayUnion for the union u that is
// the map's value or array's item type: it has a map or array branch.
func hasContainerBranch(u *avro.UnionSchema) bool {
	for _, b := range u.Types() {
		if t := b.Type(); t == avro.Array || t == avro.Map {
			return true
		}
	}
	return false
}

// isNullUnion mirrors commons: two branches, one of them null.
func isNullUnion(u *avro.UnionSchema) bool {
	if len(u.Types()) != 2 {
		return false
	}
	for _, b := range u.Types() {
		if b.Type() == avro.Null {
			return true
		}
	}
	return false
}

// branchName is the name the codec gives a union branch in a wrapper: the
// full name of a named type, otherwise the type, with ".<logicalType>" for a
// primitive with a logical type ("bytes.decimal", "map", "ns.money").
func branchName(sch avro.Schema) string {
	sch = deref(sch)
	if n, ok := sch.(avro.NamedSchema); ok {
		return n.FullName()
	}
	name := string(sch.Type())
	if p, ok := sch.(*avro.PrimitiveSchema); ok && p.Logical() != nil {
		name += "." + string(p.Logical().Type())
	}
	return name
}

// branchNamed returns the branch of u a wrapper key names, or nil.
func branchNamed(u *avro.UnionSchema, name string) avro.Schema {
	for _, b := range u.Types() {
		if branchName(b) == name {
			return deref(b)
		}
	}
	return nil
}
