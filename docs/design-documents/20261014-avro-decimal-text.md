# Avro decimals as exact decimal text

Issue: #171. Lane: ConduitIO/conduit#3007 (v0.21: Elasticsearch/OpenSearch). Risk tier: 1 (serialization of the destination's output).

## Problem

With `sdk.schema.extract.payload.enabled` (the default), the SDK middleware decodes an Avro payload before `Write`. conduit-commons decodes an Avro `decimal` (`bytes` or `fixed`) into a `*big.Rat`. `preparePayload` marshals the structured payload with `encoding/json`, which writes a `*big.Rat` as its ratio. So `-123.456` reaches Elasticsearch as `"-15432/125"`.

- With dynamic mapping, the ratio is indexed as text. It reads like corruption, and range queries and aggregations cannot use it (invariant 6).
- With a numeric mapping (`scaled_float`, `double`), Elasticsearch rejects the item (`document_parsing_exception`). The record is nacked, so no data is lost: with the default DLQ settings the pipeline stops, and with a nack threshold above 0 the batch tail goes to the DLQ.

The `*big.Rat` is normalised and has lost the scale: `1.50` at scale 2 decodes to `3/2`.

## Constraints

- Exact. No rounding and no float on the way out.
- Deterministic. A field's text must not depend on the batch, on map order, or on a transient error.
- No change to document IDs. `Key.Bytes()` is the `_id` (for a structured key, its JSON), and changing it would split existing documents.
- No change to the SDK, conduit-commons or the connector protocol in this fix.
- The decoded value is the codec's shape: records and maps as `map[string]any`, arrays as `[]any`. A union value is either plain or wrapped as `{"<branch full name>": value}`, and which one is fixed by the schema and the value's position, not by the value:
  - the codec returns it plain when every branch has a registered Go type (null, primitives, registered logical types such as `bytes.decimal`), and wraps every non-null value otherwise;
  - commons then unwraps the values of a map and the items of an array whose union has a map or array branch (`isMapUnion`, `isArrayUnion`), and a `["null", X]` union as a record field or as map values (`isNullUnion`);
  - commons finds those positions by walking the value along the schema, and the walk cannot see inside a union value that is still wrapped where it is not a map value or array item, so positions below one keep their wrappers.
  `internal/decimaltext/unionshape.go` mirrors these rules. Reading the value instead ("a one-key map whose key is a branch name is a wrapper") is wrong: inside an unwrapped container a user key such as `amount`, `map` or `bytes.decimal` is data. The wrapper reaching Elasticsearch as an object is a separate, older shape bug (#173); this fix keeps the shape.

## Decision

Before building the bulk request, `Write` replaces every `*big.Rat` in `Payload.After` with a string of exact decimal text, padded to the scale the Avro schema declares (`"1.50"`, `"-123.456"`, `"100"` at scale 0).

- The scale comes from the payload schema named in the record metadata (`opencdc.payload.schema.subject`/`version`). This is the same lookup the middleware made a moment earlier, and the SDK caches it. The schema text is parsed with `github.com/iskorotkov/avro/v2` (already in the module graph through commons, now a direct dependency) with a private schema cache, and memoised per subject:version.
- The value and the schema are walked together: record fields, array items, map values, named references (a decimal `fixed` declared once and referenced by name), and unions. For a union, the rules above say whether the value is wrapped. A wrapper selects its branch by the key. A plain value uses the only branch of its shape: the array branch for a slice, and for a map, the map branch or the one record branch whose field names are exactly its keys. When that leaves two candidates (a map branch and a matching record, or two matching records), its entries get no schema.
- When no scale can be read (no payload schema, or an unwrapped union value with two decimal branches of different scales), the text has the fewest exact digits (`"1.5"`). It is still exact.
- A `*big.Rat` with no finite decimal expansion is an error. The batch is not sent and nothing is acked. An Avro decimal is always unscaled/10^scale, so this does not happen for decoded Avro.
- A failed schema lookup, or a schema that does not parse on its own, fails the batch, rather than silently writing a different number of digits.
- Only `Payload.After` is rewritten. The key is left as it is.
- Records with no `*big.Rat` cost one walk and no allocation. Records with one get new maps and slices along the path to it. The caller's records are never mutated.

The code is in `internal/decimaltext` (formatting and the walk) and `destination/decimal.go` (the schema lookup and the `Write` hook). It is version-independent: the v5 to v8 clients all receive the rewritten records.

## Alternatives

1. **JSON number** (`-123.456` unquoted). Rejected. Elasticsearch maps a new JSON floating-point field as `float` (about 7 significant digits), so dynamic mapping loses precision in the index. Clients that read `_source` into a double lose it again. A string keeps `_source` exact under every mapping, and Elasticsearch still coerces it into `scaled_float`/`double` fields.
2. **Fewest exact digits, without the schema** (`"1.5"` for `1.50`). It is simpler, needs no schema lookup or Avro import, and is still exact. Rejected as the default because it drops the declared scale. The text of a money field would vary in width (`"1.5"`, `"1.25"`), keyword term queries would need the canonical form instead of the source system's form, and output would differ from what Kafka Connect users see from Debezium's `decimal.handling.mode=string`, which keeps the scale. It remains the fallback when the scale is unknown.
3. **Fix it in the SDK or commons**: decode decimals into a type that keeps its scale and marshals as text. This is the right long-term home, because every destination that JSON-encodes decoded Avro has the same bug. Rejected for this fix because it changes the decoded type for every connector and needs its own versioning discussion. Worth a follow-up issue in conduit-commons.
4. **Also rewrite decimals in the key.** Rejected. It would change `_id` for every existing document with a decimal key: upserts would create duplicates and deletes would miss. The ratio in an ID is ugly but stable and unique.

## Failure modes

| Failure | Effect | Handling |
| --- | --- | --- |
| Schema lookup fails (schema service unavailable) | Scale unknown | Batch fails with `0` written and is nacked. The SDK does not retry `Write`: with the default DLQ settings the pipeline stops and recovery redelivers the records; with `dlq.window-nack-threshold` above 0 the whole batch goes to the DLQ. Nothing is written with a different scale. |
| Record has a decimal but no payload schema metadata | Scale unknown | Fewest exact digits. This only happens in-process: over gRPC a `*big.Rat` exists only after the middleware decoded with a schema. |
| Schema does not parse on its own | No scale | **Permanent**: the batch fails the same way on every attempt, and the error says so. This can happen even though the middleware decoded with it: conduit-commons parses with the codec's package-level `DefaultSchemaCache`, so a schema that refers to a named type only another schema defines resolves there, but not in the private cache used here. Failing closed is deliberate; the cross-schema leak belongs to commons (`TestWrite_DecimalSchemaThatDoesNotParseAloneIsPermanent`). |
| Value does not fit the schema's scale (wrong schema) | Padding would round | Fewest exact digits, never rounding. |
| `*big.Rat` with a non-terminating expansion | No exact text | Batch fails (`ErrNotExact`), the error names the record index and field path. |
| Wrapped union value | Branch named by the wrapper key | That branch's scale. Shape kept (#173). |
| Plain union value with decimal branches of different scales | Branch unknown | Fewest exact digits. |
| Plain map value of a union with a map branch and a matching record, or two matching records | Branch unknown | No schema for its entries, so fewest exact digits. |
| The codec or commons changes its unwrap rules | Wrapped and plain values misread | `TestRender_CodecUnions` and `TestProperty_UnionShapes` decode real codec output and fail. Text stays exact either way. |
| commons mis-walks a wrapped union with unions below it (ConduitIO/conduit-commons#312) | commons drops a map key or leaves unions wrapped before `Write` | Not fixable here: the data is already mangled. Decimals in it stay exact; padding may be lost. |
| A shape the walk does not know (a container type other than `map[string]any`/`[]any`) | `*big.Rat` would reach `encoding/json` | The codec does not produce one. The golden bulk tests and the round-trip property test fail if it starts to. |
| Huge scale in a hostile schema | Long string | The text has scale digits. The decoder has already materialised 10^scale, so no new amplification. |
| Memo growth | Memory | Bounded at 64 schemas, then reset. |

Ack path (invariant 1): unchanged. Every new failure returns before the bulk request with `0` written. Ordering (invariant 4): unchanged.

## Upgrade and rollback

- This is an output change for existing users. A document updated after the upgrade holds `"-123.456"` where it held `"-15432/125"`. Documents that are not written again keep the ratio until the source re-snapshots or the index is reindexed. The README has an upgrade note, and the commit is marked `!` for the release notes.
- Numeric mappings start accepting records that were rejected before.
- Rollback: revert. Documents written in the meantime keep decimal text, and with dynamic mapping both forms coexist in the same `text`/`keyword` field, so a rollback does not break indexing.
- No serialized state, position or config changes.

## Observability

No new metrics. A failure surfaces as the `Write` error the SDK already logs and nacks with: record index, field path, and the value as a ratio. A schema that does not parse on its own is reported as permanent. For an operator, the visible signal of the fix is the end of `document_parsing_exception` on numeric decimal fields.

## Tests

- `internal/decimaltext`: table tests for formatting, the walk (every location, references, fallbacks) and non-mutation, `TestRender_CodecUnions`, which renders real codec output for wrapped and plain unions (including the three review probes: a fixed named `amount` used as a map key inside `array<["null", amount, map]>`, a `bytes.decimal` map key inside `map<["null", map, decimal]>`, and a `map` key inside `["null", map]`), and `TestProperty_UnionShapes`, which generates random nested schemas whose decimals spell their own scale and checks every decimal against the branch it was encoded with. A seeded property test over random precision (1 to 76) and scale (0 to precision) through the real codec, checking the text against an independent printer and checking that it parses back exactly. `FuzzFormat`.
- `TestDecimalBulkGolden`: bulk bodies through the real destination with the middleware, in `testdata/decimal/*.ndjson`.
- `TestAvroEquivalence`: the four frozen decimal cases now pin both the old ratio and the new text (`decimalChange`).
- `test/v8`: `TestWrite_AvroDecimalsAreExactText` against a real Elasticsearch 8, with keyword, `scaled_float`, `double` and dynamic mappings.
