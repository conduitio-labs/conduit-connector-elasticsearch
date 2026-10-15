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

import (
	"fmt"
	"math/big"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	cavro "github.com/conduitio/conduit-commons/schema/avro"
	"github.com/iskorotkov/avro/v2"
)

// oracle writes unscaled/10^scale as decimal text by placing the point in the
// digits of unscaled. It shares no code with Format.
func oracle(unscaled *big.Int, scale int) string {
	digits := new(big.Int).Abs(unscaled).String()
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if unscaled.Sign() < 0 {
		return "-" + digits
	}
	return digits
}

func decimalRat(unscaled *big.Int, scale int) *big.Rat {
	return new(big.Rat).SetFrac(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale)), nil))
}

// randUnscaled returns a random integer with at most precision digits,
// sometimes with all of them, sometimes with trailing zeros.
func randUnscaled(rng *rand.Rand, precision int) *big.Int {
	n := 1 + rng.IntN(precision)
	if rng.IntN(4) == 0 {
		n = precision
	}
	var b strings.Builder
	for i := 0; i < n; i++ {
		b.WriteByte("0123456789"[rng.IntN(10)])
	}
	u, _ := new(big.Int).SetString(b.String(), 10)
	if z := rng.IntN(4); z > 0 && n > z {
		// Force trailing zeros: the digits a fewest-digits printer would drop.
		u.Quo(u, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(z)), nil))
		u.Mul(u, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(z)), nil))
	}
	if rng.IntN(2) == 0 {
		u.Neg(u)
	}
	return u
}

func propertySeed(t *testing.T) uint64 {
	t.Helper()
	seed := uint64(171)
	if s := os.Getenv("DECIMALTEXT_SEED"); s != "" {
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		seed = v
	}
	t.Logf("seed %d (set DECIMALTEXT_SEED to replay)", seed)
	return seed
}

// TestProperty_AvroRoundTrip encodes random decimals over random precision and
// scale with the codec the SDK middleware uses, decodes them as the middleware
// does, renders them and checks, for bytes and fixed decimals, at the top
// level and inside an array, a map and a nullable union:
//
//   - the text is exactly the unscaled digits with the point placed at the
//     scale (an independent printer), so it has exactly scale fractional digits;
//   - parsing the text gives back the original value;
//   - without a schema, the text still parses back to the original value.
func TestProperty_AvroRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(propertySeed(t), 0)) //nolint:gosec // deterministic, replayable test input
	iterations := 2000
	if testing.Short() {
		iterations = 200
	}
	for i := 0; i < iterations; i++ {
		checkAvroRoundTrip(t, rng, i)
	}
}

// checkAvroRoundTrip runs one iteration of TestProperty_AvroRoundTrip.
func checkAvroRoundTrip(t *testing.T, rng *rand.Rand, i int) {
	t.Helper()
	precision := 1 + rng.IntN(76)
	scale := rng.IntN(precision + 1)
	unscaled := randUnscaled(rng, precision)
	want := oracle(unscaled, scale)
	value := decimalRat(unscaled, scale)

	// 32 bytes hold 76 digits plus sign.
	dec := fmt.Sprintf(`{"type":"bytes","logicalType":"decimal","precision":%d,"scale":%d}`, precision, scale)
	if rng.IntN(2) == 0 {
		dec = fmt.Sprintf(`{"type":"fixed","name":"fx","size":32,"logicalType":"decimal","precision":%d,"scale":%d}`, precision, scale)
	}
	text := `{"type":"record","name":"p","fields":[` +
		`{"name":"top","type":` + dec + `},` +
		`{"name":"arr","type":{"type":"array","items":"fx_or_dec"}},` +
		`{"name":"m","type":{"type":"map","values":"fx_or_dec"}},` +
		`{"name":"u","type":["null","fx_or_dec"]}]}`
	text = expandRef(text, dec)

	serde, err := cavro.Parse([]byte(text))
	if err != nil {
		t.Fatalf("iteration %d: parse %s: %v", i, text, err)
	}
	// Encode with the codec directly: the commons serde cannot encode a
	// decimal into a union with a named fixed, so the union value is
	// given in the codec's explicit {"<branch>": value} form. A fresh
	// codec API per iteration: the shared one reuses an encoder built
	// for an earlier iteration's "fx" at another scale
	// (ConduitIO/conduit-commons#305). Decoding, the part under test,
	// goes through the commons serde like the middleware.
	branch := "bytes.decimal"
	if strings.Contains(dec, `"fixed"`) {
		branch = "fx"
	}
	encoded, err := avro.Config{}.Freeze().Marshal(mustSchema(t, text), map[string]any{
		"top": value,
		"arr": []any{value, value},
		"m":   map[string]any{"k": value},
		"u":   map[string]any{branch: value},
	})
	if err != nil {
		t.Fatalf("iteration %d: encode %s (precision %d, scale %d): %v", i, value.RatString(), precision, scale, err)
	}
	var decoded opencdc.StructuredData
	if err := serde.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("iteration %d: decode: %v", i, err)
	}

	got, err := Render(decoded, mustSchema(t, text))
	if err != nil {
		t.Fatalf("iteration %d: render: %v", i, err)
	}
	doc := got.(opencdc.StructuredData)
	for name, v := range map[string]any{
		"top": doc["top"],
		"arr": doc["arr"].([]any)[1],
		"m":   doc["m"].(map[string]any)["k"],
		"u":   doc["u"],
	} {
		s, ok := v.(string)
		if !ok || s != want {
			t.Fatalf("iteration %d (precision %d, scale %d) %s: got %#v, want %q", i, precision, scale, name, v, want)
		}
		back, ok := new(big.Rat).SetString(s)
		if !ok || back.Cmp(value) != 0 {
			t.Fatalf("iteration %d %s: %q does not parse back to %s", i, name, s, value.RatString())
		}
	}

	bare, err := Render(decoded, nil)
	if err != nil {
		t.Fatalf("iteration %d: render without schema: %v", i, err)
	}
	s := bare.(opencdc.StructuredData)["top"].(string)
	back, ok := new(big.Rat).SetString(s)
	if !ok || back.Cmp(value) != 0 {
		t.Fatalf("iteration %d: fewest-digits %q does not parse back to %s", i, s, value.RatString())
	}
	if strings.Contains(s, ".") && strings.HasSuffix(s, "0") {
		t.Fatalf("iteration %d: fewest-digits %q has a trailing zero", i, s)
	}
}

// expandRef declares the decimal type once and references it by name, so the
// named-reference path is exercised for fixed; bytes decimals are unnamed and
// are repeated inline.
func expandRef(text, dec string) string {
	if strings.Contains(dec, `"fixed"`) {
		// First use declares the fixed; later uses refer to it by name.
		return strings.ReplaceAll(text, `"fx_or_dec"`, `"fx"`)
	}
	return strings.ReplaceAll(text, `"fx_or_dec"`, dec)
}

// FuzzFormat checks Format against the independent printer for any unscaled
// value and scale, and that FormatExact agrees on the value.
func FuzzFormat(f *testing.F) {
	f.Add([]byte{0x87, 0x40}, false, uint8(3))
	f.Add([]byte{0x00}, true, uint8(0))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, true, uint8(40))
	f.Fuzz(func(t *testing.T, mag []byte, neg bool, scale uint8) {
		unscaled := new(big.Int).SetBytes(mag)
		if neg {
			unscaled.Neg(unscaled)
		}
		value := decimalRat(unscaled, int(scale))
		got, err := Format(value, int(scale))
		if err != nil {
			t.Fatal(err)
		}
		if want := oracle(unscaled, int(scale)); got != want {
			t.Fatalf("Format = %q, want %q", got, want)
		}
		exact, err := FormatExact(value)
		if err != nil {
			t.Fatal(err)
		}
		back, ok := new(big.Rat).SetString(exact)
		if !ok || back.Cmp(value) != 0 {
			t.Fatalf("FormatExact = %q, not %s", exact, value.RatString())
		}
	})
}
