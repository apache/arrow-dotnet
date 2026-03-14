// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Text.Json;
using Apache.Arrow.Variant.Json;
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantSqlDecimalTests
    {
        // Minimal metadata: version 1, 0 dictionary entries.
        private static readonly byte[] MinimalMetadata = new byte[] { 0x01, 0x00 };

        // Header bytes: (primitiveType << 2) | basicType, basicType=0 for Primitive
        // Decimal4=8 -> 0x20, Decimal8=9 -> 0x24, Decimal16=10 -> 0x28
        private const byte Decimal16Header = 0x28;

        // ---------------------------------------------------------------
        // GetSqlDecimal — Decimal4
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_Decimal4()
        {
            VariantValue vv = VariantValue.FromDecimal4(123.45m);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(new SqlDecimal(123.45m), result);
        }

        // ---------------------------------------------------------------
        // GetSqlDecimal — Decimal8
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_Decimal8()
        {
            VariantValue vv = VariantValue.FromDecimal8(123456.789m);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(new SqlDecimal(123456.789m), result);
        }

        // ---------------------------------------------------------------
        // GetSqlDecimal — Decimal16 (fits in decimal)
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_Decimal16_FitsInDecimal()
        {
            decimal d = 79228162514264337593543950335m; // decimal.MaxValue
            VariantValue vv = VariantValue.FromDecimal16(d);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(new SqlDecimal(d), result);
        }

        // ---------------------------------------------------------------
        // GetSqlDecimal — Decimal16 (exceeds decimal)
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_Decimal16_ExceedsDecimal()
        {
            // Value = 2^96 = 79228162514264337593543950336
            // In 128-bit LE: lo=0, hi=0x0000000100000000 (bit 96 set)
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lo = 0
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00  // hi = 0x0000000100000000
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            SqlDecimal result = reader.GetSqlDecimal();
            SqlDecimal expected = SqlDecimal.Parse("79228162514264337593543950336");
            Assert.Equal(expected, result);
        }

        // ---------------------------------------------------------------
        // TryGetDecimal16 — returns true for small values
        // ---------------------------------------------------------------

        [Fact]
        public void TryGetDecimal16_SmallValue_ReturnsTrue()
        {
            decimal d = 12345.67m;
            VariantValue vv = VariantValue.FromDecimal16(d);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            bool success = reader.TryGetDecimal16(out decimal result);
            Assert.True(success);
            Assert.Equal(d, result);
        }

        // ---------------------------------------------------------------
        // TryGetDecimal16 — returns false for large values
        // ---------------------------------------------------------------

        [Fact]
        public void TryGetDecimal16_LargeValue_ReturnsFalse()
        {
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            bool success = reader.TryGetDecimal16(out decimal _);
            Assert.False(success);
        }

        // ---------------------------------------------------------------
        // FromSqlDecimal — small values produce Decimal4/8
        // ---------------------------------------------------------------

        [Fact]
        public void FromSqlDecimal_SmallValue_ProducesDecimal4()
        {
            SqlDecimal sd = new SqlDecimal(42.5m);
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(VariantPrimitiveType.Decimal4, vv.PrimitiveType);
            Assert.Equal(42.5m, vv.AsDecimal());
        }

        [Fact]
        public void FromSqlDecimal_MediumValue_ProducesDecimal8()
        {
            // A value that needs more than 32 bits unscaled but fits in 64 bits
            SqlDecimal sd = new SqlDecimal(12345678901.23m);
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(VariantPrimitiveType.Decimal8, vv.PrimitiveType);
            Assert.Equal(12345678901.23m, vv.AsDecimal());
        }

        // ---------------------------------------------------------------
        // FromSqlDecimal — large values produce Decimal16
        // ---------------------------------------------------------------

        [Fact]
        public void FromSqlDecimal_LargeValue_ProducesDecimal16()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(VariantPrimitiveType.Decimal16, vv.PrimitiveType);
            Assert.True(vv.IsSqlDecimalStorage);
        }

        // ---------------------------------------------------------------
        // AsSqlDecimal — from decimal-stored and SqlDecimal-stored values
        // ---------------------------------------------------------------

        [Fact]
        public void AsSqlDecimal_FromDecimalStored()
        {
            VariantValue vv = VariantValue.FromDecimal4(123.45m);
            SqlDecimal result = vv.AsSqlDecimal();
            Assert.Equal(new SqlDecimal(123.45m), result);
        }

        [Fact]
        public void AsSqlDecimal_FromSqlDecimalStored()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            SqlDecimal result = vv.AsSqlDecimal();
            Assert.Equal(sd, result);
        }

        // ---------------------------------------------------------------
        // AsDecimal from SqlDecimal-stored Decimal16 throws OverflowException
        // ---------------------------------------------------------------

        [Fact]
        public void AsDecimal_FromSqlDecimalStored_Throws()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Throws<OverflowException>(() => vv.AsDecimal());
        }

        // ---------------------------------------------------------------
        // Round-trip: SqlDecimal -> Encode -> Decode -> SqlDecimal
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_LargePositive()
        {
            SqlDecimal original = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(original);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            VariantReader reader = new VariantReader(metadata, value);
            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(original, result);
        }

        [Fact]
        public void RoundTrip_LargeNegative()
        {
            SqlDecimal original = SqlDecimal.Parse("-12345678901234567890123456789012345678");
            VariantValue vv = VariantValue.FromSqlDecimal(original);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            VariantReader reader = new VariantReader(metadata, value);
            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(original, result);
        }

        [Fact]
        public void RoundTrip_LargeWithScale()
        {
            SqlDecimal original = SqlDecimal.Parse("1234567890123456789012345678901234.5678");
            VariantValue vv = VariantValue.FromSqlDecimal(original);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            VariantReader reader = new VariantReader(metadata, value);
            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(original, result);
        }

        // ---------------------------------------------------------------
        // MaterializePrimitive uses SqlDecimal for large Decimal16
        // ---------------------------------------------------------------

        [Fact]
        public void MaterializePrimitive_LargeDecimal16_NoException()
        {
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            // Should not throw — should use SqlDecimal path
            VariantValue vv = reader.ToVariantValue();
            Assert.Equal(VariantPrimitiveType.Decimal16, vv.PrimitiveType);
            Assert.True(vv.IsSqlDecimalStorage);

            SqlDecimal result = vv.AsSqlDecimal();
            Assert.Equal(SqlDecimal.Parse("79228162514264337593543950336"), result);
        }

        // ---------------------------------------------------------------
        // JSON writer: large Decimal16 produces valid JSON number
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_LargeDecimal16_WritesValidJson()
        {
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
            };

            string json = VariantJsonWriter.ToJson(MinimalMetadata, value);
            Assert.Equal("79228162514264337593543950336", json);
        }

        // ---------------------------------------------------------------
        // JSON converter: VariantValue with SqlDecimal storage writes valid JSON
        // ---------------------------------------------------------------

        [Fact]
        public void JsonConverter_SqlDecimalStorage_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);

            string json = VariantJsonWriter.ToJson(vv);
            Assert.Equal("99999999999999999999999999999999999999", json);
        }

        // ---------------------------------------------------------------
        // Round-trip with materialization
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_Materialize_LargeDecimal16()
        {
            SqlDecimal original = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue vv1 = VariantValue.FromSqlDecimal(original);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv1);

            VariantReader reader = new VariantReader(metadata, value);
            VariantValue vv2 = reader.ToVariantValue();

            Assert.Equal(VariantPrimitiveType.Decimal16, vv2.PrimitiveType);
            Assert.True(vv2.IsSqlDecimalStorage);
            Assert.Equal(original, vv2.AsSqlDecimal());
        }

        [Fact]
        public void RoundTrip_Materialize_SmallDecimal16_UsesDecimalStorage()
        {
            decimal d = 12345.67m;
            VariantValue vv1 = VariantValue.FromDecimal16(d);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv1);

            VariantReader reader = new VariantReader(metadata, value);
            VariantValue vv2 = reader.ToVariantValue();

            Assert.Equal(VariantPrimitiveType.Decimal16, vv2.PrimitiveType);
            Assert.False(vv2.IsSqlDecimalStorage);
            Assert.Equal(d, vv2.AsDecimal());
        }

        // ---------------------------------------------------------------
        // Negative large values via hand-crafted binary
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_LargeNegative_HandCrafted()
        {
            // -2^96 in two's complement 128-bit:
            // Positive: lo=0x0000000000000000, hi=0x0000000100000000
            // Negate: ~lo=0xFFFFFFFFFFFFFFFF, ~hi=0xFFFFFFFEFFFFFFFF
            // +1: lo=0 (overflow carry), hi=0xFFFFFFFF00000000
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // lo = 0
                0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF  // hi = 0xFFFFFFFF00000000 LE
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            SqlDecimal result = reader.GetSqlDecimal();
            SqlDecimal expected = SqlDecimal.Parse("-79228162514264337593543950336");
            Assert.Equal(expected, result);
        }

        // ---------------------------------------------------------------
        // AsSqlDecimal throws for non-decimal types
        // ---------------------------------------------------------------

        [Fact]
        public void AsSqlDecimal_NonDecimalType_Throws()
        {
            VariantValue vv = VariantValue.FromInt32(42);
            Assert.Throws<InvalidOperationException>(() => vv.AsSqlDecimal());
        }

        // ---------------------------------------------------------------
        // Boundary: TryGetDecimal16 at exactly 2^96 - 1 (max 96-bit)
        // ---------------------------------------------------------------

        [Fact]
        public void TryGetDecimal16_MaxDecimal_ReturnsTrue()
        {
            // 2^96 - 1 = 0x00000000_FFFFFFFF_FFFFFFFF_FFFFFFFF
            // lo = 0xFFFFFFFFFFFFFFFF, hi = 0x00000000FFFFFFFF
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, // lo = 0xFFFFFFFFFFFFFFFF
                0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00  // hi = 0x00000000FFFFFFFF
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            bool success = reader.TryGetDecimal16(out decimal result);
            Assert.True(success);
            Assert.Equal(79228162514264337593543950335m, result);
        }

        [Fact]
        public void TryGetDecimal16_Exactly2Pow96_ReturnsFalse()
        {
            // 2^96 = hi has bit 32 set -> hi = 0x0000000100000000 > uint.MaxValue
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00, // scale = 0
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            bool success = reader.TryGetDecimal16(out decimal _);
            Assert.False(success);
        }

        // ---------------------------------------------------------------
        // Boundary: TryGetDecimal16 with negative large value
        // ---------------------------------------------------------------

        [Fact]
        public void TryGetDecimal16_NegativeLarge_ReturnsFalse()
        {
            // -2^96: after two's complement negation, magnitude hi > uint.MaxValue
            byte[] value = new byte[]
            {
                Decimal16Header,
                0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0xFF, 0xFF, 0xFF, 0xFF
            };
            VariantReader reader = new VariantReader(MinimalMetadata, value);

            bool success = reader.TryGetDecimal16(out decimal _);
            Assert.False(success);
        }

        [Fact]
        public void TryGetDecimal16_NegativeSmall_ReturnsTrue()
        {
            // Encode -42.5m as Decimal16 and verify TryGetDecimal16 works
            VariantValue vv = VariantValue.FromDecimal16(-42.5m);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            bool success = reader.TryGetDecimal16(out decimal result);
            Assert.True(success);
            Assert.Equal(-42.5m, result);
        }

        // ---------------------------------------------------------------
        // FromSqlDecimal — zero value
        // ---------------------------------------------------------------

        [Fact]
        public void FromSqlDecimal_Zero()
        {
            SqlDecimal sd = new SqlDecimal(0m);
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(VariantPrimitiveType.Decimal4, vv.PrimitiveType);
            Assert.False(vv.IsSqlDecimalStorage);
            Assert.Equal(0m, vv.AsDecimal());
        }

        // ---------------------------------------------------------------
        // FromSqlDecimal — 96-bit boundary (fits in decimal, needs Decimal16)
        // ---------------------------------------------------------------

        [Fact]
        public void FromSqlDecimal_96BitBoundary_ProducesDecimal16WithDecimalStorage()
        {
            // A value that needs all 96 bits: data[2] != 0 but data[3] == 0
            SqlDecimal sd = new SqlDecimal(79228162514264337593543950335m);
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(VariantPrimitiveType.Decimal16, vv.PrimitiveType);
            Assert.False(vv.IsSqlDecimalStorage); // stored as decimal, not SqlDecimal
            Assert.Equal(79228162514264337593543950335m, vv.AsDecimal());
        }

        // ---------------------------------------------------------------
        // AsDecimal on Decimal16 that was created via FromSqlDecimal but fits
        // ---------------------------------------------------------------

        [Fact]
        public void AsDecimal_FromSqlDecimalThatFits()
        {
            // 96-bit value goes through FromSqlDecimal -> stored as decimal
            SqlDecimal sd = new SqlDecimal(79228162514264337593543950335m);
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            Assert.Equal(79228162514264337593543950335m, vv.AsDecimal());
        }

        // ---------------------------------------------------------------
        // JSON writer for negative large Decimal16
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_NegativeLargeDecimal16_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("-79228162514264337593543950336");
            VariantValue vv = VariantValue.FromSqlDecimal(sd);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("-79228162514264337593543950336", json);
        }

        // ---------------------------------------------------------------
        // JSON writer for small Decimal16 (decimal path)
        // ---------------------------------------------------------------

        [Fact]
        public void JsonWriter_SmallDecimal16_WritesNumberValue()
        {
            VariantValue vv = VariantValue.FromDecimal16(42.5m);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            string json = VariantJsonWriter.ToJson(metadata, value);
            Assert.Equal("42.5", json);
        }

        // ---------------------------------------------------------------
        // GetSqlDecimal on non-decimal primitive throws
        // ---------------------------------------------------------------

        [Fact]
        public void GetSqlDecimal_OnInt32_Throws()
        {
            VariantValue vv = VariantValue.FromInt32(42);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] val) = builder.Encode(vv);
            VariantReader reader = new VariantReader(metadata, val);

            InvalidOperationException ex = null;
            try
            {
                reader.GetSqlDecimal();
            }
            catch (InvalidOperationException e)
            {
                ex = e;
            }
            Assert.NotNull(ex);
        }

        // ---------------------------------------------------------------
        // Nested containers with SqlDecimal Decimal16
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_ObjectWithSqlDecimal16Field()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "big", VariantValue.FromSqlDecimal(sd) },
                { "small", VariantValue.FromDecimal4(42.5m) },
            });
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(obj);

            VariantReader reader = new VariantReader(metadata, value);
            VariantValue materialized = reader.ToVariantValue();

            Assert.True(materialized.IsObject);
            IReadOnlyDictionary<string, VariantValue> fields = materialized.AsObject();
            Assert.Equal(sd, fields["big"].AsSqlDecimal());
            Assert.True(fields["big"].IsSqlDecimalStorage);
            Assert.Equal(42.5m, fields["small"].AsDecimal());
        }

        [Fact]
        public void RoundTrip_ArrayWithMixedDecimal16()
        {
            SqlDecimal large = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue arr = VariantValue.FromArray(
                VariantValue.FromSqlDecimal(large),
                VariantValue.FromDecimal16(42.5m),
                VariantValue.FromDecimal4(1.23m));

            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(arr);

            VariantReader reader = new VariantReader(metadata, value);
            VariantValue materialized = reader.ToVariantValue();

            Assert.True(materialized.IsArray);
            IReadOnlyList<VariantValue> elements = materialized.AsArray();
            Assert.Equal(3, elements.Count);
            Assert.True(elements[0].IsSqlDecimalStorage);
            Assert.Equal(large, elements[0].AsSqlDecimal());
            Assert.False(elements[1].IsSqlDecimalStorage);
            Assert.Equal(42.5m, elements[1].AsDecimal());
            Assert.Equal(1.23m, elements[2].AsDecimal());
        }

        // ---------------------------------------------------------------
        // JSON converter serialization of nested SqlDecimal
        // ---------------------------------------------------------------

        [Fact]
        public void JsonConverter_ObjectWithSqlDecimal_WritesValidJson()
        {
            SqlDecimal sd = SqlDecimal.Parse("99999999999999999999999999999999999999");
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "val", VariantValue.FromSqlDecimal(sd) },
            });

            JsonSerializerOptions options = new JsonSerializerOptions();
            options.Converters.Add(VariantJsonConverter.Instance);
            string json = JsonSerializer.Serialize(obj, options);
            Assert.Equal("{\"val\":99999999999999999999999999999999999999}", json);
        }

        // ---------------------------------------------------------------
        // Scale preservation round-trip
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_ScalePreserved_ZeroWithScale()
        {
            // 0.00000 has scale=5 — verify scale is preserved through SqlDecimal path
            decimal d = 0.00000m;
            VariantValue vv = VariantValue.FromDecimal16(d);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            VariantReader reader = new VariantReader(metadata, value);
            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(5, result.Scale);
        }

        // ---------------------------------------------------------------
        // Two's complement carry edge case
        // ---------------------------------------------------------------

        [Fact]
        public void RoundTrip_NegativeWithLoOverflow()
        {
            // Value where lo=0 in positive form, testing carry propagation in negate
            // This is -2^64 (lo=0, hi=1 in positive form -> negate produces lo=0, hi=-1)
            SqlDecimal positive = SqlDecimal.Parse("18446744073709551616"); // 2^64
            SqlDecimal negative = -positive;
            VariantValue vv = VariantValue.FromSqlDecimal(negative);
            VariantBuilder builder = new VariantBuilder();
            (byte[] metadata, byte[] value) = builder.Encode(vv);

            VariantReader reader = new VariantReader(metadata, value);
            SqlDecimal result = reader.GetSqlDecimal();
            Assert.Equal(negative, result);
        }
    }
}

