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
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Scalars.Tests
{
    /// <summary>
    /// Tests for <see cref="VariantValueWriter.CopyValue"/> and
    /// <see cref="VariantMetadataBuilder.CollectFieldNames"/> — the transcoder
    /// that walks a <see cref="VariantReader"/> and re-emits it into a writer
    /// with a (potentially different) metadata dictionary.
    /// </summary>
    public class VariantValueWriterCopyValueTests
    {
        private static readonly VariantBuilder _encoder = new VariantBuilder();

        /// <summary>
        /// Encodes a value, then transcodes it through CopyValue into a fresh writer
        /// whose metadata is collected via CollectFieldNames. Returns the transcoded
        /// VariantValue for equality comparison.
        /// </summary>
        private static VariantValue Transcode(VariantValue original)
        {
            (byte[] srcMetadata, byte[] srcValue) = _encoder.Encode(original);
            VariantReader srcReader = new VariantReader(srcMetadata, srcValue);

            VariantMetadataBuilder dstMetadata = new VariantMetadataBuilder();
            dstMetadata.CollectFieldNames(srcReader);
            byte[] dstMetadataBytes = dstMetadata.Build(out int[] idRemap);

            using VariantValueWriter writer = new VariantValueWriter(dstMetadata, idRemap);
            writer.CopyValue(srcReader);
            byte[] dstValue = writer.ToArray();

            return new VariantReader(dstMetadataBytes, dstValue).ToVariantValue();
        }

        // ---------------------------------------------------------------
        // Primitives
        // ---------------------------------------------------------------

        [Fact]
        public void CopyValue_Null() =>
            Assert.Equal(VariantValue.Null, Transcode(VariantValue.Null));

        [Fact]
        public void CopyValue_BooleanTrue() =>
            Assert.Equal(VariantValue.True, Transcode(VariantValue.True));

        [Fact]
        public void CopyValue_BooleanFalse() =>
            Assert.Equal(VariantValue.False, Transcode(VariantValue.False));

        [Theory]
        [InlineData(sbyte.MinValue)]
        [InlineData((sbyte)-1)]
        [InlineData((sbyte)0)]
        [InlineData(sbyte.MaxValue)]
        public void CopyValue_Int8(sbyte v) =>
            Assert.Equal(VariantValue.FromInt8(v), Transcode(VariantValue.FromInt8(v)));

        [Fact]
        public void CopyValue_Int16() =>
            Assert.Equal(VariantValue.FromInt16(short.MaxValue), Transcode(VariantValue.FromInt16(short.MaxValue)));

        [Fact]
        public void CopyValue_Int32() =>
            Assert.Equal(VariantValue.FromInt32(int.MinValue), Transcode(VariantValue.FromInt32(int.MinValue)));

        [Fact]
        public void CopyValue_Int64() =>
            Assert.Equal(VariantValue.FromInt64(long.MaxValue), Transcode(VariantValue.FromInt64(long.MaxValue)));

        [Fact]
        public void CopyValue_Float() =>
            Assert.Equal(VariantValue.FromFloat(3.14f), Transcode(VariantValue.FromFloat(3.14f)));

        [Fact]
        public void CopyValue_Double() =>
            Assert.Equal(VariantValue.FromDouble(Math.PI), Transcode(VariantValue.FromDouble(Math.PI)));

        [Fact]
        public void CopyValue_Decimal4() =>
            Assert.Equal(VariantValue.FromDecimal4(123.45m), Transcode(VariantValue.FromDecimal4(123.45m)));

        [Fact]
        public void CopyValue_Decimal8()
        {
            // Must fit in 64-bit unscaled (precision ≤ 18). 17 significant digits.
            VariantValue v = VariantValue.FromDecimal8(987654321.12345678m);
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_Decimal16() =>
            Assert.Equal(VariantValue.FromDecimal16(99999999.99m), Transcode(VariantValue.FromDecimal16(99999999.99m)));

        [Fact]
        public void CopyValue_Decimal16_SqlDecimalRange()
        {
            // Exceeds System.Decimal range — should route through SqlDecimal internally.
            SqlDecimal large = SqlDecimal.Parse("12345678901234567890123456789012345678");
            VariantValue original = VariantValue.FromSqlDecimal(large);
            VariantValue roundTripped = Transcode(original);
            Assert.Equal(original, roundTripped);
        }

        [Fact]
        public void CopyValue_Date() =>
            Assert.Equal(VariantValue.FromDate(19000), Transcode(VariantValue.FromDate(19000)));

        [Fact]
        public void CopyValue_Timestamp() =>
            Assert.Equal(VariantValue.FromTimestamp(1640995200000000L), Transcode(VariantValue.FromTimestamp(1640995200000000L)));

        [Fact]
        public void CopyValue_TimestampNtz() =>
            Assert.Equal(VariantValue.FromTimestampNtz(1640995200000000L), Transcode(VariantValue.FromTimestampNtz(1640995200000000L)));

        [Fact]
        public void CopyValue_TimeNtz() =>
            Assert.Equal(VariantValue.FromTimeNtz(123456789L), Transcode(VariantValue.FromTimeNtz(123456789L)));

        [Fact]
        public void CopyValue_TimestampTzNanos() =>
            Assert.Equal(VariantValue.FromTimestampTzNanos(1700000000_123456789L), Transcode(VariantValue.FromTimestampTzNanos(1700000000_123456789L)));

        [Fact]
        public void CopyValue_TimestampNtzNanos() =>
            Assert.Equal(VariantValue.FromTimestampNtzNanos(1700000000_123456789L), Transcode(VariantValue.FromTimestampNtzNanos(1700000000_123456789L)));

        [Fact]
        public void CopyValue_ShortString() =>
            // 5-byte string triggers the short-string encoding (≤ 63 bytes).
            Assert.Equal(VariantValue.FromString("hello"), Transcode(VariantValue.FromString("hello")));

        [Fact]
        public void CopyValue_LongString_PrimitiveEncoding()
        {
            // 64+ bytes forces the long-string primitive path.
            string longString = new string('x', 100);
            VariantValue v = VariantValue.FromString(longString);
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_EmptyString() =>
            Assert.Equal(VariantValue.FromString(""), Transcode(VariantValue.FromString("")));

        [Fact]
        public void CopyValue_Binary() =>
            Assert.Equal(
                VariantValue.FromBinary(new byte[] { 0, 1, 2, 255 }),
                Transcode(VariantValue.FromBinary(new byte[] { 0, 1, 2, 255 })));

        [Fact]
        public void CopyValue_Uuid()
        {
            Guid g = Guid.NewGuid();
            Assert.Equal(VariantValue.FromUuid(g), Transcode(VariantValue.FromUuid(g)));
        }

        // ---------------------------------------------------------------
        // Containers
        // ---------------------------------------------------------------

        [Fact]
        public void CopyValue_EmptyObject() =>
            Assert.Equal(
                VariantValue.FromObject(new Dictionary<string, VariantValue>()),
                Transcode(VariantValue.FromObject(new Dictionary<string, VariantValue>())));

        [Fact]
        public void CopyValue_EmptyArray() =>
            Assert.Equal(
                VariantValue.FromArray(new List<VariantValue>()),
                Transcode(VariantValue.FromArray(new List<VariantValue>())));

        [Fact]
        public void CopyValue_Object_FlatFields()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("alice") },
                { "age", VariantValue.FromInt32(30) },
                { "active", VariantValue.True },
            });
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_ArrayOfPrimitives()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(3));
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_NestedObjectInArray()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Alice") },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Bob") },
                }));
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_DeeplyNested()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "users", VariantValue.FromArray(
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "id", VariantValue.FromInt32(1) },
                        { "tags", VariantValue.FromArray(
                            VariantValue.FromString("admin"),
                            VariantValue.FromString("beta"))
                        },
                    }),
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "id", VariantValue.FromInt32(2) },
                        { "tags", VariantValue.FromArray(VariantValue.FromString("user")) },
                    }))
                },
                { "count", VariantValue.FromInt32(2) },
            });
            Assert.Equal(v, Transcode(v));
        }

        [Fact]
        public void CopyValue_MixedArray()
        {
            VariantValue v = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.Null,
                VariantValue.True,
                VariantValue.FromArray(VariantValue.FromInt32(4), VariantValue.FromInt32(5)));
            Assert.Equal(v, Transcode(v));
        }

        // ---------------------------------------------------------------
        // Field-ID remap: transcoding between distinct metadata dictionaries.
        // ---------------------------------------------------------------

        [Fact]
        public void CopyValue_RemapsFieldIds_WhenTargetMetadataIsSuperset()
        {
            // Source has fields {"age", "name"} — sorted metadata assigns IDs based
            // on byte order (age < name lexicographically).
            VariantValue source = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
            });

            (byte[] srcMetadata, byte[] srcValue) = _encoder.Encode(source);
            VariantReader srcReader = new VariantReader(srcMetadata, srcValue);

            // Build target metadata that contains extra names the source doesn't use.
            // This forces the field IDs to differ between source and target.
            VariantMetadataBuilder dstMetadata = new VariantMetadataBuilder();
            dstMetadata.Add("zzz-decoy-1");
            dstMetadata.Add("aaa-decoy-2");  // sorts before "age"
            dstMetadata.CollectFieldNames(srcReader);
            dstMetadata.Add("mmm-decoy-3");

            byte[] dstMetadataBytes = dstMetadata.Build(out int[] idRemap);
            using VariantValueWriter writer = new VariantValueWriter(dstMetadata, idRemap);
            writer.CopyValue(srcReader);
            byte[] dstValue = writer.ToArray();

            // Reading back through the target metadata should yield an equivalent
            // VariantValue even though the field IDs are numerically different.
            VariantValue reconstructed = new VariantReader(dstMetadataBytes, dstValue).ToVariantValue();
            Assert.Equal(source, reconstructed);
        }

        [Fact]
        public void CopyValue_ThrowsIfFieldNameNotInTargetMetadata()
        {
            VariantValue source = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "unknown-field", VariantValue.FromInt32(42) },
            });

            (byte[] srcMetadata, byte[] srcValue) = _encoder.Encode(source);

            // Deliberately skip CollectFieldNames — "unknown-field" is not in dst metadata.
            VariantMetadataBuilder dstMetadata = new VariantMetadataBuilder();
            byte[] _ = dstMetadata.Build(out int[] idRemap);
            using VariantValueWriter writer = new VariantValueWriter(dstMetadata, idRemap);

            // Ref structs (VariantReader) can't be captured by lambdas; reconstruct inside.
            Assert.Throws<KeyNotFoundException>(() =>
                writer.CopyValue(new VariantReader(srcMetadata, srcValue)));
        }

        // ---------------------------------------------------------------
        // CollectFieldNames — idempotence + coverage.
        // ---------------------------------------------------------------

        [Fact]
        public void CollectFieldNames_AccumulatesNamesFromNestedSources()
        {
            VariantValue v = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "outer1", VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "inner1", VariantValue.FromInt32(1) },
                        { "inner2", VariantValue.FromInt32(2) },
                    })
                },
                { "outer2", VariantValue.FromArray(
                    VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "inner3", VariantValue.FromInt32(3) },
                    }))
                },
            });

            (byte[] metadata, byte[] value) = _encoder.Encode(v);
            VariantReader reader = new VariantReader(metadata, value);

            VariantMetadataBuilder dst = new VariantMetadataBuilder();
            dst.CollectFieldNames(reader);

            // All five distinct field names should be present.
            Assert.Equal(5, dst.Count);
            Assert.Equal(0, dst.GetId("outer1"));  // insertion order (pre-sort)
            // Exact insertion IDs matter less than "every name is addable"; verify via re-add:
            int before = dst.Count;
            dst.Add("outer1");  // duplicate — no change
            Assert.Equal(before, dst.Count);
        }

        [Fact]
        public void CollectFieldNames_PrimitiveReader_IsNoOp()
        {
            (byte[] metadata, byte[] value) = _encoder.Encode(VariantValue.FromInt32(42));
            VariantReader reader = new VariantReader(metadata, value);

            VariantMetadataBuilder dst = new VariantMetadataBuilder();
            dst.CollectFieldNames(reader);
            Assert.Equal(0, dst.Count);
        }

        // ---------------------------------------------------------------
        // Merging values from multiple sources into one target.
        // ---------------------------------------------------------------

        [Fact]
        public void CopyValue_MergesTwoObjectsIntoOneTargetDictionary()
        {
            VariantValue a = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "alpha", VariantValue.FromInt32(1) },
            });
            VariantValue b = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "beta", VariantValue.FromInt32(2) },
            });

            (byte[] aMeta, byte[] aVal) = _encoder.Encode(a);
            (byte[] bMeta, byte[] bVal) = _encoder.Encode(b);
            VariantReader aReader = new VariantReader(aMeta, aVal);
            VariantReader bReader = new VariantReader(bMeta, bVal);

            // Single target metadata that covers both sources.
            VariantMetadataBuilder dst = new VariantMetadataBuilder();
            dst.CollectFieldNames(aReader);
            dst.CollectFieldNames(bReader);
            byte[] dstMeta = dst.Build(out int[] remap);

            // Transcode each into its own value stream (still referencing `dst`).
            using VariantValueWriter writerA = new VariantValueWriter(dst, remap);
            writerA.CopyValue(aReader);
            using VariantValueWriter writerB = new VariantValueWriter(dst, remap);
            writerB.CopyValue(bReader);

            Assert.Equal(a, new VariantReader(dstMeta, writerA.ToArray()).ToVariantValue());
            Assert.Equal(b, new VariantReader(dstMeta, writerB.ToArray()).ToVariantValue());
        }
    }
}
