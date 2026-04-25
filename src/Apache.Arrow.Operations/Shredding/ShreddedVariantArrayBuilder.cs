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
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Memory;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Assembles a shredded <see cref="VariantArray"/> from pre-shredded rows.
    /// Produces an Arrow struct with shared <c>metadata</c>, residual <c>value</c>,
    /// and the <c>typed_value</c> tree whose Arrow shape matches the <see cref="ShredSchema"/>.
    /// </summary>
    public static class ShreddedVariantArrayBuilder
    {
        /// <summary>
        /// Builds a shredded <see cref="VariantArray"/> from the output of
        /// <see cref="VariantShredder.Shred(System.Collections.Generic.IEnumerable{VariantValue}, ShredSchema)"/>.
        /// </summary>
        /// <param name="schema">The shredding schema applied to each row.</param>
        /// <param name="metadata">The column-level variant metadata (shared across rows).</param>
        /// <param name="rows">Per-row shred results whose residual bytes reference <paramref name="metadata"/>.</param>
        /// <param name="allocator">Arrow memory allocator, or default if null.</param>
        public static VariantArray Build(
            ShredSchema schema,
            byte[] metadata,
            IReadOnlyList<ShredResult> rows,
            MemoryAllocator allocator = null)
        {
            if (schema == null) throw new ArgumentNullException(nameof(schema));
            if (metadata == null) throw new ArgumentNullException(nameof(metadata));
            if (rows == null) throw new ArgumentNullException(nameof(rows));

            int rowCount = rows.Count;

            // metadata column: emit the shared bytes once per row. (A dictionary-encoded
            // or run-end-encoded representation would compress this; VariantArray's reader
            // already handles those, but for simplicity we emit the plain binary form.)
            BinaryArray.Builder metadataBuilder = new BinaryArray.Builder();
            for (int i = 0; i < rowCount; i++)
            {
                metadataBuilder.Append((ReadOnlySpan<byte>)metadata);
            }
            BinaryArray metadataArr = metadataBuilder.Build(allocator);

            // value column: residual bytes (or null).
            BinaryArray valueArr = BuildBinaryColumn(rows, allocator);

            // typed_value column (if the schema has one).
            List<Field> fields = new List<Field>
            {
                new Field("metadata", BinaryType.Default, false),
                new Field("value", BinaryType.Default, true),
            };
            List<IArrowArray> children = new List<IArrowArray> { metadataArr, valueArr };

            if (schema.TypedValueType != ShredType.None)
            {
                List<object> typedValues = new List<object>(rowCount);
                for (int i = 0; i < rowCount; i++) typedValues.Add(rows[i].TypedValue);
                IArrowArray typedValueArr = BuildTypedValueArray(schema, typedValues, allocator);
                fields.Add(new Field("typed_value", typedValueArr.Data.DataType, true));
                children.Add(typedValueArr);
            }

            StructType structType = new StructType(fields);
            StructArray structArr = new StructArray(
                structType, rowCount, children, ArrowBuffer.Empty, nullCount: 0);
            // The public VariantArray(IArrowArray) constructor infers the VariantType
            // from the struct's shape (including detecting the shredded layout).
            return new VariantArray(structArr);
        }

        // ---------------------------------------------------------------
        // Recursive builders
        // ---------------------------------------------------------------

        private static BinaryArray BuildBinaryColumn(IReadOnlyList<ShredResult> rows, MemoryAllocator allocator)
        {
            BinaryArray.Builder b = new BinaryArray.Builder();
            foreach (ShredResult r in rows)
            {
                if (r.Value == null) b.AppendNull();
                else b.Append((ReadOnlySpan<byte>)r.Value);
            }
            return b.Build(allocator);
        }

        private static IArrowArray BuildTypedValueArray(
            ShredSchema schema,
            IList<object> typedValues,
            MemoryAllocator allocator)
        {
            switch (schema.TypedValueType)
            {
                case ShredType.Object: return BuildObjectTyped(schema, typedValues, allocator);
                case ShredType.Array:  return BuildArrayTyped(schema, typedValues, allocator);
                default:               return BuildPrimitiveTyped(schema.TypedValueType, typedValues, allocator);
            }
        }

        private static StructArray BuildObjectTyped(
            ShredSchema schema,
            IList<object> typedValues,
            MemoryAllocator allocator)
        {
            int rowCount = typedValues.Count;
            List<Field> fieldDefs = new List<Field>(schema.ObjectFields.Count);
            List<IArrowArray> fieldArrays = new List<IArrowArray>(schema.ObjectFields.Count);

            foreach (KeyValuePair<string, ShredSchema> entry in schema.ObjectFields)
            {
                List<ShredResult> fieldShreds = new List<ShredResult>(rowCount);
                foreach (object tv in typedValues)
                {
                    if (tv is ShredObjectResult obj &&
                        obj.Fields.TryGetValue(entry.Key, out ShredResult r))
                    {
                        fieldShreds.Add(r);
                    }
                    else
                    {
                        fieldShreds.Add(ShredResult.Missing);
                    }
                }
                StructArray elementGroup = BuildElementGroupArray(entry.Value, fieldShreds, allocator);
                fieldArrays.Add(elementGroup);
                fieldDefs.Add(new Field(entry.Key, elementGroup.Data.DataType, true));
            }

            ArrowBuffer nullBitmap = BuildNullBitmap(typedValues, v => v != null, rowCount,
                allocator, out int nullCount);
            StructType structType = new StructType(fieldDefs);
            return new StructArray(structType, rowCount, fieldArrays, nullBitmap, nullCount);
        }

        private static ListArray BuildArrayTyped(
            ShredSchema schema,
            IList<object> typedValues,
            MemoryAllocator allocator)
        {
            int rowCount = typedValues.Count;
            List<ShredResult> flatElements = new List<ShredResult>();
            ArrowBuffer.Builder<int> offsets = new ArrowBuffer.Builder<int>();
            offsets.Append(0);
            ArrowBuffer.BitmapBuilder validity = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;

            foreach (object tv in typedValues)
            {
                if (tv is ShredArrayResult arr)
                {
                    foreach (ShredResult e in arr.Elements) flatElements.Add(e);
                    validity.Append(true);
                }
                else
                {
                    validity.Append(false);
                    nullCount++;
                }
                offsets.Append(flatElements.Count);
            }

            StructArray elementGroup = BuildElementGroupArray(schema.ArrayElement, flatElements, allocator);
            Field elementField = new Field("element", elementGroup.Data.DataType, true);
            ListType listType = new ListType(elementField);
            ArrowBuffer nullBitmap = nullCount > 0 ? validity.Build(allocator) : ArrowBuffer.Empty;
            return new ListArray(listType, rowCount, offsets.Build(allocator), elementGroup, nullBitmap, nullCount);
        }

        /// <summary>
        /// Builds a <c>{value?, typed_value?}</c> element group. Always emits both
        /// sub-fields (for simplicity) — readers tolerate the absent-field case via
        /// null entries.
        /// </summary>
        private static StructArray BuildElementGroupArray(
            ShredSchema schema,
            IReadOnlyList<ShredResult> results,
            MemoryAllocator allocator)
        {
            int rowCount = results.Count;
            List<Field> fieldDefs = new List<Field>(2);
            List<IArrowArray> children = new List<IArrowArray>(2);

            // value column.
            BinaryArray valueArr = BuildBinaryColumn(results, allocator);
            fieldDefs.Add(new Field("value", BinaryType.Default, true));
            children.Add(valueArr);

            // typed_value column (only when schema has one).
            if (schema.TypedValueType != ShredType.None)
            {
                List<object> typedValues = new List<object>(rowCount);
                foreach (ShredResult r in results) typedValues.Add(r.TypedValue);
                IArrowArray typedArr = BuildTypedValueArray(schema, typedValues, allocator);
                fieldDefs.Add(new Field("typed_value", typedArr.Data.DataType, true));
                children.Add(typedArr);
            }

            // Outer slot validity: non-null iff the slot isn't "missing" (both columns null).
            ArrowBuffer.BitmapBuilder validity = new ArrowBuffer.BitmapBuilder();
            int nullCount = 0;
            foreach (ShredResult r in results)
            {
                if (r.IsMissing) { validity.Append(false); nullCount++; }
                else validity.Append(true);
            }
            ArrowBuffer nullBitmap = nullCount > 0 ? validity.Build(allocator) : ArrowBuffer.Empty;

            StructType structType = new StructType(fieldDefs);
            return new StructArray(structType, rowCount, children, nullBitmap, nullCount);
        }

        // ---------------------------------------------------------------
        // Primitive typed-value builders
        // ---------------------------------------------------------------

        private static IArrowArray BuildPrimitiveTyped(
            ShredType shredType,
            IList<object> typedValues,
            MemoryAllocator allocator)
        {
            switch (shredType)
            {
                case ShredType.Boolean:
                {
                    BooleanArray.Builder b = new BooleanArray.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((bool)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Int8:
                {
                    Int8Array.Builder b = new Int8Array.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((sbyte)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Int16:
                {
                    Int16Array.Builder b = new Int16Array.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((short)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Int32:
                {
                    Int32Array.Builder b = new Int32Array.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((int)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Int64:
                {
                    Int64Array.Builder b = new Int64Array.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((long)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Float:
                {
                    FloatArray.Builder b = new FloatArray.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((float)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Double:
                {
                    DoubleArray.Builder b = new DoubleArray.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((double)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Decimal4:
                case ShredType.Decimal8:
                case ShredType.Decimal16:
                    return BuildDecimalArray(shredType, typedValues, allocator);
                case ShredType.Date:
                    return BuildDate32(typedValues, allocator);
                case ShredType.Timestamp:
                    return BuildTimestamp(typedValues, TimeUnit.Microsecond, "UTC", allocator);
                case ShredType.TimestampNtz:
                    return BuildTimestamp(typedValues, TimeUnit.Microsecond, null, allocator);
                case ShredType.TimestampTzNanos:
                    return BuildTimestamp(typedValues, TimeUnit.Nanosecond, "UTC", allocator);
                case ShredType.TimestampNtzNanos:
                    return BuildTimestamp(typedValues, TimeUnit.Nanosecond, null, allocator);
                case ShredType.TimeNtz:
                    return BuildTime64(typedValues, allocator);
                case ShredType.String:
                {
                    StringArray.Builder b = new StringArray.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((string)v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Binary:
                {
                    BinaryArray.Builder b = new BinaryArray.Builder();
                    foreach (object v in typedValues)
                    {
                        if (v == null) b.AppendNull(); else b.Append((ReadOnlySpan<byte>)(byte[])v);
                    }
                    return b.Build(allocator);
                }
                case ShredType.Uuid:
                    return BuildUuidArray(typedValues, allocator);
                default:
                    throw new NotSupportedException($"Cannot build typed column for ShredType.{shredType}.");
            }
        }

        private static TimestampArray BuildTimestamp(
            IList<object> typedValues, TimeUnit unit, string timezone, MemoryAllocator allocator)
        {
            TimestampType type = new TimestampType(unit, timezone);
            (ArrowBuffer values, ArrowBuffer nullBitmap, int nullCount) =
                BuildLongBuffers(typedValues, allocator);
            return new TimestampArray(type, values, nullBitmap, typedValues.Count, nullCount, offset: 0);
        }

        private static Date32Array BuildDate32(IList<object> typedValues, MemoryAllocator allocator)
        {
            (ArrowBuffer values, ArrowBuffer nullBitmap, int nullCount) =
                BuildIntBuffers(typedValues, allocator);
            return new Date32Array(values, nullBitmap, typedValues.Count, nullCount, offset: 0);
        }

        private static Time64Array BuildTime64(IList<object> typedValues, MemoryAllocator allocator)
        {
            Time64Type type = new Time64Type(TimeUnit.Microsecond);
            (ArrowBuffer values, ArrowBuffer nullBitmap, int nullCount) =
                BuildLongBuffers(typedValues, allocator);
            return new Time64Array(type, values, nullBitmap, typedValues.Count, nullCount, offset: 0);
        }

        private static (ArrowBuffer values, ArrowBuffer nullBitmap, int nullCount) BuildLongBuffers(
            IList<object> typedValues, MemoryAllocator allocator)
        {
            ArrowBuffer.Builder<long> values = new ArrowBuffer.Builder<long>(typedValues.Count);
            ArrowBuffer.BitmapBuilder bitmap = new ArrowBuffer.BitmapBuilder(typedValues.Count);
            int nullCount = 0;
            foreach (object v in typedValues)
            {
                if (v == null)
                {
                    values.Append(0L);
                    bitmap.Append(false);
                    nullCount++;
                }
                else
                {
                    values.Append((long)v);
                    bitmap.Append(true);
                }
            }
            ArrowBuffer nullBitmap = nullCount > 0 ? bitmap.Build(allocator) : ArrowBuffer.Empty;
            return (values.Build(allocator), nullBitmap, nullCount);
        }

        private static (ArrowBuffer values, ArrowBuffer nullBitmap, int nullCount) BuildIntBuffers(
            IList<object> typedValues, MemoryAllocator allocator)
        {
            ArrowBuffer.Builder<int> values = new ArrowBuffer.Builder<int>(typedValues.Count);
            ArrowBuffer.BitmapBuilder bitmap = new ArrowBuffer.BitmapBuilder(typedValues.Count);
            int nullCount = 0;
            foreach (object v in typedValues)
            {
                if (v == null)
                {
                    values.Append(0);
                    bitmap.Append(false);
                    nullCount++;
                }
                else
                {
                    values.Append((int)v);
                    bitmap.Append(true);
                }
            }
            ArrowBuffer nullBitmap = nullCount > 0 ? bitmap.Build(allocator) : ArrowBuffer.Empty;
            return (values.Build(allocator), nullBitmap, nullCount);
        }

        private static Decimal128Array BuildDecimalArray(
            ShredType shredType, IList<object> typedValues, MemoryAllocator allocator)
        {
            int precision = shredType == ShredType.Decimal4 ? 9
                          : shredType == ShredType.Decimal8 ? 18
                          : 38;
            // Scale: pick the max scale seen across all rows. Arrow's builder will rescale
            // individual values to match; if rows have heterogeneous scales the larger one
            // accommodates all values exactly (assuming precision is not exceeded).
            int scale = 0;
            foreach (object v in typedValues)
            {
                if (v is decimal d)
                {
                    int s = (decimal.GetBits(d)[3] >> 16) & 0x7F;
                    if (s > scale) scale = s;
                }
            }

            Decimal128Array.Builder b = new Decimal128Array.Builder(new Decimal128Type(precision, scale));
            foreach (object v in typedValues)
            {
                if (v == null) b.AppendNull(); else b.Append((decimal)v);
            }
            return b.Build(allocator);
        }

        /// <summary>
        /// UUID is encoded as FixedSizeBinary(16) in big-endian (RFC 4122) byte order.
        /// FixedSizeBinaryArray has no concrete public Builder, so we construct the
        /// value buffer manually (16 bytes per row).
        /// </summary>
        private static FixedSizeBinaryArray BuildUuidArray(
            IList<object> typedValues, MemoryAllocator allocator)
        {
            FixedSizeBinaryType type = new FixedSizeBinaryType(16);
            int rowCount = typedValues.Count;
            ArrowBuffer.Builder<byte> values = new ArrowBuffer.Builder<byte>(rowCount * 16);
            ArrowBuffer.BitmapBuilder bitmap = new ArrowBuffer.BitmapBuilder(rowCount);
            int nullCount = 0;
            byte[] scratch = new byte[16];

            foreach (object v in typedValues)
            {
                if (v == null)
                {
                    // Emit 16 zero bytes as a placeholder; the null bitmap marks it invalid.
                    for (int i = 0; i < 16; i++) values.Append((byte)0);
                    bitmap.Append(false);
                    nullCount++;
                    continue;
                }

                Guid g = (Guid)v;
#if NET8_0_OR_GREATER
                g.TryWriteBytes(scratch.AsSpan(), bigEndian: true, out _);
#else
                byte[] native = g.ToByteArray();
                // Convert .NET mixed-endian to big-endian.
                scratch[0] = native[3]; scratch[1] = native[2]; scratch[2] = native[1]; scratch[3] = native[0];
                scratch[4] = native[5]; scratch[5] = native[4];
                scratch[6] = native[7]; scratch[7] = native[6];
                Buffer.BlockCopy(native, 8, scratch, 8, 8);
#endif
                for (int i = 0; i < 16; i++) values.Append(scratch[i]);
                bitmap.Append(true);
            }

            ArrowBuffer nullBitmap = nullCount > 0 ? bitmap.Build(allocator) : ArrowBuffer.Empty;
            ArrayData data = new ArrayData(
                type, rowCount, nullCount, 0,
                new[] { nullBitmap, values.Build(allocator) });
            return new FixedSizeBinaryArray(data);
        }

        // ---------------------------------------------------------------
        // Utility
        // ---------------------------------------------------------------

        private static ArrowBuffer BuildNullBitmap<T>(
            IList<T> items, Func<T, bool> isValid, int rowCount,
            MemoryAllocator allocator, out int nullCount)
        {
            ArrowBuffer.BitmapBuilder bitmap = new ArrowBuffer.BitmapBuilder(rowCount);
            int nulls = 0;
            for (int i = 0; i < rowCount; i++)
            {
                bool valid = isValid(items[i]);
                bitmap.Append(valid);
                if (!valid) nulls++;
            }
            nullCount = nulls;
            return nulls > 0 ? bitmap.Build(allocator) : ArrowBuffer.Empty;
        }
    }
}
