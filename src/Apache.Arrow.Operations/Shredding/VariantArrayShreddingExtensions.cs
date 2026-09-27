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
using Apache.Arrow.Memory;
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Shredding-aware extensions on <see cref="VariantArray"/>. Provides both
    /// transparent materialization (<see cref="GetLogicalVariantValue"/>) and a
    /// reader-style API (<see cref="GetShreddedVariant"/>) that exposes typed
    /// columns and residual bytes side-by-side.
    /// </summary>
    public static class VariantArrayShreddingExtensions
    {
        /// <summary>
        /// Gets the <see cref="ShredSchema"/> for a variant array, derived from
        /// its Arrow storage type. Returns <see cref="ShredSchema.Unshredded"/>
        /// for unshredded columns.
        /// </summary>
        public static ShredSchema GetShredSchema(this VariantArray array)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            return ShredSchema.FromArrowType(array.VariantType.TypedValueField?.DataType);
        }

        /// <summary>
        /// Gets a <see cref="ShreddedVariant"/> reader for the element at the given index.
        /// Exposes typed-column access and residual bytes without materializing the
        /// full logical variant. Works for both shredded and unshredded columns.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the element is null.</exception>
        public static ShreddedVariant GetShreddedVariant(this VariantArray array, int index)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (index < 0 || index >= array.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (array.IsNull(index))
                throw new InvalidOperationException("Cannot create a ShreddedVariant for a null element.");

            ShredSchema schema = GetShredSchema(array);
            ReadOnlySpan<byte> metadata = array.GetMetadataBytes(index);
            IArrowArray valueArr = array.VariantType.HasValueColumn
                ? GetValueArray(array)
                : null;
            IArrowArray typedValueArr = array.TypedValueArray;

            return new ShreddedVariant(schema, metadata, valueArr, typedValueArr, index);
        }

        /// <summary>
        /// Materializes the element at <paramref name="index"/> into a logical
        /// <see cref="VariantValue"/>, transparently merging shredded columns and
        /// residual bytes. Works for both shredded and unshredded columns.
        /// </summary>
        public static VariantValue GetLogicalVariantValue(this VariantArray array, int index)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (index < 0 || index >= array.Length)
                throw new ArgumentOutOfRangeException(nameof(index));
            if (array.IsNull(index))
                return VariantValue.Null;

            return GetShreddedVariant(array, index).ToVariantValue();
        }

        /// <summary>
        /// Infers a <see cref="ShredSchema"/> from the logical values of a variant array.
        /// Null elements are ignored. Works for both shredded and unshredded columns.
        /// </summary>
        /// <remarks>
        /// When writing a column in batches (e.g. Parquet row groups), infer once over a
        /// representative batch and pass the result to <see cref="Shred"/> for every batch,
        /// so that all batches share one layout.
        /// </remarks>
        public static ShredSchema InferShredSchema(this VariantArray array, ShredOptions options = null)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            return new ShredSchemaInferer().Infer(GetLogicalValues(array), options);
        }

        /// <summary>
        /// Shreds a variant array into the layout described by <paramref name="schema"/>.
        /// The input may itself be shredded (under any schema); its logical values are
        /// re-shredded. Null elements remain null.
        /// </summary>
        /// <param name="array">The variant array to shred.</param>
        /// <param name="schema">The target shredding schema.</param>
        /// <param name="allocator">Arrow memory allocator, or default if null.</param>
        public static VariantArray Shred(this VariantArray array, ShredSchema schema, MemoryAllocator allocator = null)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            (byte[] metadata, IReadOnlyList<ShredResult> rows) =
                VariantShredder.Shred(GetLogicalValues(array), schema);
            return ShreddedVariantArrayBuilder.Build(schema, metadata, rows, allocator);
        }

        /// <summary>
        /// Infers a shredding schema from <paramref name="array"/> and, if it produces a
        /// shredded layout, shreds the array into it.
        /// </summary>
        /// <param name="array">The variant array to shred.</param>
        /// <param name="options">Inference options, or <see cref="ShredOptions.Default"/> if null.</param>
        /// <param name="shredded">The shredded array, or null when this method returns false.</param>
        /// <param name="allocator">Arrow memory allocator, or default if null.</param>
        /// <returns>
        /// True if a shredded layout was inferred; false if the values have no layout
        /// worth shredding (the inferred schema is <see cref="ShredSchema.Unshredded"/>).
        /// </returns>
        public static bool TryShred(
            this VariantArray array,
            ShredOptions options,
            out VariantArray shredded,
            MemoryAllocator allocator = null)
        {
            ShredSchema schema = InferShredSchema(array, options);
            if (schema.TypedValueType == ShredType.None)
            {
                shredded = null;
                return false;
            }
            shredded = Shred(array, schema, allocator);
            return true;
        }

        /// <summary>
        /// Converts a shredded variant array into its unshredded equivalent, in which
        /// every element is stored as self-contained metadata and value bytes. Null
        /// elements remain null. An unshredded input is returned unchanged.
        /// </summary>
        /// <param name="array">The variant array to reassemble.</param>
        /// <param name="allocator">Arrow memory allocator, or default if null.</param>
        public static VariantArray Reassemble(this VariantArray array, MemoryAllocator allocator = null)
        {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (!array.IsShredded) return array;

            var builder = new VariantArray.Builder();
            builder.AppendRange(GetLogicalValues(array));
            return builder.Build(allocator);
        }

        /// <summary>
        /// Enumerates the logical value of every element, with null for null elements.
        /// Resolves the column's schema and child arrays once rather than per row.
        /// </summary>
        private static IEnumerable<VariantValue?> GetLogicalValues(VariantArray array)
        {
            ShredSchema schema = GetShredSchema(array);
            IArrowArray valueArr = array.VariantType.HasValueColumn ? GetValueArray(array) : null;
            IArrowArray typedValueArr = array.TypedValueArray;

            for (int i = 0; i < array.Length; i++)
            {
                yield return array.IsNull(i)
                    ? (VariantValue?)null
                    : GetLogicalValue(array, schema, valueArr, typedValueArr, i);
            }
        }

        private static VariantValue GetLogicalValue(
            VariantArray array, ShredSchema schema, IArrowArray valueArr, IArrowArray typedValueArr, int index)
        {
            return new ShreddedVariant(schema, array.GetMetadataBytes(index), valueArr, typedValueArr, index)
                .ToVariantValue();
        }

        /// <summary>
        /// Returns the underlying <c>value</c> sub-array of the VariantArray's struct storage.
        /// This mirrors what <see cref="VariantArray.GetValueBytes"/> uses internally.
        /// </summary>
        private static IArrowArray GetValueArray(VariantArray array)
        {
            StructArray storage = array.StorageArray;
            var structType = (Apache.Arrow.Types.StructType)storage.Data.DataType;
            int idx = structType.GetFieldIndex("value");
            return storage.Fields[idx];
        }
    }
}
