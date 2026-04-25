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
using Apache.Arrow;
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
