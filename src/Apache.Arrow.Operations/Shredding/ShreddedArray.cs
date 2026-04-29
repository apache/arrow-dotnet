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
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Reader for a single row of a shredded-array slot. The underlying storage
    /// is a list of element groups (each a <c>{value, typed_value}</c> struct).
    /// </summary>
    public ref struct ShreddedArray
    {
        private readonly ShredSchema _schema;
        private readonly ReadOnlySpan<byte> _metadata;
        // The typed_value list (elements are {value, typed_value} structs). May be null.
        private readonly ListArray _list;
        // The residual binary column at the array level (for unshredded arrays). May be null.
        private readonly IArrowArray _residual;
        private readonly int _row;

        internal ShreddedArray(
            ShredSchema schema,
            ReadOnlySpan<byte> metadata,
            ListArray list,
            IArrowArray residual,
            int row)
        {
            _schema = schema;
            _metadata = metadata;
            _list = list;
            _residual = residual;
            _row = row;
        }

        /// <summary>
        /// True when the typed list is populated at this row (the array is stored
        /// element-by-element in the shredded column).
        /// </summary>
        public bool IsTypedList => _list != null && !_list.IsNull(_row);

        /// <summary>
        /// The number of shredded elements at this row. Only valid when
        /// <see cref="IsTypedList"/> is true.
        /// </summary>
        /// <exception cref="InvalidOperationException">If the array is stored as a residual.</exception>
        public int ElementCount
        {
            get
            {
                if (!IsTypedList)
                {
                    throw new InvalidOperationException(
                        "Array at this row is stored as a residual (not a typed list). " +
                        "Use TryGetResidualReader and iterate via VariantArrayReader.");
                }
                return _list.ValueOffsets[_row + 1] - _list.ValueOffsets[_row];
            }
        }

        /// <summary>
        /// Gets a <see cref="ShreddedVariant"/> reader for the element at position
        /// <paramref name="index"/>. Only valid when <see cref="IsTypedList"/> is true.
        /// </summary>
        public ShreddedVariant GetElement(int index)
        {
            if (!IsTypedList)
            {
                throw new InvalidOperationException(
                    "Array at this row is stored as a residual (not a typed list).");
            }
            int start = _list.ValueOffsets[_row];
            int end = _list.ValueOffsets[_row + 1];
            if ((uint)index >= (uint)(end - start))
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            StructArray elementGroup = (StructArray)_list.Values;
            return ShreddingHelpers.BuildSlot(_schema.ArrayElement, _metadata, elementGroup, start + index);
        }

        /// <summary>
        /// If the array is stored as a residual at this row (not shredded), returns
        /// a <see cref="VariantReader"/> over the residual bytes. Callers can then
        /// inspect the array via <c>VariantArrayReader</c>.
        /// </summary>
        public bool TryGetResidualReader(out VariantReader reader)
        {
            if (_residual == null || _residual.IsNull(_row))
            {
                reader = default;
                return false;
            }
            ReadOnlySpan<byte> bytes = ((BinaryArray)_residual).GetBytes(_row, out _);
            reader = new VariantReader(_metadata, bytes);
            return true;
        }

        /// <summary>
        /// Materializes the array into a <see cref="VariantValue"/>. If the typed
        /// list is null at this row, falls back to the residual binary (the array
        /// was stored unshredded for this row). When neither is populated, the
        /// slot encodes a variant null — consistent with <see cref="ShreddedObject"/>
        /// and <see cref="ShreddedVariant"/>.
        /// </summary>
        public VariantValue ToVariantValue()
        {
            if (_list != null && !_list.IsNull(_row))
            {
                int start = _list.ValueOffsets[_row];
                int end = _list.ValueOffsets[_row + 1];
                int count = end - start;

                StructArray elementGroup = (StructArray)_list.Values;
                List<VariantValue> elements = new List<VariantValue>(count);
                for (int i = start; i < end; i++)
                {
                    // For array elements, a both-null slot encodes a variant null
                    // (arrays cannot contain "missing"). ShreddedVariant.ToVariantValue
                    // already returns VariantValue.Null for a missing slot.
                    ShreddedVariant slot = ShreddingHelpers.BuildSlot(_schema.ArrayElement, _metadata, elementGroup, i);
                    elements.Add(slot.ToVariantValue());
                }
                return VariantValue.FromArray(elements);
            }

            // No typed list at this row — decode from the residual if present,
            // otherwise the slot is variant null.
            if (_residual == null || _residual.IsNull(_row))
            {
                return VariantValue.Null;
            }
            BinaryArray residualBinary = (BinaryArray)_residual;
            ReadOnlySpan<byte> bytes = residualBinary.GetBytes(_row, out _);
            return new VariantReader(_metadata, bytes).ToVariantValue();
        }
    }
}
