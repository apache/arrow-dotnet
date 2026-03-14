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

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Zero-copy reader for a variant array value.
    /// </summary>
    /// <remarks>
    /// <para>Binary layout (after the header byte):</para>
    /// <code>
    /// [num_elements: 1 or 4 bytes depending on is_large]
    /// [offsets: (num_elements + 1) * offset_size bytes]
    /// [element values: concatenated variant values]
    /// </code>
    /// </remarks>
    public ref struct VariantArrayReader
    {
        private readonly ReadOnlySpan<byte> _metadata;
        private readonly ReadOnlySpan<byte> _value;
        private readonly int _offsetSize;
        private readonly int _elementCount;
        private readonly int _offsetsStart;
        private readonly int _valuesStart;

        /// <summary>
        /// Creates an array reader from the metadata and the array value buffer.
        /// </summary>
        public VariantArrayReader(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
        {
            _metadata = metadata;
            _value = value;

            VariantBasicType basicType = VariantEncodingHelper.GetBasicType(value[0]);
            if (basicType != VariantBasicType.Array)
            {
                throw new ArgumentException($"Expected Array basic type but got {basicType}.", nameof(value));
            }

            VariantEncodingHelper.ParseArrayHeader(value[0], out _offsetSize, out bool isLarge);

            int pos = 1;
            int countSize = isLarge ? 4 : 1;
            _elementCount = VariantEncodingHelper.ReadLittleEndianInt(value.Slice(pos), countSize);
            pos += countSize;

            _offsetsStart = pos;
            _valuesStart = _offsetsStart + (_elementCount + 1) * _offsetSize;
        }

        /// <summary>Gets the number of elements in this array.</summary>
        public int ElementCount => _elementCount;

        /// <summary>
        /// Gets a <see cref="VariantReader"/> for the element at the given index.
        /// </summary>
        public VariantReader GetElement(int index)
        {
            if ((uint)index >= (uint)_elementCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int offsetPos = _offsetsStart + index * _offsetSize;
            int start = VariantEncodingHelper.ReadLittleEndianInt(_value.Slice(offsetPos), _offsetSize);
            int end = VariantEncodingHelper.ReadLittleEndianInt(_value.Slice(offsetPos + _offsetSize), _offsetSize);

            return new VariantReader(_metadata, _value.Slice(_valuesStart + start, end - start));
        }
    }
}
