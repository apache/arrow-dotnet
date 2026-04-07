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
using System.Text;

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Zero-copy reader for a variant object value. Provides access to field
    /// names (via the metadata dictionary) and field values.
    /// </summary>
    /// <remarks>
    /// <para>Binary layout (after the header byte):</para>
    /// <code>
    /// [num_fields: 1 or 4 bytes depending on is_large]
    /// [field_ids: num_fields * field_id_size bytes]
    /// [offsets: (num_fields + 1) * offset_size bytes]
    /// [field values: concatenated variant values]
    /// </code>
    /// <para>
    /// Field IDs index into the metadata string dictionary.
    /// Field IDs must be sorted by the lexicographic order of their
    /// corresponding field names in the metadata dictionary.
    /// </para>
    /// </remarks>
    public ref struct VariantObjectReader
    {
        private readonly ReadOnlySpan<byte> _metadata;
        private readonly ReadOnlySpan<byte> _value;
        private readonly int _fieldIdSize;
        private readonly int _offsetSize;
        private readonly int _fieldCount;
        private readonly int _fieldIdsStart;
        private readonly int _offsetsStart;
        private readonly int _valuesStart;
        private VariantMetadata _meta;

        /// <summary>
        /// Creates an object reader from the metadata and the object value buffer.
        /// </summary>
        public VariantObjectReader(ReadOnlySpan<byte> metadata, ReadOnlySpan<byte> value)
        {
            _metadata = metadata;
            _meta = new VariantMetadata(metadata);
            _value = value;

            VariantBasicType basicType = VariantEncodingHelper.GetBasicType(value[0]);
            if (basicType != VariantBasicType.Object)
            {
                throw new ArgumentException($"Expected Object basic type but got {basicType}.", nameof(value));
            }

            VariantEncodingHelper.ParseObjectHeader(value[0], out _fieldIdSize, out _offsetSize, out bool isLarge);

            int pos = 1;
            int countSize = isLarge ? 4 : 1;
            _fieldCount = VariantEncodingHelper.ReadLittleEndianInt(value.Slice(pos), countSize);
            pos += countSize;

            _fieldIdsStart = pos;
            _offsetsStart = _fieldIdsStart + _fieldCount * _fieldIdSize;
            _valuesStart = _offsetsStart + (_fieldCount + 1) * _offsetSize;
        }

        /// <summary>Gets the number of fields in this object.</summary>
        public int FieldCount => _fieldCount;

        /// <summary>
        /// Gets the metadata dictionary field ID for the field at the given index.
        /// </summary>
        public int GetFieldId(int index)
        {
            if ((uint)index >= (uint)_fieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int pos = _fieldIdsStart + index * _fieldIdSize;
            return VariantEncodingHelper.ReadLittleEndianInt(_value.Slice(pos), _fieldIdSize);
        }

        /// <summary>
        /// Gets the field name for the field at the given index by looking up
        /// the field ID in the metadata dictionary.
        /// </summary>
        public string GetFieldName(int index)
        {
            int fieldId = GetFieldId(index);
            return _meta.GetString(fieldId);
        }

        /// <summary>
        /// Gets the raw UTF-8 bytes of the field name at the given index.
        /// </summary>
        public ReadOnlySpan<byte> GetFieldNameBytes(int index)
        {
            int fieldId = GetFieldId(index);
            return _meta.GetStringBytes(fieldId);
        }

        /// <summary>
        /// Gets a <see cref="VariantReader"/> for the field value at the given index.
        /// </summary>
        public VariantReader GetFieldValue(int index)
        {
            if ((uint)index >= (uint)_fieldCount)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int offsetPos = _offsetsStart + index * _offsetSize;
            int start = VariantEncodingHelper.ReadLittleEndianInt(_value.Slice(offsetPos), _offsetSize);

            // Pass remaining bytes from the field's start offset rather than
            // slicing [offset[i], offset[i+1]) because the spec allows
            // non-monotonic offsets (field IDs are sorted by name but field
            // values may be stored in a different physical order).
            // VariantReader is self-describing and reads only the bytes it needs.
            return new VariantReader(_metadata, _value.Slice(_valuesStart + start));
        }

        /// <summary>
        /// Tries to find a field by name and returns a reader for its value.
        /// </summary>
        /// <param name="name">The UTF-8 bytes of the field name to look for.</param>
        /// <param name="value">The reader for the field value, if found.</param>
        /// <returns>True if the field was found; false otherwise.</returns>
        public bool TryGetField(scoped ReadOnlySpan<byte> name, out VariantReader value)
        {
            int lo = 0;
            int hi = _fieldCount - 1;

            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                int fieldId = GetFieldId(mid);
                int cmp = _meta.GetStringBytes(fieldId).SequenceCompareTo(name);

                if (cmp == 0)
                {
                    value = GetFieldValue(mid);
                    return true;
                }
                else if (cmp < 0)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }

            value = default;
            return false;
        }

        /// <summary>
        /// Tries to find a field by name (string) and returns a reader for its value.
        /// </summary>
        public bool TryGetField(string name, out VariantReader value)
        {
#if NET8_0_OR_GREATER
            int byteCount = Encoding.UTF8.GetByteCount(name);
            Span<byte> utf8 = byteCount <= 256 ? stackalloc byte[byteCount] : new byte[byteCount];
            Encoding.UTF8.GetBytes(name.AsSpan(), utf8);
            return TryGetField((ReadOnlySpan<byte>)utf8, out value);
#else
            byte[] utf8 = Encoding.UTF8.GetBytes(name);
            return TryGetField(new ReadOnlySpan<byte>(utf8), out value);
#endif
        }
    }
}
