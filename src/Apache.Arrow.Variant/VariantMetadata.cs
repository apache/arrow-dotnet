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
    /// Zero-copy reader for variant metadata: a header byte followed by a string
    /// dictionary used to look up field names in variant objects.
    /// </summary>
    /// <remarks>
    /// <para>Binary layout:</para>
    /// <code>
    /// [header: 1 byte]
    /// [dictionary_size: offset_size bytes]
    /// [offsets: (dictionary_size + 1) * offset_size bytes]
    /// [string bytes: concatenated UTF-8]
    /// </code>
    /// </remarks>
    public ref struct VariantMetadata
    {
        private readonly ReadOnlySpan<byte> _data;
        private readonly int _version;
        private readonly bool _sortedStrings;
        private readonly int _offsetSize;
        private readonly int _dictionarySize;
        private readonly int _offsetsStart;
        private readonly int _stringsStart;

        /// <summary>
        /// Parses variant metadata from a raw byte span.
        /// </summary>
        /// <param name="data">The complete metadata buffer.</param>
        public VariantMetadata(ReadOnlySpan<byte> data)
        {
            if (data.Length < 1)
            {
                throw new ArgumentException("Metadata buffer must be at least 1 byte.", nameof(data));
            }

            _data = data;

            VariantEncodingHelper.ParseMetadataHeader(data[0], out _version, out _sortedStrings, out _offsetSize);

            if (_version != VariantEncodingHelper.MetadataVersion)
            {
                throw new NotSupportedException($"Unsupported variant metadata version {_version}; expected {VariantEncodingHelper.MetadataVersion}.");
            }

            int pos = 1;

            if (pos + _offsetSize > data.Length)
            {
                throw new ArgumentException("Metadata buffer is too short to contain the dictionary size.", nameof(data));
            }

            _dictionarySize = VariantEncodingHelper.ReadLittleEndianInt(data.Slice(pos), _offsetSize);
            pos += _offsetSize;

            _offsetsStart = pos;
            _stringsStart = pos + (_dictionarySize + 1) * _offsetSize;

            if (_stringsStart > data.Length)
            {
                throw new ArgumentException("Metadata buffer is too short to contain the offset table.", nameof(data));
            }
        }

        /// <summary>
        /// Gets the number of strings in the metadata dictionary.
        /// </summary>
        public int DictionarySize => _dictionarySize;

        /// <summary>
        /// Gets a value indicating whether the dictionary strings are sorted
        /// in lexicographic (UTF-8 byte) order, enabling binary search.
        /// </summary>
        public bool IsSorted => _sortedStrings;

        /// <summary>
        /// Gets the raw UTF-8 bytes of the string at the given dictionary index.
        /// </summary>
        public ReadOnlySpan<byte> GetStringBytes(int index)
        {
            if ((uint)index >= (uint)_dictionarySize)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            int offsetPos = _offsetsStart + index * _offsetSize;
            int start = VariantEncodingHelper.ReadLittleEndianInt(_data.Slice(offsetPos), _offsetSize);
            int end = VariantEncodingHelper.ReadLittleEndianInt(_data.Slice(offsetPos + _offsetSize), _offsetSize);

            return _data.Slice(_stringsStart + start, end - start);
        }

        /// <summary>
        /// Gets the string at the given dictionary index, decoded from UTF-8.
        /// </summary>
        public string GetString(int index)
        {
            ReadOnlySpan<byte> bytes = GetStringBytes(index);
#if NET8_0_OR_GREATER
            return Encoding.UTF8.GetString(bytes);
#else
            return Encoding.UTF8.GetString(bytes.ToArray());
#endif
        }

        /// <summary>
        /// Finds the dictionary index of the given UTF-8 string.
        /// Returns -1 if not found.
        /// </summary>
        /// <remarks>
        /// Uses binary search if the dictionary is marked as sorted;
        /// otherwise falls back to linear scan.
        /// </remarks>
        public int FindString(ReadOnlySpan<byte> utf8)
        {
            if (_sortedStrings)
            {
                return BinarySearch(utf8);
            }

            return LinearSearch(utf8);
        }

        private int LinearSearch(ReadOnlySpan<byte> utf8)
        {
            for (int i = 0; i < _dictionarySize; i++)
            {
                if (GetStringBytes(i).SequenceEqual(utf8))
                {
                    return i;
                }
            }

            return -1;
        }

        private int BinarySearch(ReadOnlySpan<byte> utf8)
        {
            int lo = 0;
            int hi = _dictionarySize - 1;

            while (lo <= hi)
            {
                int mid = lo + (hi - lo) / 2;
                int cmp = GetStringBytes(mid).SequenceCompareTo(utf8);

                if (cmp == 0)
                {
                    return mid;
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

            return -1;
        }

    }
}
