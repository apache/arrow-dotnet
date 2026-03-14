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
using System.Text;

namespace Apache.Arrow.Variant
{
    /// <summary>
    /// Collects unique field names and builds the sorted binary metadata
    /// dictionary used by variant objects.
    /// </summary>
    public sealed class VariantMetadataBuilder
    {
        private const int StackAllocThreshold = 256;

        private readonly Dictionary<string, int> _nameToId = new Dictionary<string, int>();
        private readonly List<string> _names = new List<string>();

        /// <summary>
        /// Adds a field name to the dictionary. Returns the assigned field ID.
        /// If the name already exists, returns the existing ID.
        /// </summary>
        public int Add(string name)
        {
            if (_nameToId.TryGetValue(name, out int existingId))
            {
                return existingId;
            }

            int id = _names.Count;
            _names.Add(name);
            _nameToId[name] = id;
            return id;
        }

        /// <summary>
        /// Gets the field ID for an already-added name.
        /// </summary>
        public int GetId(string name) => _nameToId[name];

        /// <summary>
        /// Gets the number of unique field names.
        /// </summary>
        public int Count => _names.Count;

        /// <summary>
        /// Builds the binary metadata with the dictionary sorted by UTF-8 byte order.
        /// </summary>
        /// <param name="idRemap">
        /// Maps original (insertion-order) field IDs to sorted field IDs.
        /// idRemap[originalId] = sortedId.
        /// </param>
        /// <returns>The binary metadata bytes.</returns>
        public byte[] Build(out int[] idRemap)
        {
            int count = _names.Count;

            // Encode all names into a single contiguous UTF-8 buffer.
            int[] nameOffsets = new int[count + 1];
            nameOffsets[0] = 0;
            for (int i = 0; i < count; i++)
            {
                nameOffsets[i + 1] = nameOffsets[i] + Encoding.UTF8.GetByteCount(_names[i]);
            }
            int totalUtf8 = nameOffsets[count];
            byte[] utf8Buffer = new byte[totalUtf8];
            for (int i = 0; i < count; i++)
            {
                Encoding.UTF8.GetBytes(_names[i], 0, _names[i].Length, utf8Buffer, nameOffsets[i]);
            }

            // Sort indices by UTF-8 byte order using SequenceCompareTo.
#if NET8_0_OR_GREATER
            Span<int> sortedIndices = count <= StackAllocThreshold
                ? stackalloc int[count]
                : new int[count];
            for (int i = 0; i < count; i++)
            {
                sortedIndices[i] = i;
            }
            sortedIndices.Sort((a, b) =>
                new ReadOnlySpan<byte>(utf8Buffer, nameOffsets[a], nameOffsets[a + 1] - nameOffsets[a])
                    .SequenceCompareTo(new ReadOnlySpan<byte>(utf8Buffer, nameOffsets[b], nameOffsets[b + 1] - nameOffsets[b])));
#else
            int[] sortedIndices = new int[count];
            for (int i = 0; i < count; i++)
            {
                sortedIndices[i] = i;
            }
            Array.Sort(sortedIndices, (a, b) =>
                new ReadOnlySpan<byte>(utf8Buffer, nameOffsets[a], nameOffsets[a + 1] - nameOffsets[a])
                    .SequenceCompareTo(new ReadOnlySpan<byte>(utf8Buffer, nameOffsets[b], nameOffsets[b + 1] - nameOffsets[b])));
#endif

            // Build the remap: idRemap[originalId] = sortedPosition
            idRemap = new int[count];
            for (int sortedPos = 0; sortedPos < count; sortedPos++)
            {
                idRemap[sortedIndices[sortedPos]] = sortedPos;
            }

            // Compute offsets into the string data area (in sorted order).
            int offsetCount = count + 1;
            Span<int> offsets = offsetCount <= StackAllocThreshold
                ? stackalloc int[offsetCount]
                : new int[offsetCount];
            offsets[0] = 0;
            for (int i = 0; i < count; i++)
            {
                int origIdx = sortedIndices[i];
                offsets[i + 1] = offsets[i] + (nameOffsets[origIdx + 1] - nameOffsets[origIdx]);
            }

            int totalStringBytes = offsets[count];
            int offsetSize = VariantEncodingHelper.ByteWidthForValue(
                Math.Max(totalStringBytes, count)); // offset must also fit dict_size

            // Layout: header(1) + dict_size(offsetSize) + offsets((count+1)*offsetSize) + strings
            int totalSize = 1 + offsetSize + (count + 1) * offsetSize + totalStringBytes;
            byte[] result = new byte[totalSize];

            int pos = 0;

            // Header byte: version=1, sorted=true
            result[pos++] = VariantEncodingHelper.MakeMetadataHeader(sortedStrings: true, offsetSize);

            // Dictionary size
            VariantEncodingHelper.WriteLittleEndianInt(new Span<byte>(result, pos, offsetSize), count, offsetSize);
            pos += offsetSize;

            // Offsets
            for (int i = 0; i <= count; i++)
            {
                VariantEncodingHelper.WriteLittleEndianInt(new Span<byte>(result, pos, offsetSize), offsets[i], offsetSize);
                pos += offsetSize;
            }

            // String bytes (in sorted order, copied from contiguous buffer)
            for (int i = 0; i < count; i++)
            {
                int origIdx = sortedIndices[i];
                int start = nameOffsets[origIdx];
                int length = nameOffsets[origIdx + 1] - start;
                Buffer.BlockCopy(utf8Buffer, start, result, pos, length);
                pos += length;
            }

            return result;
        }

        /// <summary>
        /// Builds the binary metadata. Use this overload when you don't need the ID remap.
        /// </summary>
        public byte[] Build() => Build(out int[] _);
    }
}
