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
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Internal helpers shared by the shredded-variant reader trio.
    /// </summary>
    internal static class ShreddingHelpers
    {
        /// <summary>
        /// Builds a <see cref="ShreddedVariant"/> slot for the given index of an element-group
        /// struct (one with <c>value</c> and/or <c>typed_value</c> sub-fields). Either sub-field
        /// may be absent from the struct.
        /// </summary>
        public static ShreddedVariant BuildSlot(
            ShredSchema slotSchema,
            ReadOnlySpan<byte> metadata,
            StructArray elementGroup,
            int index)
        {
            StructType elementGroupType = (StructType)elementGroup.Data.DataType;
            int valueIdx = elementGroupType.GetFieldIndex("value");
            int typedIdx = elementGroupType.GetFieldIndex("typed_value");

            IArrowArray valueArr = valueIdx >= 0 ? elementGroup.Fields[valueIdx] : null;
            IArrowArray typedArr = typedIdx >= 0 ? elementGroup.Fields[typedIdx] : null;

            return new ShreddedVariant(slotSchema, metadata, valueArr, typedArr, index);
        }

        /// <summary>
        /// Reads the bytes at <paramref name="index"/> from any binary representation a
        /// variant column may use (binary, large_binary or binary_view).
        /// </summary>
        public static ReadOnlySpan<byte> GetBytes(IArrowArray array, int index)
        {
            switch (array)
            {
                case BinaryArray binary: return binary.GetBytes(index);
                case LargeBinaryArray largeBinary: return largeBinary.GetBytes(index);
                case BinaryViewArray binaryView: return binaryView.GetBytes(index);
                default:
                    throw new InvalidOperationException(
                        $"Cannot read variant bytes from an array of type {array.Data.DataType.TypeId}.");
            }
        }

        /// <summary>
        /// Reads the string at <paramref name="index"/> from any string representation
        /// (utf8, large_utf8 or utf8_view).
        /// </summary>
        public static string GetString(IArrowArray array, int index)
        {
            switch (array)
            {
                case StringArray str: return str.GetString(index);
                case LargeStringArray largeStr: return largeStr.GetString(index);
                case StringViewArray strView: return strView.GetString(index);
                default:
                    throw new InvalidOperationException(
                        $"Cannot read a string from an array of type {array.Data.DataType.TypeId}.");
            }
        }
    }
}
