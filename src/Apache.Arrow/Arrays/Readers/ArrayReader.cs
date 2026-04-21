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
using System.Collections;
using System.Collections.Generic;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    // ===================================================================
    // Design B: Generic ArrayReader<T> abstract class
    //
    // Provides a uniform typed reader abstraction with IsNull support,
    // wrapping any encoding transparently. The static Create factory
    // selects the right implementation.
    // ===================================================================

    /// <summary>
    /// Abstract base class providing uniform typed read access over Arrow arrays
    /// regardless of encoding (plain, dictionary-encoded, or run-end encoded).
    /// </summary>
    /// <typeparam name="T">The logical element type.</typeparam>
    public abstract class ArrayReader<T> : IReadOnlyList<T>
    {
        /// <summary>
        /// Gets the logical element at the specified index.
        /// </summary>
        public abstract T this[int index] { get; }

        /// <summary>
        /// Gets the number of logical elements.
        /// </summary>
        public abstract int Count { get; }

        /// <summary>
        /// Returns true if the element at the specified index is null.
        /// </summary>
        public abstract bool IsNull(int index);

        /// <summary>
        /// Gets the underlying Arrow array.
        /// </summary>
        public abstract IArrowArray Array { get; }

        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
                yield return this[i];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>
    /// Provides factory methods for creating <see cref="ArrayReader{T}"/> instances.
    /// </summary>
    public static class ArrayReader
    {
        /// <summary>
        /// Creates an <see cref="ArrayReader{T}"/> of nullable Int32 values
        /// for the given array, regardless of encoding.
        /// </summary>
        public static ArrayReader<int?> GetInt32Reader(IArrowArray array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case Int32Array plain:
                    return new PlainInt32Reader(plain);

                case DictionaryArray dict:
                    ValidateDictionaryValueType(dict, ArrowTypeId.Int32);
                    return new DictionaryInt32Reader(dict);

                case RunEndEncodedArray ree:
                    ValidateReeValueType(ree, ArrowTypeId.Int32);
                    return new ReeInt32Reader(ree);

                default:
                    throw new ArgumentException(
                        $"Cannot create Int32 reader for array of type {array.Data.DataType.TypeId}.",
                        nameof(array));
            }
        }

        /// <summary>
        /// Creates an <see cref="ArrayReader{T}"/> of string values
        /// for the given array, regardless of encoding.
        /// </summary>
        public static ArrayReader<string> GetStringReader(IArrowArray array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case StringArray plain:
                    return new PlainStringReader(plain);

                case DictionaryArray dict:
                    ValidateDictionaryValueType(dict, ArrowTypeId.String);
                    return new DictionaryStringReader(dict);

                case RunEndEncodedArray ree:
                    ValidateReeValueType(ree, ArrowTypeId.String);
                    return new ReeStringReader(ree);

                default:
                    throw new ArgumentException(
                        $"Cannot create String reader for array of type {array.Data.DataType.TypeId}.",
                        nameof(array));
            }
        }

        private static void ValidateDictionaryValueType(DictionaryArray dict, ArrowTypeId expected)
        {
            var dicType = (DictionaryType)dict.Data.DataType;
            if (dicType.ValueType.TypeId != expected)
                throw new ArgumentException(
                    $"Dictionary value type is {dicType.ValueType.TypeId}, expected {expected}.");
        }

        private static void ValidateReeValueType(RunEndEncodedArray ree, ArrowTypeId expected)
        {
            var reeType = (RunEndEncodedType)ree.Data.DataType;
            if (reeType.ValuesDataType.TypeId != expected)
                throw new ArgumentException(
                    $"Run-end encoded value type is {reeType.ValuesDataType.TypeId}, expected {expected}.");
        }
    }

    // ===================================================================
    // Int32 reader implementations
    // ===================================================================

    internal sealed class PlainInt32Reader : ArrayReader<int?>
    {
        private readonly Int32Array _array;

        public PlainInt32Reader(Int32Array array) => _array = array;

        public override IArrowArray Array => _array;
        public override int Count => _array.Length;
        public override bool IsNull(int index) => _array.IsNull(index);
        public override int? this[int index] => _array.GetValue(index);
    }

    internal sealed class DictionaryInt32Reader : ArrayReader<int?>
    {
        private readonly DictionaryArray _dict;
        private readonly Int32Array _values;

        public DictionaryInt32Reader(DictionaryArray dict)
        {
            _dict = dict;
            _values = (Int32Array)dict.Dictionary;
        }

        public override IArrowArray Array => _dict;
        public override int Count => _dict.Length;
        public override bool IsNull(int index) => _dict.IsNull(index);

        public override int? this[int index]
        {
            get
            {
                if (_dict.IsNull(index))
                    return null;

                int dictIndex = DictionaryIndexResolver.GetIndex(_dict.Indices, index);
                return _values.GetValue(dictIndex);
            }
        }
    }

    internal sealed class ReeInt32Reader : ArrayReader<int?>
    {
        private readonly RunEndEncodedArray _ree;
        private readonly Int32Array _values;

        public ReeInt32Reader(RunEndEncodedArray ree)
        {
            _ree = ree;
            _values = (Int32Array)ree.Values;
        }

        public override IArrowArray Array => _ree;
        public override int Count => _ree.Length;

        public override bool IsNull(int index)
        {
            int physicalIndex = _ree.FindPhysicalIndex(index);
            return _values.IsNull(physicalIndex);
        }

        public override int? this[int index]
        {
            get
            {
                int physicalIndex = _ree.FindPhysicalIndex(index);
                return _values.GetValue(physicalIndex);
            }
        }
    }

    // ===================================================================
    // String reader implementations
    // ===================================================================

    internal sealed class PlainStringReader : ArrayReader<string>
    {
        private readonly StringArray _array;

        public PlainStringReader(StringArray array) => _array = array;

        public override IArrowArray Array => _array;
        public override int Count => _array.Length;
        public override bool IsNull(int index) => _array.IsNull(index);
        public override string this[int index] => _array.GetString(index);
    }

    internal sealed class DictionaryStringReader : ArrayReader<string>
    {
        private readonly DictionaryArray _dict;
        private readonly StringArray _values;

        public DictionaryStringReader(DictionaryArray dict)
        {
            _dict = dict;
            _values = (StringArray)dict.Dictionary;
        }

        public override IArrowArray Array => _dict;
        public override int Count => _dict.Length;
        public override bool IsNull(int index) => _dict.IsNull(index);

        public override string this[int index]
        {
            get
            {
                if (_dict.IsNull(index))
                    return null;

                int dictIndex = DictionaryIndexResolver.GetIndex(_dict.Indices, index);
                return _values.GetString(dictIndex);
            }
        }
    }

    internal sealed class ReeStringReader : ArrayReader<string>
    {
        private readonly RunEndEncodedArray _ree;
        private readonly StringArray _values;

        public ReeStringReader(RunEndEncodedArray ree)
        {
            _ree = ree;
            _values = (StringArray)ree.Values;
        }

        public override IArrowArray Array => _ree;
        public override int Count => _ree.Length;

        public override bool IsNull(int index)
        {
            int physicalIndex = _ree.FindPhysicalIndex(index);
            return _values.IsNull(physicalIndex);
        }

        public override string this[int index]
        {
            get
            {
                int physicalIndex = _ree.FindPhysicalIndex(index);
                return _values.GetString(physicalIndex);
            }
        }
    }

    // ===================================================================
    // Shared helpers
    // ===================================================================

    internal static class DictionaryIndexResolver
    {
        public static int GetIndex(IArrowArray indices, int logicalIndex)
        {
            switch (indices)
            {
                case Int8Array i8: return i8.GetValue(logicalIndex) ?? 0;
                case Int16Array i16: return i16.GetValue(logicalIndex) ?? 0;
                case Int32Array i32: return i32.GetValue(logicalIndex) ?? 0;
                case Int64Array i64: return (int)(i64.GetValue(logicalIndex) ?? 0);
                case UInt8Array u8: return u8.GetValue(logicalIndex) ?? 0;
                case UInt16Array u16: return u16.GetValue(logicalIndex) ?? 0;
                case UInt32Array u32: return (int)(u32.GetValue(logicalIndex) ?? 0);
                case UInt64Array u64: return (int)(u64.GetValue(logicalIndex) ?? 0);
                default:
                    throw new InvalidOperationException(
                        $"Unsupported dictionary index type: {indices.Data.DataType.TypeId}");
            }
        }
    }
}
