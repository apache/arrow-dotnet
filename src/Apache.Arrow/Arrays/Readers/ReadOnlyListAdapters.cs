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
    // Design A: IReadOnlyList<T> wrappers via static factory methods
    //
    // Plain arrays already implement IReadOnlyList<T?>/IReadOnlyList<string>,
    // so this approach returns them directly with zero overhead.
    // Dictionary and REE arrays get lightweight wrappers.
    // ===================================================================

    /// <summary>
    /// Provides factory methods that return <see cref="IReadOnlyList{T}"/> views
    /// over Arrow arrays, transparently handling plain, dictionary-encoded,
    /// and run-end encoded layouts.
    /// </summary>
    public static class ReadOnlyListAdapters
    {
        /// <summary>
        /// Returns an <see cref="IReadOnlyList{T}"/> of nullable Int32 values
        /// for the given array, regardless of encoding.
        /// </summary>
        public static IReadOnlyList<int?> AsInt32ReadOnlyList(this IArrowArray array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case Int32Array plain:
                    return plain;

                case DictionaryArray dict:
                    ValidateDictionaryValueType(dict, ArrowTypeId.Int32);
                    return new DictionaryInt32ReadOnlyList(dict);

                case RunEndEncodedArray ree:
                    ValidateReeValueType(ree, ArrowTypeId.Int32);
                    return new ReeInt32ReadOnlyList(ree);

                default:
                    throw new ArgumentException(
                        $"Cannot create Int32 reader for array of type {array.Data.DataType.TypeId}.",
                        nameof(array));
            }
        }

        /// <summary>
        /// Returns an <see cref="IReadOnlyList{T}"/> of string values
        /// for the given array, regardless of encoding.
        /// </summary>
        public static IReadOnlyList<string> AsStringReadOnlyList(this IArrowArray array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case StringArray plain:
                    return plain;

                case DictionaryArray dict:
                    ValidateDictionaryValueType(dict, ArrowTypeId.String);
                    return new DictionaryStringReadOnlyList(dict);

                case RunEndEncodedArray ree:
                    ValidateReeValueType(ree, ArrowTypeId.String);
                    return new ReeStringReadOnlyList(ree);

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

        // ---------------------------------------------------------------
        // Dictionary wrappers
        // ---------------------------------------------------------------

        private sealed class DictionaryInt32ReadOnlyList : IReadOnlyList<int?>
        {
            private readonly DictionaryArray _dict;
            private readonly Int32Array _values;

            public DictionaryInt32ReadOnlyList(DictionaryArray dict)
            {
                _dict = dict;
                _values = (Int32Array)dict.Dictionary;
            }

            public int Count => _dict.Length;

            public int? this[int index]
            {
                get
                {
                    if (_dict.IsNull(index))
                        return null;

                    int dictIndex = GetDictionaryIndex(_dict.Indices, index);
                    return _values.GetValue(dictIndex);
                }
            }

            public IEnumerator<int?> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                    yield return this[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private sealed class DictionaryStringReadOnlyList : IReadOnlyList<string>
        {
            private readonly DictionaryArray _dict;
            private readonly StringArray _values;

            public DictionaryStringReadOnlyList(DictionaryArray dict)
            {
                _dict = dict;
                _values = (StringArray)dict.Dictionary;
            }

            public int Count => _dict.Length;

            public string this[int index]
            {
                get
                {
                    if (_dict.IsNull(index))
                        return null;

                    int dictIndex = GetDictionaryIndex(_dict.Indices, index);
                    return _values.GetString(dictIndex);
                }
            }

            public IEnumerator<string> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                    yield return this[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        // ---------------------------------------------------------------
        // REE wrappers
        // ---------------------------------------------------------------

        private sealed class ReeInt32ReadOnlyList : IReadOnlyList<int?>
        {
            private readonly RunEndEncodedArray _ree;
            private readonly Int32Array _values;

            public ReeInt32ReadOnlyList(RunEndEncodedArray ree)
            {
                _ree = ree;
                _values = (Int32Array)ree.Values;
            }

            public int Count => _ree.Length;

            public int? this[int index]
            {
                get
                {
                    int physicalIndex = _ree.FindPhysicalIndex(index);
                    return _values.GetValue(physicalIndex);
                }
            }

            public IEnumerator<int?> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                    yield return this[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private sealed class ReeStringReadOnlyList : IReadOnlyList<string>
        {
            private readonly RunEndEncodedArray _ree;
            private readonly StringArray _values;

            public ReeStringReadOnlyList(RunEndEncodedArray ree)
            {
                _ree = ree;
                _values = (StringArray)ree.Values;
            }

            public int Count => _ree.Length;

            public string this[int index]
            {
                get
                {
                    int physicalIndex = _ree.FindPhysicalIndex(index);
                    return _values.GetString(physicalIndex);
                }
            }

            public IEnumerator<string> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                    yield return this[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        // ---------------------------------------------------------------
        // Dictionary index resolution
        // ---------------------------------------------------------------

        private static int GetDictionaryIndex(IArrowArray indices, int logicalIndex)
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
