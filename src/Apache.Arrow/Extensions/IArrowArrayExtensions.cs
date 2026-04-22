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
using Apache.Arrow.Types;

namespace Apache.Arrow
{
    /// <summary>
    /// Provides factory methods that return <see cref="IReadOnlyList{T}"/> views
    /// over Arrow arrays, transparently handling plain, dictionary-encoded,
    /// and run-end encoded layouts.
    /// </summary>
    public static class IArrowArrayExtensions
    {
        /// <summary>
        /// Returns an <see cref="IReadOnlyList{T}"/> view for the given array,
        /// regardless of encoding.
        /// Null slots are represented as <c>default(T)</c>. Callers should use
        /// nullable value types, as that's what the underlying <c>IArrowArray</c> uses.
        /// </summary>
        public static IReadOnlyList<T> AsDecodedReadOnlyList<T>(this IArrowArray array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            switch (array)
            {
                case IReadOnlyList<T> plain:
                    return plain;

                case DictionaryArray dict:
                    IReadOnlyList<T> values = dict.Dictionary as IReadOnlyList<T>;
                    if (values == null)
                        throw new ArgumentException(
                            $"Dictionary value type {dict.Dictionary.Data.DataType.TypeId} cannot be read as {typeof(T).Name}.");
                    return new DictionaryReadOnlyList<T>(dict, values);

                case RunEndEncodedArray ree:
                    IReadOnlyList<T> reeValues = ree.Values as IReadOnlyList<T>;
                    if (reeValues == null)
                        throw new ArgumentException(
                            $"Run-end encoded value type {ree.Values.Data.DataType.TypeId} cannot be read as {typeof(T).Name}.");
                    return new ReeReadOnlyList<T>(ree, reeValues);

                default:
                    throw new ArgumentException(
                        $"Cannot create {typeof(T).Name} reader for array of type {array.Data.DataType.TypeId}.",
                        nameof(array));
            }
        }

        private sealed class DictionaryReadOnlyList<T> : IReadOnlyList<T>
        {
            private readonly IArrowArray _indices;
            private readonly IReadOnlyList<T> _values;
            private readonly Func<IArrowArray, int, int> _indexLookup;

            public DictionaryReadOnlyList(DictionaryArray dict, IReadOnlyList<T> values)
            {
                _indices = dict.Indices;
                _values = values;
                _indexLookup = GetDictionaryIndex(dict.Indices.Data.DataType);
            }

            public int Count => _indices.Length;

            public T this[int index]
            {
                get
                {
                    if (index < 0 || index >= _indices.Length)
                    {
                        throw new ArgumentOutOfRangeException(nameof(index));
                    }

                    if (_indices.IsNull(index))
                        return default;

                    int dictIndex = _indexLookup(_indices, index);
                    return _values[dictIndex];
                }
            }

            public IEnumerator<T> GetEnumerator()
            {
                for (int i = 0; i < Count; i++)
                    yield return this[i];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private sealed class ReeReadOnlyList<T> : IReadOnlyList<T>
        {
            private readonly RunEndEncodedArray _ree;
            private readonly IReadOnlyList<T> _values;

            public ReeReadOnlyList(RunEndEncodedArray ree, IReadOnlyList<T> values)
            {
                _ree = ree;
                _values = values;
            }

            public int Count => _ree.Length;

            public T this[int index]
            {
                get
                {
                    int physicalIndex = _ree.FindPhysicalIndex(index);
                    return _values[physicalIndex];
                }
            }

            public IEnumerator<T> GetEnumerator()
            {
                foreach (int physicalIndex in _ree.EnumeratePhysicalIndices())
                    yield return _values[physicalIndex];
            }

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private static Func<IArrowArray, int, int> GetDictionaryIndex(IArrowType type)
        {
            switch (type.TypeId)
            {
                case ArrowTypeId.Int8: return (array, logicalIndex) => ((Int8Array)array).GetValue(logicalIndex) ?? 0;
                case ArrowTypeId.Int16: return (array, logicalIndex) => ((Int16Array)array).GetValue(logicalIndex) ?? 0;
                case ArrowTypeId.Int32: return (array, logicalIndex) => ((Int32Array)array).GetValue(logicalIndex) ?? 0;
                case ArrowTypeId.Int64: return (array, logicalIndex) => checked((int)(((Int64Array)array).GetValue(logicalIndex) ?? 0));
                case ArrowTypeId.UInt8: return (array, logicalIndex) => ((UInt8Array)array).GetValue(logicalIndex) ?? 0;
                case ArrowTypeId.UInt16: return (array, logicalIndex) => ((UInt16Array)array).GetValue(logicalIndex) ?? 0;
                case ArrowTypeId.UInt32: return (array, logicalIndex) => checked((int)(((UInt32Array)array).GetValue(logicalIndex) ?? 0));
                case ArrowTypeId.UInt64: return (array, logicalIndex) => checked((int)(((UInt64Array)array).GetValue(logicalIndex) ?? 0));
                default:
                    throw new InvalidOperationException(
                        $"Unsupported dictionary index type: {type.TypeId}");
            }
        }
    }
}
