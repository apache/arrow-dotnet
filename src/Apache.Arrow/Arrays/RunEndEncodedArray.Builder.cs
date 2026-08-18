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
using Apache.Arrow.Memory;

namespace Apache.Arrow;

public partial class RunEndEncodedArray
{
    /// <summary>
    /// Builder for <see cref="RunEndEncodedArray"/>.
    /// </summary>
    /// <typeparam name="TRunEndBuilder">The type of the run ends array builder.</typeparam>
    /// <typeparam name="TValueBuilder">The type of the values array builder.</typeparam>
    /// <typeparam name="TRunEndArray">The type of the run ends array (must be Int16Array, Int32Array, or Int64Array).</typeparam>
    /// <typeparam name="TValueArray">The type of the values array.</typeparam>
    /// <typeparam name="TValue">The type of values contained in the values array.</typeparam>
    public class Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue>
        : IArrowArrayBuilder<TValue, RunEndEncodedArray, Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue>>
        where TRunEndBuilder : IArrowArrayBuilder<TRunEndArray>
        where TValueBuilder : IArrowArrayBuilder<TValueArray>
        where TRunEndArray : IArrowArray
        where TValueArray : IArrowArray
    {
        private readonly IEqualityComparer<TValue> _comparer;
        private int _length;
        private TValue _lastValue;
        private bool _lastValueIsNull;
        private bool _hasValue;

        /// <summary>
        /// Gets the run ends builder.
        /// </summary>
        public TRunEndBuilder RunEndsBuilder { get; }

        /// <summary>
        /// Gets the values builder.
        /// </summary>
        public TValueBuilder ValuesBuilder { get; }

        /// <summary>
        /// Gets the total logical length of elements appended to this builder.
        /// </summary>
        public int Length => _length;

        /// <summary>
        /// Initializes a new instance of the <see cref="Builder{TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue}"/> class.
        /// </summary>
        /// <param name="runEndsBuilder">The builder to use for run-ends. Must be a builder for an Int16, Int32 or Int64 array.</param>
        /// <param name="valuesBuilder">The builder to use for values.</param>
        /// <param name="comparer">Optional equality comparer for value run-length grouping.</param>
        public Builder(TRunEndBuilder runEndsBuilder, TValueBuilder valuesBuilder, IEqualityComparer<TValue> comparer = null)
        {
            RunEndsBuilder = runEndsBuilder ?? throw new ArgumentNullException(nameof(runEndsBuilder));
            ValuesBuilder = valuesBuilder ?? throw new ArgumentNullException(nameof(valuesBuilder));
            _comparer = comparer ?? EqualityComparer<TValue>.Default;
        }

        /// <summary>
        /// Appends a single value to the builder, automatically grouping identical consecutive values into runs.
        /// </summary>
        /// <param name="value">The value to append.</param>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Append(TValue value)
        {
            if (value is null)
            {
                return AppendNull();
            }

            if (_hasValue)
            {
                if (!_lastValueIsNull && _comparer.Equals(value, _lastValue))
                {
                    checked
                    {
                        _length++;
                    }
                }
                else
                {
                    FlushCurrentRun();
                    StartNewRun(value, isNull: false);
                }
            }
            else
            {
                StartNewRun(value, isNull: false);
            }

            return this;
        }

        /// <summary>
        /// Appends a null value to the builder.
        /// </summary>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> AppendNull()
        {
            if (_hasValue)
            {
                if (_lastValueIsNull)
                {
                    checked
                    {
                        _length++;
                    }
                }
                else
                {
                    FlushCurrentRun();
                    StartNewRun(default, isNull: true);
                }
            }
            else
            {
                StartNewRun(default, isNull: true);
            }

            return this;
        }

        /// <summary>
        /// Appends a span of values to the builder.
        /// </summary>
        /// <param name="span">The span of values to append.</param>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Append(ReadOnlySpan<TValue> span)
        {
            foreach (TValue value in span)
            {
                Append(value);
            }
            return this;
        }

        /// <summary>
        /// Appends a sequence of values to the builder.
        /// </summary>
        /// <param name="values">The sequence of values to append.</param>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> AppendRange(IEnumerable<TValue> values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            foreach (TValue value in values)
            {
                Append(value);
            }
            return this;
        }

        /// <summary>
        /// Validates the capacity argument. Does not preallocate inner builders to allow doubling growth strategy with REE compression.
        /// </summary>
        /// <param name="capacity">The capacity to validate.</param>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Reserve(int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            return this;
        }

        /// <summary>
        /// Resizing is not supported for RunEndEncodedArray.Builder.
        /// </summary>
        /// <param name="length">The target length.</param>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Resize(int length)
        {
            throw new NotSupportedException("Resize is not supported on RunEndEncodedArray.Builder.");
        }

        /// <summary>
        /// Clears the state of the builder and inner builders.
        /// </summary>
        /// <returns>The builder instance for method chaining.</returns>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Clear()
        {
            _length = 0;
            _lastValue = default;
            _lastValueIsNull = false;
            _hasValue = false;
            ClearBuilder<TRunEndArray, TRunEndBuilder>(RunEndsBuilder);
            ClearBuilder<TValueArray, TValueBuilder>(ValuesBuilder);
            return this;
        }

        /// <summary>
        /// Swapping elements is not supported for RunEndEncodedArray.Builder.
        /// </summary>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Swap(int i, int j)
        {
            throw new NotSupportedException("Swap is not supported on RunEndEncodedArray.Builder.");
        }

        /// <summary>
        /// Setting elements at specific indices is not supported for RunEndEncodedArray.Builder.
        /// </summary>
        public Builder<TRunEndBuilder, TValueBuilder, TRunEndArray, TValueArray, TValue> Set(int index, TValue value)
        {
            throw new NotSupportedException("Set is not supported on RunEndEncodedArray.Builder.");
        }

        /// <summary>
        /// Flushes any pending run and builds the <see cref="RunEndEncodedArray"/>.
        /// </summary>
        /// <param name="allocator">Optional memory allocator.</param>
        /// <returns>The constructed <see cref="RunEndEncodedArray"/>.</returns>
        public RunEndEncodedArray Build(MemoryAllocator allocator = default)
        {
            if (_hasValue)
            {
                FlushCurrentRun();
            }

            TRunEndArray runEnds = RunEndsBuilder.Build(allocator);
            TValueArray values = ValuesBuilder.Build(allocator);

            return new RunEndEncodedArray(runEnds, values);
        }

        private void StartNewRun(TValue value, bool isNull)
        {
            _lastValue = value;
            _lastValueIsNull = isNull;
            _hasValue = true;
            checked
            {
                _length++;
            }
        }

        private void FlushCurrentRun()
        {
            if (!_hasValue)
            {
                return;
            }

            AppendRunEndToRunEndsBuilder(RunEndsBuilder, _length);

            if (_lastValueIsNull)
            {
                AppendNullToValuesBuilder(ValuesBuilder);
            }
            else
            {
                AppendValueToValuesBuilder(ValuesBuilder, _lastValue);
            }

            _hasValue = false;
        }

        private static void AppendRunEndToRunEndsBuilder(TRunEndBuilder runEndsBuilder, int runEnd)
        {
            if (runEndsBuilder is Int32Array.Builder b32)
            {
                b32.Append(runEnd);
            }
            else if (runEndsBuilder is Int16Array.Builder b16)
            {
                b16.Append(checked((short)runEnd));
            }
            else if (runEndsBuilder is Int64Array.Builder b64)
            {
                b64.Append(runEnd);
            }
            else if (runEndsBuilder is IArrowArrayBuilder<int, TRunEndArray, IArrowArrayBuilder<TRunEndArray>> bInt)
            {
                bInt.Append(runEnd);
            }
            else if (runEndsBuilder is IArrowArrayBuilder<short, TRunEndArray, IArrowArrayBuilder<TRunEndArray>> bShort)
            {
                bShort.Append(checked((short)runEnd));
            }
            else if (runEndsBuilder is IArrowArrayBuilder<long, TRunEndArray, IArrowArrayBuilder<TRunEndArray>> bLong)
            {
                bLong.Append(runEnd);
            }
            else
            {
                throw new NotSupportedException($"Run ends builder type '{typeof(TRunEndBuilder).Name}' is not supported.");
            }
        }

        private static void AppendNullToValuesBuilder(TValueBuilder valuesBuilder)
        {
            if (valuesBuilder is IArrowArrayBuilder<TValueArray, IArrowArrayBuilder<TValueArray>> builder)
            {
                builder.AppendNull();
            }
            else if (valuesBuilder is StringArray.Builder sb)
            {
                sb.AppendNull();
            }
            else if (valuesBuilder is LargeStringArray.Builder lsb)
            {
                lsb.AppendNull();
            }
            else if (valuesBuilder is StringViewArray.Builder svb)
            {
                svb.AppendNull();
            }
            else if (valuesBuilder is BinaryArray.Builder bb)
            {
                bb.AppendNull();
            }
            else if (valuesBuilder is LargeBinaryArray.Builder lbb)
            {
                lbb.AppendNull();
            }
            else if (valuesBuilder is BinaryViewArray.Builder bvb)
            {
                bvb.AppendNull();
            }
            else
            {
                throw new NotSupportedException($"Appending null to values builder type '{typeof(TValueBuilder).Name}' is not supported.");
            }
        }

        private static void AppendValueToValuesBuilder(TValueBuilder valuesBuilder, TValue value)
        {
            if (valuesBuilder is IArrowArrayBuilder<TValue, TValueArray, IArrowArrayBuilder<TValueArray>> builder)
            {
                builder.Append(value);
            }
            else if (valuesBuilder is StringArray.Builder sb && value is string s)
            {
                sb.Append(s);
            }
            else if (valuesBuilder is LargeStringArray.Builder lsb && value is string ls)
            {
                lsb.Append(ls);
            }
            else if (valuesBuilder is StringViewArray.Builder svb && value is string svs)
            {
                svb.Append(svs);
            }
            else if (valuesBuilder is BinaryArray.Builder bb && value is byte[] b)
            {
                bb.Append((ReadOnlySpan<byte>)b);
            }
            else if (valuesBuilder is LargeBinaryArray.Builder lbb && value is byte[] lb)
            {
                lbb.Append((ReadOnlySpan<byte>)lb);
            }
            else if (valuesBuilder is BinaryViewArray.Builder bvb && value is byte[] bv)
            {
                bvb.Append((ReadOnlySpan<byte>)bv);
            }
            else
            {
                throw new NotSupportedException($"Appending to values builder type '{typeof(TValueBuilder).Name}' with value type '{typeof(TValue).Name}' is not supported.");
            }
        }


        private static void ClearBuilder<TArray, TBuilder>(TBuilder builder)
            where TArray : IArrowArray
            where TBuilder : IArrowArrayBuilder<TArray>
        {
            if (builder is IArrowArrayBuilder<TArray, IArrowArrayBuilder<TArray>> b)
            {
                b.Clear();
            }
            else if (builder is StringArray.Builder sb)
            {
                sb.Clear();
            }
            else if (builder is LargeStringArray.Builder lsb)
            {
                lsb.Clear();
            }
            else if (builder is StringViewArray.Builder svb)
            {
                svb.Clear();
            }
            else if (builder is BinaryArray.Builder bb)
            {
                bb.Clear();
            }
            else if (builder is LargeBinaryArray.Builder lbb)
            {
                lbb.Clear();
            }
            else if (builder is BinaryViewArray.Builder bvb)
            {
                bvb.Clear();
            }
            else
            {
                throw new NotSupportedException($"Clearing builder type '{typeof(TBuilder).Name}' is not supported.");
            }
        }
    }
}

