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
using System.Linq;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public abstract class PrimitiveArrayBuilder<TFrom, TTo, TArray, TBuilder> : IArrowArrayBuilder<TFrom, TArray, TBuilder>, IDisposable
        where TTo : struct
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        protected TBuilder Instance => this as TBuilder;
        protected IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TArray>> ArrayBuilder { get; }

        public int Length => ArrayBuilder.Length;

        public PrimitiveArrayBuilder(IArrowArrayBuilder<TTo, TArray, IArrowArrayBuilder<TArray>> builder)
        {
            ArrayBuilder = builder ?? throw new ArgumentNullException(nameof(builder));
        }

        /// <summary>
        /// Releases the builder's working memory. Arrays already built are unaffected.
        /// </summary>
        /// <remarks>
        /// Disposal is required only when the working memory comes from an allocator whose
        /// <see cref="MemoryAllocator.Lifetime"/> is <see cref="AllocationLifetime.CallerOwned"/>;
        /// otherwise it only releases memory sooner.
        /// </remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                (ArrayBuilder as IDisposable)?.Dispose();
            }
        }

        public TArray Build(MemoryAllocator allocator = default) => ArrayBuilder.Build(allocator);

        public TBuilder Append(TFrom value)
        {
            ArrayBuilder.Append(ConvertTo(value));
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<TFrom> span)
        {
            ArrayBuilder.Reserve(span.Length);
            foreach (TFrom value in span)
            {
                Append(value);
            }
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<TFrom> values)
        {
            ArrayBuilder.AppendRange(values.Select(ConvertTo));
            return Instance;
        }

        public TBuilder AppendNull()
        {
            ArrayBuilder.AppendNull();
            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ArrayBuilder.Reserve(capacity);
            return Instance;
        }

        public TBuilder Resize(int length)
        {
            ArrayBuilder.Resize(length);
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            ArrayBuilder.Swap(i, j);
            return Instance;
        }

        public TBuilder Set(int index, TFrom value)
        {
            ArrayBuilder.Set(index, ConvertTo(value));
            return Instance;
        }

        public TBuilder Clear()
        {
            ArrayBuilder.Clear();
            return Instance;
        }

        protected abstract TTo ConvertTo(TFrom value);
    }

    public abstract class PrimitiveArrayBuilder<T, TArray, TBuilder> : IArrowArrayBuilder<T, TArray, TBuilder>, IDisposable
        where T : struct
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<TArray>
    {
        // Created when the first null is appended. Until then every element is valid, which
        // is what an empty validity buffer means, so arrays without nulls never allocate one.
        private ArrowBuffer.BitmapBuilder _validity;

        // Kept so that a bitmap created later uses the same allocator as the values. May be
        // null, which means managed working memory copied into the default allocator on Build.
        private readonly MemoryAllocator _allocator;

        protected TBuilder Instance => this as TBuilder;
        protected ArrowBuffer.Builder<T> ValueBuffer { get; }

        /// <summary>
        /// The validity bitmap. Reading this creates the bitmap if no null has been appended
        /// yet, marking every element so far as valid.
        /// </summary>
        protected ArrowBuffer.BitmapBuilder ValidityBuffer => _validity ??= CreateValidityBuffer();

        public int Length => ValueBuffer.Length;
        protected int NullCount => _validity?.UnsetBitCount ?? 0;

        // The initial capacity builders have always had, matching ArrowBuffer.Builder<T>.
        private const int DefaultCapacity = 8;

        // The smallest bitmap to create, in bits: BitmapBuilder's own default. Sizing the bitmap
        // only to the value capacity would start it at a single byte and make it grow repeatedly.
        private const int MinimumValidityCapacity = 64;

        /// <summary>
        /// Creates a builder with room for a few values, which grows as values are appended.
        /// </summary>
        public PrimitiveArrayBuilder()
            : this(DefaultCapacity, null)
        {
        }

        /// <summary>
        /// Creates a builder with room for <paramref name="capacity"/> values. A builder given
        /// its final length allocates once, and that memory becomes the built array's memory.
        /// </summary>
        /// <param name="capacity">Number of values to allocate room for.</param>
        /// <param name="allocator">
        /// Allocator for the builder's working memory, which usually becomes the built array's
        /// memory. When <c>null</c>, the builder works in managed memory and copies
        /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
        /// </param>
        protected PrimitiveArrayBuilder(int capacity, MemoryAllocator allocator = null)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            _allocator = allocator;
            ValueBuffer = new ArrowBuffer.Builder<T>(capacity, _allocator);
        }

        public TBuilder Resize(int length)
        {
            if (length > ValueBuffer.Length)
            {
                // Elements added by growing are null, so the bitmap is needed to record that.
                ValidityBuffer.Resize(length);
            }
            else
            {
                _validity?.Resize(length);
            }

            ValueBuffer.Resize(length);
            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ValueBuffer.Reserve(capacity);
            _validity?.Reserve(capacity);
            return Instance;
        }

        public TBuilder Append(T value)
        {
            ValueBuffer.Append(value);
            _validity?.Append(true);
            return Instance;
        }

        public TBuilder Append(T? value) =>
            (value == null) ? AppendNull() : Append(value.Value);

        public TBuilder Append(ReadOnlySpan<T> span)
        {
            ValueBuffer.Append(span);
            _validity?.AppendRange(true, span.Length);
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<T> values)
        {
            int len = ValueBuffer.Length;
            ValueBuffer.AppendRange(values);
            _validity?.AppendRange(true, ValueBuffer.Length - len);
            return Instance;
        }

        public TBuilder AppendNull()
        {
            ValidityBuffer.Append(false);
            ValueBuffer.Append(default(T));
            return Instance;
        }

        /// <summary>
        /// Returns a span of exactly <paramref name="count"/> values just past the end of the
        /// builder, for writing values directly. Call <see cref="Advance"/> afterwards to
        /// append the values written.
        /// </summary>
        /// <remarks>
        /// The span's initial contents are unspecified; write every value that will be
        /// advanced over. The span is invalidated by any other call on the builder.
        /// </remarks>
        public Span<T> GetSpan(int count) => ValueBuffer.GetSpan(count);

        /// <summary>
        /// Appends <paramref name="count"/> non-null values previously written to the span
        /// returned by <see cref="GetSpan"/>.
        /// </summary>
        public TBuilder Advance(int count)
        {
            ValueBuffer.Advance(count);
            _validity?.AppendRange(true, count);
            return Instance;
        }

        public TBuilder Clear()
        {
            ValueBuffer.Clear();
            _validity?.Clear();
            return Instance;
        }

        public TBuilder Set(int index, T value)
        {
            CheckIndex(index);
            ValueBuffer.Span[index] = value;
            _validity?.Set(index, true);
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            CheckIndex(i);
            CheckIndex(j);
            Span<T> values = ValueBuffer.Span;
            T x = values[i];
            values[i] = values[j];
            values[j] = x;
            _validity?.Swap(i, j);
            return Instance;
        }

        /// <summary>
        /// Releases the builder's working memory. Arrays already built are unaffected.
        /// </summary>
        /// <remarks>
        /// Disposal is required only when the working memory comes from an allocator whose
        /// <see cref="MemoryAllocator.Lifetime"/> is <see cref="AllocationLifetime.CallerOwned"/>;
        /// otherwise it only releases memory sooner.
        /// </remarks>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                ValueBuffer.Dispose();
                _validity?.Dispose();
            }
        }

        public TArray Build(MemoryAllocator allocator = default)
        {
            ArrowBuffer validityBuffer = NullCount > 0
                                    ? _validity.Build(allocator)
                                    : ArrowBuffer.Empty;

            return Build(
                ValueBuffer.Build(allocator), validityBuffer,
                ValueBuffer.Length, NullCount, 0);
        }

        protected abstract TArray Build(
            ArrowBuffer valueBuffer, ArrowBuffer nullBitmapBuffer,
            int length, int nullCount, int offset);

        private ArrowBuffer.BitmapBuilder CreateValidityBuffer()
        {
            ArrowBuffer.BitmapBuilder validity = new ArrowBuffer.BitmapBuilder(
                Math.Max(ValueBuffer.Capacity, MinimumValidityCapacity), _allocator);
            validity.AppendRange(true, ValueBuffer.Length);
            return validity;
        }

        private void CheckIndex(int index)
        {
            if ((uint)index >= (uint)ValueBuffer.Length)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
        }
    }
}
