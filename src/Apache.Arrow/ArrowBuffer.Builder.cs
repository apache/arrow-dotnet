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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;

namespace Apache.Arrow
{
    public partial struct ArrowBuffer
    {
        /// <summary>
        /// The <see cref="Builder{T}"/> class is able to append value-type items, with fluent-style methods, to build
        /// up an <see cref="ArrowBuffer"/> of contiguous items.
        /// </summary>
        /// <remarks>
        /// Note that <see cref="bool"/> is not supported as a generic type argument for this class.  Please use
        /// <see cref="BitmapBuilder"/> instead.
        /// </remarks>
        /// <typeparam name="T">Value-type of item to build into a buffer.</typeparam>
        public class Builder<T> : IDisposable
            where T : struct
        {
            private const int DefaultCapacity = 8;

            private readonly BuilderMemory _buffer;

            // Unsafe.SizeOf<T>() is a JIT-time constant, so dividing by it compiles to a shift.
            // A field holding the same value would cost an integer divide on every append.
            private static int Size
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Unsafe.SizeOf<T>();
            }

            /// <summary>
            /// Gets the number of items that can be contained in the memory allocated by the current instance.
            /// </summary>
            public int Capacity => _buffer.Capacity / Size;

            /// <summary>
            /// Gets the number of items currently appended.
            /// </summary>
            public int Length { get; private set; }

            /// <summary>
            /// Gets the raw byte memory underpinning the builder.
            /// </summary>
            /// <remarks>
            /// If this memory was handed to a buffer by <see cref="Build(MemoryAllocator)"/>,
            /// reading this property first copies it, so that writes through the result can
            /// never modify that buffer.
            /// </remarks>
            public Memory<byte> Memory => _buffer.Memory;

            /// <summary>
            /// Gets the span of memory underpinning the builder.
            /// </summary>
            public Span<T> Span
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => Memory.Span.CastTo<T>();
            }

            /// <summary>
            /// Creates an instance of the <see cref="Builder{T}"/> class.
            /// </summary>
            /// <param name="capacity">Number of items of initial capacity to reserve.</param>
            public Builder(int capacity = DefaultCapacity)
                : this(capacity, null)
            {
            }

            /// <summary>
            /// Creates an instance of the <see cref="Builder{T}"/> class.
            /// </summary>
            /// <param name="capacity">Number of items of initial capacity to reserve.</param>
            /// <param name="allocator">
            /// Allocator used for the builder's working memory, which by default also becomes
            /// the memory of the buffer that <see cref="Build(MemoryAllocator)"/> returns.
            /// When <c>null</c>, the builder works in managed memory and copies
            /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
            /// </param>
            /// <remarks>
            /// When an allocator is given, capacity is rounded up so that the working memory is a
            /// multiple of 64 bytes. A builder created with enough capacity for all of its items
            /// therefore allocates exactly once.
            /// </remarks>
            public Builder(int capacity, MemoryAllocator allocator)
            {
                // Using `bool` as the template argument, if used in an unrestricted fashion, would result in a buffer
                // with inappropriate contents being produced.  Because C# does not support template specialisation,
                // and because generic type constraints do not support negation, we will throw a runtime error to
                // indicate that such a template type is not supported.
                if (typeof(T) == typeof(bool))
                {
                    throw new NotSupportedException(
                        $"An instance of {nameof(Builder<T>)} cannot be instantiated, as `bool` is not an " +
                        $"appropriate generic type to use with this class - please use {nameof(BitmapBuilder)} " +
                        $"instead");
                }

                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity));
                }

                _buffer = new BuilderMemory(allocator, checked(capacity * Size));
                Length = 0;
            }

            /// <summary>
            /// Append a buffer, assumed to contain items of the same type.
            /// </summary>
            /// <param name="buffer">Buffer to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(ArrowBuffer buffer)
            {
                Append(buffer.Span.CastTo<T>());
                return this;
            }

            /// <summary>
            /// Append a single item.
            /// </summary>
            /// <param name="value">Item to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(T value)
            {
                EnsureAdditionalCapacity(1);
                Span[Length++] = value;
                return this;
            }

            /// <summary>
            /// Append a span of items.
            /// </summary>
            /// <param name="source">Source of item span.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Append(ReadOnlySpan<T> source)
            {
                // Writing can release the memory backing the builder: growing releases the working
                // memory, and the first write after a build releases the builder's reference to the
                // built buffer. A source that points into that memory must be copied first. The check
                // reads the memory without detaching, since detaching would itself release it.
                if ((source.Length > Capacity - Length || _buffer.IsShared) &&
                    MemoryMarshal.AsBytes(source).Overlaps(_buffer.CurrentBytes))
                {
                    source = source.ToArray();
                }

                EnsureAdditionalCapacity(source.Length);
                source.CopyTo(Span.Slice(Length, source.Length));
                Length += source.Length;
                return this;
            }

            /// <summary>
            /// Append a number of items.
            /// </summary>
            /// <param name="values">Items to append.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> AppendRange(IEnumerable<T> values)
            {
                if (values != null)
                {
                    if (values is ICollection<T> collection)
                    {
                        EnsureAdditionalCapacity(collection.Count);
                    }
                    else if (values is IReadOnlyCollection<T> readOnlyCollection)
                    {
                        EnsureAdditionalCapacity(readOnlyCollection.Count);
                    }

                    foreach (T v in values)
                    {
                        Append(v);
                    }
                }

                return this;
            }

            /// <summary>
            /// Reserve a given number of items' additional capacity.
            /// </summary>
            /// <param name="additionalCapacity">Number of items of required additional capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Reserve(int additionalCapacity)
            {
                if (additionalCapacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(additionalCapacity));
                }

                EnsureAdditionalCapacity(additionalCapacity);
                return this;
            }

            /// <summary>
            /// Returns a span of exactly <paramref name="count"/> items just past the end of the
            /// appended contents, growing the buffer if needed. Call <see cref="Advance"/>
            /// afterwards to append the items written.
            /// </summary>
            /// <remarks>
            /// The span's initial contents are unspecified; write every item that will be
            /// advanced over. The span is invalidated by any other call on the builder.
            /// </remarks>
            /// <param name="count">Number of items to be written.</param>
            public Span<T> GetSpan(int count)
            {
                if (count < 0)
                {
                    ThrowArgumentOutOfRange(nameof(count));
                }

                EnsureAdditionalCapacity(count);
                return Span.Slice(Length, count);
            }

            /// <summary>
            /// Appends <paramref name="count"/> items previously written to the span returned by
            /// <see cref="GetSpan"/>.
            /// </summary>
            /// <param name="count">Number of items written.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Advance(int count)
            {
                // Unsigned comparison covers count < 0 as well.
                if ((uint)count > (uint)(Capacity - Length))
                {
                    ThrowArgumentOutOfRange(nameof(count));
                }

                Length += count;
                return this;
            }

            // Kept out of line so that the methods that throw stay small enough to inline.
            [MethodImpl(MethodImplOptions.NoInlining)]
            private static void ThrowArgumentOutOfRange(string paramName) =>
                throw new ArgumentOutOfRangeException(paramName);

            /// <summary>
            /// Resize the buffer to a given size.
            /// </summary>
            /// <remarks>
            /// Note that if the required capacity is larger than the current length of the populated buffer so far,
            /// the buffer's contents in the new, expanded region are undefined.
            /// </remarks>
            /// <remarks>
            /// Note that if the required capacity is smaller than the current length of the populated buffer so far,
            /// the buffer will be truncated and items at the end of the buffer will be lost.
            /// </remarks>
            /// <param name="capacity">Number of items of required capacity.</param>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Resize(int capacity)
            {
                if (capacity < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be non-negative");
                }

                EnsureCapacity(capacity);
                Length = capacity;

                return this;
            }

            /// <summary>
            /// Clear all contents appended so far.
            /// </summary>
            /// <returns>Returns the builder (for fluent-style composition).</returns>
            public Builder<T> Clear()
            {
                _buffer.Reset();
                Length = 0;
                return this;
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far.
            /// </summary>
            /// <remarks>
            /// If the builder was created with an allocator, and <paramref name="allocator"/> is
            /// <c>null</c> or that same allocator, the builder's working memory usually becomes the
            /// buffer's memory, with no allocation or copy. Otherwise the contents are copied into
            /// <paramref name="allocator"/>, or into <see cref="MemoryAllocator.Default"/> when it is
            /// <c>null</c>. After a hand-over the builder stays usable: it copies the contents back into
            /// memory of its own the next time it is written to, and <see cref="Clear"/> discards
            /// them without copying. Building again without writing returns a buffer that shares
            /// the same memory.
            /// </remarks>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
            public ArrowBuffer Build(MemoryAllocator allocator = default)
            {
                return Build(64, allocator);
            }

            /// <summary>
            /// Build an Arrow buffer from the appended contents so far of the specified byte size.
            /// </summary>
            /// <param name="allocator">Optional memory allocator.</param>
            /// <returns>Returns an <see cref="ArrowBuffer"/> object.</returns>
            internal ArrowBuffer Build(int byteSize, MemoryAllocator allocator = default)
            {
                return _buffer.Build(Length * Size, byteSize, allocator);
            }

            private void EnsureAdditionalCapacity(int additionalCapacity)
            {
                EnsureCapacity(checked(Length + additionalCapacity));
            }

            private void EnsureCapacity(int requiredCapacity)
            {
                if (requiredCapacity > Capacity)
                {
                    // TODO: specifiable growth strategy
                    // Double the length of the in-memory array, or use the byte count of the capacity, whichever is
                    // greater.
                    int capacity = Math.Max(checked(requiredCapacity * Size), _buffer.Capacity * 2);

                    // Grow through the allocator so that the builder's working memory is
                    // visible to it, and so that allocators able to grow in place can do so.
                    _buffer.Grow(capacity);
                }
            }

            /// <summary>
            /// Releases the builder's working memory.
            /// </summary>
            /// <remarks>
            /// Disposal is required only when the working memory came from an allocator whose
            /// <see cref="MemoryAllocator.Lifetime"/> is
            /// <see cref="AllocationLifetime.CallerOwned"/>; otherwise it merely returns the
            /// memory for reuse. Buffers already produced by <see cref="Build(MemoryAllocator)"/>
            /// are unaffected: if one of them holds the builder's memory, disposal only drops the
            /// builder's reference to it.
            /// </remarks>
            public void Dispose()
            {
                _buffer.Dispose();
                Length = 0;
            }
        }

    }
}
