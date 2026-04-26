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
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;

namespace Apache.Arrow.Scalars.Variant
{
    /// <summary>
    /// Mutable, non-thread-safe growable buffer designed for single-owner use as
    /// a field on a class (or as a local owned by the same method that created
    /// it). The backing array is rented from a caller-supplied
    /// <see cref="Stack{T}"/> so that capacity is "sticky" across reuse cycles
    /// within one owner — unlike <see cref="ArrayPool{T}.Shared"/>, which
    /// buckets by size class and may hand back a different array than the one
    /// last returned.
    /// </summary>
    /// <remarks>
    /// <b>Warning — mutable value type.</b> A <c>Buffer&lt;T&gt;</c> must never
    /// be copied by value. All mutation has to go through the original storage
    /// location, or a stale copy will silently corrupt output by writing past
    /// its own view of the length into a shared backing array:
    /// <list type="bullet">
    ///   <item>Pass by <c>ref</c>, not by value. Helper methods that take a
    ///     <c>Buffer&lt;T&gt;</c> parameter must declare it
    ///     <c>ref Buffer&lt;T&gt;</c>.</item>
    ///   <item>Assign from a getter only through a ref local
    ///     (<c>ref Buffer&lt;T&gt; b = ref GetBuffer();</c>) — a plain
    ///     <c>var</c> local is a copy.</item>
    ///   <item>Byte-specific helpers live on <see cref="ByteBufferExtensions"/>
    ///     as <c>ref this</c> extension methods precisely so they can't be
    ///     invoked on a by-value receiver. Do not add methods that would force
    ///     a by-value receiver.</item>
    /// </list>
    /// <para>
    /// <b>Explicit lifetime.</b> Pair every <see cref="Acquire"/> with
    /// <see cref="Release"/> (typically at the begin/end of a scope).
    /// <see cref="Acquire"/> must be called before any write; writes on a
    /// default-initialized buffer throw <see cref="NullReferenceException"/>.
    /// Failing to <see cref="Release"/> leaks the backing array to the GC.
    /// For the same reason, the local pool must also be cleaned up when done.
    /// This can be done with <see cref="DrainPool"/>.
    /// 
    /// </para>
    /// </remarks>
    internal struct Buffer<T>
    {
        private const int InitialCapacity = 64;

        private T[] _buf;
        private int _length;

        /// <summary>Number of items currently written.</summary>
        public int Length => _length;

        /// <summary>
        /// The backing array. Slots beyond <see cref="Length"/> are unspecified;
        /// callers must respect the length when reading.
        /// </summary>
        public T[] RawBuffer => _buf;

        /// <summary>
        /// Rents a backing array from <paramref name="pool"/> (falling back to
        /// <see cref="ArrayPool{T}.Shared"/> on pool miss) and resets length to
        /// zero. Must be called before any write.
        /// </summary>
        public void Acquire(Stack<T[]> pool)
        {
            _buf = pool.Count > 0 ? pool.Pop() : ArrayPool<T>.Shared.Rent(InitialCapacity);
            _length = 0;
        }

        /// <summary>
        /// Returns the backing array to <paramref name="pool"/> and clears
        /// state. Safe to call on a default-initialized buffer (no-op).
        /// </summary>
        public void Release(Stack<T[]> pool)
        {
            if (_buf != null)
            {
                pool.Push(_buf);
                _buf = null;
                _length = 0;
            }
        }

        /// <summary>
        /// Returns every array stashed in <paramref name="pool"/> to
        /// <see cref="ArrayPool{T}.Shared"/>. Use at end-of-life of the owner
        /// to release the per-owner cache built up by <see cref="Release"/>.
        /// </summary>
        public static void DrainPool(Stack<T[]> pool)
        {
            while (pool.Count > 0)
            {
                ArrayPool<T>.Shared.Return(pool.Pop());
            }
        }

        /// <summary>
        /// Returns a span covering the next <paramref name="sizeHint"/> writable
        /// items, growing the backing array if necessary. Call
        /// <see cref="Advance"/> after writing to commit.
        /// </summary>
        public Span<T> GetSpan(int sizeHint)
        {
            EnsureCapacity(_length + sizeHint);
            return _buf.AsSpan(_length);
        }

        /// <summary>Advances the written length by <paramref name="count"/>.</summary>
        public void Advance(int count) => _length += count;

        /// <summary>Appends a single item.</summary>
        public void Append(T value)
        {
            EnsureCapacity(_length + 1);
            _buf[_length++] = value;
        }

        /// <summary>Appends a span of items.</summary>
        public void Append(ReadOnlySpan<T> src)
        {
            EnsureCapacity(_length + src.Length);
            src.CopyTo(_buf.AsSpan(_length));
            _length += src.Length;
        }

        /// <summary>Appends a range from an array.</summary>
        public void Append(T[] src, int start, int count)
        {
            EnsureCapacity(_length + count);
            Array.Copy(src, start, _buf, _length, count);
            _length += count;
        }

        /// <summary>Copies the written items into a freshly allocated array of exact length.</summary>
        public T[] ToArray()
        {
            T[] result = new T[_length];
            Array.Copy(_buf, 0, result, 0, _length);
            return result;
        }

        private void EnsureCapacity(int required)
        {
            if (required > _buf.Length)
            {
                int newSize = _buf.Length;
                do
                {
                    newSize *= 2;
                } while (newSize < required);
                T[] grown = ArrayPool<T>.Shared.Rent(newSize);
                Array.Copy(_buf, 0, grown, 0, _length);
                ArrayPool<T>.Shared.Return(_buf);
                _buf = grown;
            }
        }
    }

    /// <summary>
    /// Byte-specific writers for <see cref="Buffer{T}"/> of <see cref="byte"/>.
    /// Declared as <c>ref this</c> extension methods so invocation through a
    /// by-value receiver is a compile error, not a silent copy-and-desync.
    /// </summary>
    internal static class ByteBufferExtensions
    {
        public static void WriteInt16LE(this ref Buffer<byte> buf, short value)
        {
            BinaryPrimitives.WriteInt16LittleEndian(buf.GetSpan(2), value);
            buf.Advance(2);
        }

        public static void WriteInt32LE(this ref Buffer<byte> buf, int value)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buf.GetSpan(4), value);
            buf.Advance(4);
        }

        public static void WriteInt64LE(this ref Buffer<byte> buf, long value)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buf.GetSpan(8), value);
            buf.Advance(8);
        }

        public static void WriteFloatLE(this ref Buffer<byte> buf, float value)
        {
#if NET8_0_OR_GREATER
            BinaryPrimitives.WriteSingleLittleEndian(buf.GetSpan(4), value);
            buf.Advance(4);
#else
            int bits = System.Runtime.CompilerServices.Unsafe.As<float, int>(ref value);
            buf.WriteInt32LE(bits);
#endif
        }

        public static void WriteDoubleLE(this ref Buffer<byte> buf, double value)
        {
#if NET8_0_OR_GREATER
            BinaryPrimitives.WriteDoubleLittleEndian(buf.GetSpan(8), value);
            buf.Advance(8);
#else
            long bits = BitConverter.DoubleToInt64Bits(value);
            buf.WriteInt64LE(bits);
#endif
        }

        public static void WriteSmallInt(this ref Buffer<byte> buf, int value, int byteWidth)
        {
            VariantEncodingHelper.WriteLittleEndianInt(buf.GetSpan(byteWidth), value, byteWidth);
            buf.Advance(byteWidth);
        }
    }
}
