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
using System.Runtime.CompilerServices;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;

namespace Apache.Arrow;

public class LargeBinaryArray : Array, IBinaryArray, IReadOnlyList<byte[]>, ICollection<byte[]>
{
    public class Builder : BuilderBase<LargeBinaryArray, Builder>
    {
        public Builder() : base(LargeBinaryType.Default) { }
        public Builder(IArrowType dataType) : base(dataType) { }

        /// <summary>
        /// Creates a builder whose working memory comes from <paramref name="allocator"/>.
        /// </summary>
        /// <param name="dataType">The binary type to build.</param>
        /// <param name="allocator">
        /// Allocator for the builder's working memory, which usually becomes the built array's
        /// memory. When <c>null</c>, the builder works in managed memory and copies
        /// what it builds into <see cref="MemoryAllocator.Default"/>, as builders always have.
        /// </param>
        public Builder(IArrowType dataType, MemoryAllocator allocator) : base(dataType, allocator) { }

        protected override LargeBinaryArray Build(ArrayData data)
        {
            return new LargeBinaryArray(data);
        }
    }

    public abstract class BuilderBase<TArray, TBuilder> : IArrowArrayBuilder<byte, TArray, TBuilder>, IDisposable
        where TArray : IArrowArray
        where TBuilder : class, IArrowArrayBuilder<byte, TArray, TBuilder>
    {
        protected IArrowType DataType { get; }
        protected TBuilder Instance => this as TBuilder;
        protected ArrowBuffer.Builder<long> ValueOffsets { get; }
        protected ArrowBuffer.Builder<byte> ValueBuffer { get; }
        protected ArrowBuffer.BitmapBuilder ValidityBuffer { get; }
        protected long Offset { get; set; }
        protected int NullCount => this.ValidityBuffer.UnsetBitCount;

        protected BuilderBase(IArrowType dataType)
            : this(dataType, null)
        {
        }

        protected BuilderBase(IArrowType dataType, MemoryAllocator allocator)
        {
            DataType = dataType;
            ValueOffsets = new ArrowBuffer.Builder<long>(0, allocator);
            ValueBuffer = new ArrowBuffer.Builder<byte>(0, allocator);
            ValidityBuffer = new ArrowBuffer.BitmapBuilder(0, allocator);
            ValueOffsets.Append(this.Offset);
        }

        protected abstract TArray Build(ArrayData data);

        public int Length => ValueOffsets.Length - 1;

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
                ValueOffsets.Dispose();
                ValueBuffer.Dispose();
                ValidityBuffer.Dispose();
            }
        }

        public TArray Build(MemoryAllocator allocator = default)
        {
            var bufs = new ArrowBuffer[3];
            try
            {
                bufs[0] = NullCount > 0 ? ValidityBuffer.Build(allocator) : ArrowBuffer.Empty;
                bufs[1] = ValueOffsets.Build(allocator);
                bufs[2] = ValueBuffer.Build(allocator);
                var data = new ArrayData(
                    DataType,
                    length: Length,
                    NullCount,
                    offset: 0,
                    bufs);

                return Build(data);
            }
            catch
            {
                // For example an allocator that has run out of its budget; release what was built.
                foreach (ArrowBuffer buffer in bufs)
                {
                    buffer.Dispose();
                }
                throw;
            }
        }

        public TBuilder AppendNull()
        {
            ValidityBuffer.Append(false);
            ValueOffsets.Append(Offset);
            return Instance;
        }

        public TBuilder Append(byte value)
        {
            ValueBuffer.Append(value);
            ValidityBuffer.Append(true);
            Offset++;
            ValueOffsets.Append(Offset);
            return Instance;
        }

        public TBuilder Append(ReadOnlySpan<byte> span)
        {
            ValueBuffer.Append(span);
            ValidityBuffer.Append(true);
            Offset += span.Length;
            ValueOffsets.Append(Offset);
            return Instance;
        }

        public TBuilder Append(IEnumerable<byte> value)
        {
            if (value == null)
            {
                return AppendNull();
            }

            long priorLength = ValueBuffer.Length;
            ValueBuffer.AppendRange(value);
            long valueLength = ValueBuffer.Length - priorLength;
            Offset += valueLength;
            ValidityBuffer.Append(true);
            ValueOffsets.Append(Offset);
            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<byte> values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            foreach (byte b in values)
            {
                Append(b);
            }

            return Instance;
        }

        public TBuilder AppendRange(IEnumerable<byte[]> values)
        {
            if (values == null)
            {
                throw new ArgumentNullException(nameof(values));
            }

            foreach (byte[] arr in values)
            {
                if (arr == null)
                {
                    AppendNull();
                }
                else
                {
                    Append((ReadOnlySpan<byte>)arr);
                }
            }

            return Instance;
        }

        public TBuilder Reserve(int capacity)
        {
            ValueOffsets.Reserve(capacity + 1);
            ValueBuffer.Reserve(capacity);
            ValidityBuffer.Reserve(capacity);
            return Instance;
        }

        public TBuilder Resize(int length)
        {
            ValueOffsets.Resize(length + 1);
            ValueBuffer.Resize(length);
            ValidityBuffer.Resize(length);
            return Instance;
        }

        public TBuilder Swap(int i, int j)
        {
            throw new NotImplementedException();
        }

        public TBuilder Set(int index, byte value)
        {
            throw new NotImplementedException();
        }

        public TBuilder Clear()
        {
            ValueOffsets.Clear();
            ValueBuffer.Clear();
            ValidityBuffer.Clear();
            Offset = 0;
            ValueOffsets.Append(Offset);
            return Instance;
        }
    }

    public LargeBinaryArray(ArrayData data)
        : base(data)
    {
        data.EnsureDataType(ArrowTypeId.LargeBinary);
        data.EnsureBufferCount(3);
    }

    public LargeBinaryArray(ArrowTypeId typeId, ArrayData data)
        : base(data)
    {
        data.EnsureDataType(typeId);
        data.EnsureBufferCount(3);
    }

    public LargeBinaryArray(IArrowType dataType, int length,
        ArrowBuffer valueOffsetsBuffer,
        ArrowBuffer dataBuffer,
        ArrowBuffer nullBitmapBuffer,
        int nullCount = 0, int offset = 0)
    : this(new ArrayData(dataType, length, nullCount, offset,
        new[] { nullBitmapBuffer, valueOffsetsBuffer, dataBuffer }))
    { }

    public override void Accept(IArrowArrayVisitor visitor) => Accept(this, visitor);

    public ArrowBuffer ValueOffsetsBuffer => Data.Buffers[1];

    public ArrowBuffer ValueBuffer => Data.Buffers[2];

    // A zero-length array may have an empty offsets buffer (e.g. when read from IPC).
    private static ReadOnlySpan<long> ZeroOffset => new long[] { 0 };

    public ReadOnlySpan<long> ValueOffsets => Length == 0 && ValueOffsetsBuffer.IsEmpty
        ? ZeroOffset
        : ValueOffsetsBuffer.Span.CastTo<long>().Slice(Offset, Length + 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int GetValueLength(int index)
    {
        if (index < 0 || index >= Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }
        if (!IsValid(index))
        {
            return 0;
        }

        ReadOnlySpan<long> offsets = ValueOffsets;
        return checked((int)(offsets[index + 1] - offsets[index]));
    }

    /// <summary>
    /// Get the collection of bytes, as a read-only span, at a given index in the array.
    /// </summary>
    /// <remarks>
    /// Note that this method cannot reliably identify null values, which are indistinguishable from empty byte
    /// collection values when seen in the context of this method's return type of <see cref="ReadOnlySpan{Byte}"/>.
    /// Use the <see cref="Array.IsNull"/> method or the <see cref="GetBytes(int, out bool)"/> overload instead
    /// to reliably determine null values.
    /// </remarks>
    /// <param name="index">Index at which to get bytes.</param>
    /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
    /// </exception>
    public ReadOnlySpan<byte> GetBytes(int index) => GetBytes(index, out _);

    /// <summary>
    /// Get the collection of bytes, as a read-only span, at a given index in the array.
    /// </summary>
    /// <param name="index">Index at which to get bytes.</param>
    /// <param name="isNull">Set to <see langword="true"/> if the value at the given index is null.</param>
    /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
    /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
    /// </exception>
    public ReadOnlySpan<byte> GetBytes(int index, out bool isNull)
    {
        if (index < 0 || index >= Length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        isNull = IsNull(index);

        if (isNull)
        {
            // Note that `return null;` is valid syntax, but would be misleading as `null` in the context of a span
            // is actually returned as an empty span.
            return ReadOnlySpan<byte>.Empty;
        }

        var offset = checked((int)ValueOffsets[index]);
        return ValueBuffer.Span.Slice(offset, GetValueLength(index));
    }

    int IReadOnlyCollection<byte[]>.Count => Length;

    byte[] IReadOnlyList<byte[]>.this[int index] => GetBytes(index).ToArray();

    IEnumerator<byte[]> IEnumerable<byte[]>.GetEnumerator()
    {
        for (int index = 0; index < Length; index++)
        {
            yield return GetBytes(index).ToArray();
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable<byte[]>)this).GetEnumerator();

    int ICollection<byte[]>.Count => Length;
    bool ICollection<byte[]>.IsReadOnly => true;
    void ICollection<byte[]>.Add(byte[] item) => throw new NotSupportedException("Collection is read-only.");
    bool ICollection<byte[]>.Remove(byte[] item) => throw new NotSupportedException("Collection is read-only.");
    void ICollection<byte[]>.Clear() => throw new NotSupportedException("Collection is read-only.");

    bool ICollection<byte[]>.Contains(byte[] item)
    {
        for (int index = 0; index < Length; index++)
        {
            if (GetBytes(index).SequenceEqual(item))
                return true;
        }

        return false;
    }

    void ICollection<byte[]>.CopyTo(byte[][] array, int arrayIndex)
    {
        for (int srcIndex = 0, destIndex = arrayIndex; srcIndex < Length; srcIndex++, destIndex++)
        {
            array[destIndex] = GetBytes(srcIndex).ToArray();
        }
    }
}
