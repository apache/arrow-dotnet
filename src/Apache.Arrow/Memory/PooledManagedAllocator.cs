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
using System.Threading;

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Allocates buffers from the garbage-collected heap, keeping disposed buffers' arrays for
    /// reuse so that allocating repeatedly does not have to create new arrays.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A span over a buffer from this allocator keeps its array alive, so a buffer that is never
    /// disposed is simply collected, and disposal is optional. But disposal hands the array back
    /// for reuse: a span used after its buffer was disposed can read or overwrite the data of
    /// whichever buffer reuses the array. If that contract is a concern, use
    /// <see cref="ManagedMemoryAllocator"/>, which never reuses arrays, or set
    /// <see cref="PooledManagedAllocatorOptions.PoisonDisposedBuffers"/> to make such bugs visible.
    /// </para>
    /// <para>
    /// How much memory is kept for reuse is bounded by <see cref="PooledManagedAllocatorOptions"/>;
    /// allocations larger than <see cref="PooledManagedAllocatorOptions.MaxPooledLength"/> are not
    /// pooled. Memory is aligned only to the pointer size, and may be moved by the garbage
    /// collector; code that needs a stable, 64-byte-aligned address should pin the memory or use
    /// <see cref="CallerOwnedNativeAllocator"/>.
    /// </para>
    /// </remarks>
    public sealed class PooledManagedAllocator : MemoryAllocator
    {
        /// <summary>The largest allocation that can be configured to be pooled: 1 GiB.</summary>
        internal const int LargestPoolableLength = 1 << 30;

        private const int MinimumArrayLength = 16;
        private const byte PoisonByte = 0xDE;

        private readonly int _maxPooledLength;
        private readonly long _maxRetainedBytes;
        private readonly bool _poison;
        private readonly Bucket[] _buckets;
        private long _retainedBytes;

        public PooledManagedAllocator()
            : this(new PooledManagedAllocatorOptions())
        {
        }

        public PooledManagedAllocator(PooledManagedAllocatorOptions options)
            : base(IntPtr.Size)
        {
            if (options == null)
            {
                throw new ArgumentNullException(nameof(options));
            }

            options.Validate();
            _maxPooledLength = options.MaxPooledLength;
            _maxRetainedBytes = options.MaxRetainedBytes;
            _poison = options.PoisonDisposedBuffers;

            // One bucket per power-of-two size from MinimumArrayLength up to the size that holds
            // the largest pooled allocation. No bucket allocates anything until it is used.
            int bucketCount = _maxPooledLength == 0 ? 0 : BucketIndex(_maxPooledLength) + 1;
            _buckets = new Bucket[bucketCount];
            for (int i = 0; i < bucketCount; i++)
            {
                _buckets[i] = new Bucket(MinimumArrayLength << i, options.MaxArraysPerSize);
            }
        }

        public override AllocationLifetime Lifetime => AllocationLifetime.GarbageCollected;

        /// <summary>The number of bytes currently held for reuse.</summary>
        internal long RetainedBytes => Interlocked.Read(ref _retainedBytes);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            byte[] buffer = Rent(length, out bool reused);

            // A reused array holds an earlier buffer's data, and Arrow buffers are expected to
            // start zeroed. A new array is zeroed already.
            if (reused)
            {
                new Span<byte>(buffer, 0, length).Clear();
            }

            bytesAllocated = length;
            return new PooledMemoryOwner(this, buffer, length);
        }

        /// <summary>
        /// Grows or shrinks a buffer, zeroing only the region past the copied data. The base
        /// implementation zeroes the whole new buffer and then copies over most of it, which
        /// doubles the write traffic on append-heavy paths.
        /// </summary>
        public override IMemoryOwner<byte> Resize(IMemoryOwner<byte> owner, int newLength)
        {
            if (owner == null)
            {
                throw new ArgumentNullException(nameof(owner));
            }

            if (newLength < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(newLength));
            }

            if (newLength == 0)
            {
                owner.Dispose();
                return Allocate(0);
            }

            byte[] buffer = Rent(newLength, out bool reused);
            ReadOnlyMemory<byte> existing = owner.Memory;
            int copyLength = Math.Min(existing.Length, newLength);

            if (copyLength > 0)
            {
                existing.Slice(0, copyLength).CopyTo(new Memory<byte>(buffer, 0, copyLength));
            }

            if (reused)
            {
                new Span<byte>(buffer, copyLength, newLength - copyLength).Clear();
            }

            owner.Dispose();
            Statistics.Allocate(newLength);
            return new PooledMemoryOwner(this, buffer, newLength);
        }

        private byte[] Rent(int length, out bool reused)
        {
            if (length > _maxPooledLength)
            {
                reused = false;
                return new byte[length];
            }

            Bucket bucket = _buckets[BucketIndex(length)];
            byte[] buffer = bucket.TryTake();
            if (buffer != null)
            {
                Interlocked.Add(ref _retainedBytes, -buffer.Length);
                reused = true;
                return buffer;
            }

            reused = false;
            return new byte[bucket.ArrayLength];
        }

        private void Return(byte[] buffer)
        {
            if (_poison)
            {
                buffer.AsSpan().Fill(PoisonByte);
            }

            // Only arrays of exactly a bucket's size are pooled; anything else (including
            // allocations too large to pool) is left for the garbage collector. The size check
            // must come first: BucketIndex is only defined up to the largest bucket's size.
            int length = buffer.Length;
            if (_buckets.Length == 0 || length > _buckets[_buckets.Length - 1].ArrayLength)
            {
                return;
            }

            int index = BucketIndex(length);
            if (_buckets[index].ArrayLength != length)
            {
                return;
            }

            // Reserve room under the byte limit before pooling, and give it back if the array
            // is not kept after all.
            if (Interlocked.Add(ref _retainedBytes, length) > _maxRetainedBytes || !_buckets[index].TryAdd(buffer))
            {
                Interlocked.Add(ref _retainedBytes, -length);
            }
        }

        /// <summary>
        /// The index of the smallest bucket whose arrays hold <paramref name="length"/> bytes.
        /// Only valid for lengths up to <see cref="LargestPoolableLength"/>.
        /// </summary>
        private static int BucketIndex(int length)
        {
            int index = 0;
            int size = MinimumArrayLength;
            while (size < length)
            {
                size <<= 1;
                index++;
            }

            return index;
        }

        /// <summary>A bounded stack of arrays of one size.</summary>
        private sealed class Bucket
        {
            private readonly byte[][] _arrays;
            private int _count;

            public Bucket(int arrayLength, int capacity)
            {
                ArrayLength = arrayLength;
                _arrays = new byte[capacity][];
            }

            public int ArrayLength { get; }

            public byte[] TryTake()
            {
                lock (_arrays)
                {
                    if (_count == 0)
                    {
                        return null;
                    }

                    byte[] array = _arrays[--_count];
                    _arrays[_count] = null;
                    return array;
                }
            }

            public bool TryAdd(byte[] array)
            {
                lock (_arrays)
                {
                    if (_count == _arrays.Length)
                    {
                        return false;
                    }

                    _arrays[_count++] = array;
                    return true;
                }
            }
        }

        private sealed class PooledMemoryOwner : IMemoryOwner<byte>
        {
            private readonly PooledManagedAllocator _allocator;
            private readonly int _length;
            private byte[] _buffer;

            public PooledMemoryOwner(PooledManagedAllocator allocator, byte[] buffer, int length)
            {
                _allocator = allocator;
                _buffer = buffer;
                _length = length;
            }

            public Memory<byte> Memory
            {
                get
                {
                    byte[] buffer = Volatile.Read(ref _buffer);
                    if (buffer == null)
                    {
                        throw new ObjectDisposedException(nameof(PooledMemoryOwner));
                    }

                    // Pooled arrays are a power of two in size, so usually larger than asked for.
                    return new Memory<byte>(buffer, 0, _length);
                }
            }

            // No finalizer. An undisposed buffer is collected by the GC like any other array;
            // it is simply not returned to the pool.
            public void Dispose()
            {
                byte[] buffer = Interlocked.Exchange(ref _buffer, null);
                if (buffer != null)
                {
                    _allocator.Return(buffer);
                }
            }
        }
    }
}
