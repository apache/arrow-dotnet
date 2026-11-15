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

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// The working memory of a buffer builder.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Working memory grows in a <em>temporary</em> allocator and the built buffer belongs to a
    /// <em>long-term</em> one. When the two are the same allocator, the working memory can be
    /// handed to the built buffer instead of being copied; otherwise building copies it.
    /// </para>
    /// <para>
    /// A builder given no allocator uses managed memory as its temporary allocator and
    /// <see cref="MemoryAllocator.Default"/> as its long-term one, exactly reproducing how
    /// builders have always behaved: growth never touches native memory, and building makes a
    /// single native allocation of the exact size. Passing an allocator opts into the hand-over.
    /// </para>
    /// <para>
    /// After a hand-over the builder no longer owns writable memory. It keeps a retained
    /// reference to the built buffer so that its contents stay readable, and copies them into
    /// fresh working memory the first time the builder is written to again. Code that builds
    /// once and discards the builder therefore allocates once; code that keeps appending after
    /// a build pays one copy, which is what every build cost before.
    /// </para>
    /// <para>
    /// When it can hand over, working memory is always a multiple of <see cref="Padding"/>
    /// bytes, so that a builder created with the right capacity never has to grow to pad its
    /// result.
    /// </para>
    /// </remarks>
    internal sealed class BuilderMemory : IDisposable
    {
        internal const int Padding = 64;

        /// <summary>
        /// Hand over only when at most this fraction of the working memory would be unused
        /// (1/2). Doubling growth means this almost always holds; it rules out handing a large
        /// reservation to a small array, which would hold the slack for the array's lifetime.
        /// </summary>
        private const int MaxSlackDivisor = 2;

        private readonly MemoryAllocator _temporary;
        private readonly MemoryAllocator _longTerm;
        private readonly bool _canHandOver;

        // Exactly one of these is in use: _owner/_memory while the builder owns its working
        // memory, _shared after that memory has been handed to a built buffer. Both are empty
        // after disposal.
        private IMemoryOwner<byte> _owner;
        private Memory<byte> _memory;
        private ArrowBuffer _shared;
        private bool _isShared;

        /// <param name="allocator">
        /// The allocator for both working memory and built buffers, or <c>null</c> for managed
        /// working memory copied into <see cref="MemoryAllocator.Default"/> when built.
        /// </param>
        /// <param name="capacityBytes">Initial capacity in bytes.</param>
        public BuilderMemory(MemoryAllocator allocator, int capacityBytes)
            : this(allocator ?? ManagedMemoryAllocator.Instance, allocator ?? MemoryAllocator.Default.Value, capacityBytes)
        {
        }

        private BuilderMemory(MemoryAllocator temporary, MemoryAllocator longTerm, int capacityBytes)
        {
            if (capacityBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacityBytes));
            }

            _temporary = temporary;
            _longTerm = longTerm;
            _canHandOver = ReferenceEquals(temporary, longTerm);
            _owner = _temporary.Allocate(RoundUp(capacityBytes));
            _memory = _owner.Memory;
        }

        /// <summary>
        /// Whether built buffers can take over the working memory. When false, the builder
        /// behaves exactly as builders always have.
        /// </summary>
        public bool CanHandOver => _canHandOver;

        /// <summary>
        /// Size of the working memory in bytes. Reading this never copies.
        /// </summary>
        public int Capacity => _isShared ? _shared.Length : _memory.Length;

        /// <summary>
        /// The writable working memory. If it has been handed to a built buffer, the contents
        /// are first copied into memory the builder owns, so writes never reach that buffer.
        /// </summary>
        public Memory<byte> Memory
        {
            get
            {
                if (_isShared)
                {
                    Detach(_shared.Length);
                }

                return _memory;
            }
        }

        /// <summary>
        /// Grows the working memory to at least <paramref name="capacityBytes"/>, preserving
        /// its contents and zeroing the added region.
        /// </summary>
        public void Grow(int capacityBytes)
        {
            int rounded = RoundUp(capacityBytes);
            if (rounded <= Capacity)
            {
                return;
            }

            if (_isShared)
            {
                Detach(rounded);
                return;
            }

            ThrowIfDisposed();
            _owner = _temporary.Resize(_owner, rounded);
            _memory = _owner.Memory;
        }

        /// <summary>
        /// Zeroes the working memory, keeping its capacity. Discards rather than copies memory
        /// that was handed to a built buffer, so clearing a builder after a build is cheap.
        /// </summary>
        public void Reset()
        {
            if (_isShared)
            {
                int capacity = _shared.Length;
                ReleaseShared();
                _owner = _temporary.Allocate(capacity);
                _memory = _owner.Memory;
                return;
            }

            _memory.Span.Clear();
        }

        /// <summary>
        /// Builds a buffer holding the first <paramref name="usedBytes"/> bytes, zero-padded to
        /// a multiple of <paramref name="byteSize"/>.
        /// </summary>
        /// <param name="target">
        /// Allocator that should own the result; <c>null</c> means this builder's long-term
        /// allocator. The working memory is handed over only when that is also the temporary
        /// allocator; otherwise the data is copied.
        /// </param>
        public ArrowBuffer Build(int usedBytes, int byteSize, MemoryAllocator target)
        {
            int padded = checked((int)BitUtility.RoundUpToMultiplePowerOfTwo(usedBytes, byteSize));
            MemoryAllocator destination = target ?? _longTerm;
            bool handOver = _canHandOver && ReferenceEquals(destination, _temporary);

            if (_isShared)
            {
                // Built again without being written to: share the same memory.
                if (handOver && padded == _shared.Length)
                {
                    return _shared.Retain();
                }

                return Copy(_shared.Span, usedBytes, padded, destination);
            }

            ThrowIfDisposed();

            if (!handOver || padded == 0 || padded > _memory.Length ||
                _memory.Length - padded > _memory.Length / MaxSlackDivisor)
            {
                return Copy(_memory.Span, usedBytes, padded, destination);
            }

            // The bytes between the data and the padding boundary may hold values left behind
            // by a Resize or a Set, which the copying path would have left as zero.
            _memory.Span.Slice(usedBytes, padded - usedBytes).Clear();

            IMemoryOwner<byte> owner = padded == _memory.Length
                ? _owner
                : new SlicedMemoryOwner(_owner, padded);

            ArrowBuffer result = new ArrowBuffer(owner, _temporary.Lifetime);
            _shared = result.Retain();
            _isShared = true;
            _owner = null;
            _memory = Memory<byte>.Empty;

            return result;
        }

        public void Dispose()
        {
            if (_isShared)
            {
                ReleaseShared();
                return;
            }

            IMemoryOwner<byte> owner = _owner;
            _owner = null;
            _memory = Memory<byte>.Empty;
            owner?.Dispose();
        }

        private void Detach(int capacityBytes)
        {
            IMemoryOwner<byte> owner = _temporary.Allocate(capacityBytes);
            _shared.Span.CopyTo(owner.Memory.Span);
            ReleaseShared();
            _owner = owner;
            _memory = owner.Memory;
        }

        private void ReleaseShared()
        {
            ArrowBuffer shared = _shared;
            _shared = default;
            _isShared = false;
            shared.Dispose();
        }

        private static ArrowBuffer Copy(ReadOnlySpan<byte> source, int usedBytes, int padded, MemoryAllocator allocator)
        {
            IMemoryOwner<byte> owner = allocator.Allocate(padded);
            source.Slice(0, usedBytes).CopyTo(owner.Memory.Span);
            return new ArrowBuffer(owner, allocator.Lifetime);
        }

        private void ThrowIfDisposed()
        {
            if (_owner == null)
            {
                throw new ObjectDisposedException(nameof(ArrowBuffer.Builder<byte>));
            }
        }

        // Only memory that may be handed over needs padding; otherwise allocate exactly what
        // was asked for, as builders always have.
        private int RoundUp(int bytes) =>
            _canHandOver ? checked((int)BitUtility.RoundUpToMultipleOf64(bytes)) : bytes;
    }
}
