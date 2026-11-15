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
using System.Runtime.InteropServices;
using System.Threading;

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Allocates aligned native memory that the caller is responsible for releasing.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike <see cref="NativeMemoryAllocator"/>, buffers from this allocator have no
    /// finalizer. Memory is released only by disposal, so it can never be freed while a
    /// span over it is still in use, and no GC memory pressure is reported because the
    /// garbage collector plays no part in reclaiming it.
    /// </para>
    /// <para>
    /// The corresponding obligation is that a buffer which is never disposed is leaked for
    /// the lifetime of the process. Use this allocator when buffers need a stable, aligned
    /// address — interop and explicitly aligned paths — and disposal can be guaranteed.
    /// </para>
    /// </remarks>
    public sealed unsafe class CallerOwnedNativeAllocator : MemoryAllocator
    {
        public CallerOwnedNativeAllocator(int alignment = DefaultAlignment)
            : base(alignment)
        {
            if (alignment <= 0 || (alignment & (alignment - 1)) != 0)
            {
                throw new ArgumentOutOfRangeException(nameof(alignment), "Alignment must be a positive power of two.");
            }
        }

        private int _liveAllocations;

        public override AllocationLifetime Lifetime => AllocationLifetime.CallerOwned;

        /// <summary>The number of allocations not yet freed.</summary>
        internal int LiveAllocations => Volatile.Read(ref _liveAllocations);

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
#if NET6_0_OR_GREATER
            int offset = 0;
            void* ptr = NativeMemory.AlignedAlloc((nuint)length, (nuint)Alignment);
            if (ptr == null)
            {
                throw new OutOfMemoryException($"NativeMemory.AlignedAlloc({length}, {Alignment}) returned null.");
            }
#else
            void* ptr = AlignedNative.AlignedAlloc(length, Alignment, out int offset);
#endif
            new Span<byte>((byte*)ptr + offset, length).Clear();

            bytesAllocated = length + Alignment;
            Interlocked.Increment(ref _liveAllocations);
            return new CallerOwnedMemoryManager(this, ptr, offset, length);
        }

        /// <summary>
        /// A <see cref="MemoryManager{T}"/> over native memory with no finalizer, so that
        /// reachability never determines when the memory is freed (CA2015).
        /// </summary>
        /// <remarks>
        /// Disposal while the memory is pinned marks the manager disposed but defers freeing the
        /// memory until the last pin is released, so a pinned pointer never dangles. This differs
        /// from <see cref="NativeMemoryManager"/>, which throws instead; throwing from disposal is
        /// discouraged (CA1065), and deferral cannot leak unless a pin is never released.
        /// </remarks>
        private sealed class CallerOwnedMemoryManager : MemoryManager<byte>
        {
            private readonly CallerOwnedNativeAllocator _allocator;
            private readonly int _offset;
            private readonly int _length;
            private readonly object _sync = new object();
            private IntPtr _ptr;
            private int _pinCount;
            private bool _disposed;

            public CallerOwnedMemoryManager(CallerOwnedNativeAllocator allocator, void* ptr, int offset, int length)
            {
                _allocator = allocator;
                _ptr = (IntPtr)ptr;
                _offset = offset;
                _length = length;
            }

            public override Span<byte> GetSpan()
            {
                IntPtr ptr = Volatile.Read(ref _ptr);
                if (ptr == IntPtr.Zero || Volatile.Read(ref _disposed))
                {
                    throw new ObjectDisposedException(nameof(CallerOwnedMemoryManager));
                }

                return new Span<byte>((byte*)ptr + _offset, _length);
            }

            public override MemoryHandle Pin(int elementIndex = 0)
            {
                if (elementIndex < 0 || elementIndex > _length)
                {
                    throw new ArgumentOutOfRangeException(nameof(elementIndex));
                }

                IntPtr ptr;
                lock (_sync)
                {
                    if (_disposed)
                    {
                        throw new ObjectDisposedException(nameof(CallerOwnedMemoryManager));
                    }

                    _pinCount++;
                    ptr = _ptr;
                }

                // Native memory does not move, so there is nothing to pin in the GC sense; the
                // count only keeps disposal from freeing the memory while the pointer is in use.
                return new MemoryHandle((byte*)ptr + _offset + elementIndex, default, this);
            }

            public override void Unpin()
            {
                bool free;
                lock (_sync)
                {
                    if (_pinCount == 0)
                    {
                        return;
                    }

                    _pinCount--;
                    free = _disposed && _pinCount == 0;
                }

                if (free)
                {
                    Free();
                }
            }

            protected override void Dispose(bool disposing)
            {
                bool free;
                lock (_sync)
                {
                    if (_disposed)
                    {
                        return;
                    }

                    _disposed = true;
                    free = _pinCount == 0;
                }

                if (free)
                {
                    Free();
                }
            }

            private void Free()
            {
                IntPtr ptr = Interlocked.Exchange(ref _ptr, IntPtr.Zero);
                if (ptr != IntPtr.Zero)
                {
#if NET6_0_OR_GREATER
                    NativeMemory.AlignedFree((void*)ptr);
#else
                    AlignedNative.AlignedFree((void*)ptr);
#endif
                    Interlocked.Decrement(ref _allocator._liveAllocations);
                }
            }
        }
    }
}
