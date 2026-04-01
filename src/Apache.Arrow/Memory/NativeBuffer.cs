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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Apache.Arrow.Memory
{
    public sealed class NativeBuffer<TItem, TTracker> : IDisposable
        where TItem : struct
        where TTracker : struct, INativeAllocationTracker
    {
        private const int Alignment = MemoryAllocator.DefaultAlignment;

        private MemoryManager _owner;
        private int _byteLength;

        /// <summary>Number of <typeparamref name="TItem"/> elements that fit in the buffer.</summary>
        public int Length { get; private set; }

        /// <summary>Creates a native buffer sized for <paramref name="elementCount"/> elements of <typeparamref name="TItem"/>.</summary>
        /// <param name="elementCount">Number of elements.</param>
        /// <param name="zeroFill">If true, the buffer is zeroed. Set to false if the caller will initialize the entire span itself.</param>
        /// <param name="tracker">Allows native allocation sizes to be tracked and affect the GC.</param>
        public NativeBuffer(int elementCount, bool zeroFill = true, TTracker tracker = default)
        {
            int elementSize = Unsafe.SizeOf<TItem>();
            _byteLength = checked(elementCount * elementSize);
            Length = elementCount;
            _owner = new MemoryManager(_byteLength, tracker);

            if (zeroFill)
            {
                Span.Clear();
            }
        }

        /// <summary>Gets a <see cref="Span{T}"/> over the native buffer.</summary>
        public Span<TItem> Span
        {
            get
            {
                var byteSpan = _owner!.Memory.Span;
                return MemoryMarshal.Cast<byte, TItem>(byteSpan);
            }
        }

        /// <summary>Gets a <see cref="Span{T}"/> over the raw bytes of the native buffer.</summary>
        public Span<byte> ByteSpan => _owner!.Memory.Span.Slice(0, _byteLength);

        /// <summary>
        /// Transfers ownership to an <see cref="ArrowBuffer"/>. This instance becomes unusable.
        /// </summary>
        public ArrowBuffer Build()
        {
            var owner = _owner ?? throw new ObjectDisposedException(nameof(NativeBuffer<TItem, TTracker>));
            _owner = null;
            return new ArrowBuffer(owner);
        }

        /// <summary>
        /// Grows the buffer to hold at least <paramref name="newElementCount"/> elements,
        /// preserving existing data.
        /// </summary>
        public void Grow(int newElementCount, bool zeroFill = true)
        {
            if (newElementCount <= Length)
                return;

            // Exponential growth (2x) to amortise repeated grows
            // TODO: There might be a size that's big enough to work for this case but not too big to overflow.
            // We could use that instead of blindly doubling.
            int newCount = Math.Max(newElementCount, checked(Length * 2));
            int elementSize = Unsafe.SizeOf<TItem>();
            int needed = checked(newCount * elementSize);

            var owner = _owner ?? throw new ObjectDisposedException(nameof(NativeBuffer<TItem, TTracker>));
            owner.Reallocate(needed);

            if (zeroFill)
            {
                Span.Slice(Length, newCount - Length).Clear();
            }

            _byteLength = needed;
            Length = newCount;
        }

        public void Dispose()
        {
            IDisposable disposable = _owner;
            _owner = null;
            disposable?.Dispose();
        }

        /// <summary>
        /// A <see cref="MemoryManager{T}"/> backed by aligned native memory.
        /// On .NET 6+ uses <see cref="NativeMemory.AlignedAlloc"/>; on downlevel platforms
        /// uses <see cref="AlignedNative"/> (P/Invoke to ucrtbase.dll with fallback).
        /// Disposing frees the native memory.
        /// </summary>
        private sealed class MemoryManager : MemoryManager<byte>
        {
            private unsafe void* _pointer;
#if !NET6_0_OR_GREATER
            private int _offset;
#endif
            private int _length;
            private int _pinCount;
            private TTracker _tracker;

            public unsafe MemoryManager(int length, TTracker tracker)
            {
                _length = length;
#if NET6_0_OR_GREATER
                _pointer = NativeMemory.AlignedAlloc((nuint)length, (nuint)Alignment);
#else
                _pointer = AlignedNative.AlignedAlloc(length, Alignment, out _offset);
#endif
                _tracker = tracker;
                _tracker.Track(1, length);
            }

            public override Span<byte> GetSpan()
            {
                unsafe
                {
#if NET6_0_OR_GREATER
                    return new Span<byte>(_pointer, _length);
#else
                    return new Span<byte>((void*)((byte*)_pointer + _offset), _length);
#endif
                }
            }

            public override MemoryHandle Pin(int elementIndex = 0)
            {
                Interlocked.Increment(ref _pinCount);
                unsafe
                {
#if NET6_0_OR_GREATER
                    return new MemoryHandle((byte*)_pointer + elementIndex, pinnable: this);
#else
                    return new MemoryHandle((byte*)_pointer + _offset + elementIndex, pinnable: this);
#endif
                }
            }

            public override void Unpin()
            {
                Interlocked.Decrement(ref _pinCount);
            }

            protected override void Dispose(bool disposing)
            {
                unsafe
                {
                    if (_pointer != null)
                    {
#if NET6_0_OR_GREATER
                        NativeMemory.AlignedFree(_pointer);
#else
                        AlignedNative.AlignedFree(_pointer);
#endif
                        _pointer = null;
                        _tracker.Track(-1, -_length);
                    }
                }
            }

            /// <summary>
            /// Reallocates the native buffer to <paramref name="newLength"/> bytes in place,
            /// preserving existing data. Equivalent to <c>_aligned_realloc</c>.
            /// </summary>
            /// <exception cref="InvalidOperationException">Thrown if the memory is currently pinned.</exception>
            public unsafe void Reallocate(int newLength)
            {
                if (Volatile.Read(ref _pinCount) > 0)
                    throw new InvalidOperationException(
                        "Cannot reallocate a NativeMemoryManager that is currently pinned.");

                int oldLength = _length;
#if NET6_0_OR_GREATER
                _pointer = NativeMemory.AlignedRealloc(_pointer, (nuint)newLength, Alignment);
#else
                _pointer = AlignedNative.AlignedRealloc(_pointer, newLength, Alignment, oldLength, ref _offset);
#endif
                _length = newLength;
                _tracker.Track(0, newLength - oldLength);
            }
        }
    }
}
