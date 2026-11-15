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
using Apache.Arrow.Memory;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests that code which does not pass an allocator keeps its historical behaviour, and that
    /// the new behaviours are opt-in, chosen by passing an allocator.
    /// </summary>
    public class DefaultAllocationBehaviorTests
    {
        [Fact]
        public void BuilderWithoutAllocatorGrowsInManagedMemory()
        {
            ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>();
            for (int i = 0; i < 1000; i++)
            {
                builder.Append(i);
            }

            Assert.True(MemoryMarshal.TryGetArray<byte>(builder.Memory, out _), "Working memory should be a managed array.");
        }

        [Fact]
        public void BuilderWithoutAllocatorCopiesExactlyIntoTheDefaultAllocator()
        {
            ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>();
            for (int i = 0; i < 1000; i++)
            {
                builder.Append(i);
            }

            ArrowBuffer buffer = builder.Build();

            // One native allocation of exactly the padded length: no slack is kept.
            Assert.True(MemoryMarshal.TryGetMemoryManager(buffer.Memory, out NativeMemoryManager manager));
            Assert.Equal(4032, buffer.Length);
            Assert.Equal(4032, manager.Memory.Length);

            // The builder keeps its own memory, as it always has.
            builder.Span[0] = -1;
            Assert.Equal(0, buffer.Span.CastTo<int>()[0]);
        }

        [Fact]
        public void BuilderWithoutAllocatorKeepsExactCapacity()
        {
            Assert.Equal(1, new ArrowBuffer.Builder<int>(1).Capacity);
            Assert.Equal(8, new ArrowBuffer.Builder<int>().Capacity);
        }

        [Fact]
        public void BitmapBuilderWithoutAllocatorKeepsItsHistoricalBufferLength()
        {
            // Historically the built bitmap covered all of the working memory.
            ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(1024);
            builder.Append(true);

            Assert.Equal(128, builder.Build().Length);
        }

        [Fact]
        public void ArrayBuilderWithoutAllocatorBuildsIntoTheDefaultAllocator()
        {
            Int32Array array = new Int32Array.Builder().Append(1).AppendNull().Append(3).Build();

            Assert.True(MemoryMarshal.TryGetMemoryManager(array.ValueBuffer.Memory, out NativeMemoryManager _));
            Assert.True(MemoryMarshal.TryGetMemoryManager(array.NullBitmapBuffer.Memory, out NativeMemoryManager _));
        }

        [Fact]
        public void UndisposedBuffersFromFinalizedAllocatorsAreReleasedByCollection()
        {
            // The historical behaviour, kept for any allocator whose Lifetime is Finalized,
            // including custom allocators that rely on it.
            CountingAllocator allocator = new CountingAllocator(AllocationLifetime.Finalized);
            BuildAndAbandon(allocator);

            Collect();

            Assert.Equal(0, allocator.LiveAllocations);
        }

        [Theory]
        [InlineData(AllocationLifetime.CallerOwned)]
        [InlineData(AllocationLifetime.GarbageCollected)]
        public void OtherLifetimesOptOutOfReleaseByCollection(AllocationLifetime lifetime)
        {
            // Release by collection is what lets a span outlive its memory (issue #397), so
            // allocators with other lifetimes opt out of it.
            CountingAllocator allocator = new CountingAllocator(lifetime);
            BuildAndAbandon(allocator);

            Collect();

            Assert.Equal(1, allocator.LiveAllocations);
        }

        [Theory]
        [InlineData(MemoryAllocator.DefaultAlignment)]
        [InlineData(128)]
        [InlineData(256)]
        public void NativeAllocatorRemovesTheMemoryPressureItAdds(int alignment)
        {
            // https://github.com/apache/arrow-dotnet/issues/50
            NativeMemoryAllocator allocator = new NativeMemoryAllocator(alignment);
            allocator.Allocate(10).Dispose();

            Assert.Equal(10 + alignment, allocator.Statistics.BytesAllocated);
            Assert.Equal(10 + alignment, allocator.ReleasedPressureBytes(10));
        }

        /// <summary>
        /// Builds a buffer that is never disposed and that nothing references afterwards. The
        /// builder is disposed, so only the buffer's own handle holds the allocation.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void BuildAndAbandon(MemoryAllocator allocator)
        {
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            builder.Append(1).Build();
        }

        private static void Collect()
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }

        /// <summary>
        /// An allocator over managed arrays that reports a chosen lifetime and counts allocations
        /// not yet released. Its owners have no finalizer, so only the buffer handle can release
        /// an allocation that is never disposed.
        /// </summary>
        private sealed class CountingAllocator : MemoryAllocator
        {
            private int _live;

            public CountingAllocator(AllocationLifetime lifetime) => Lifetime = lifetime;

            public override AllocationLifetime Lifetime { get; }

            public int LiveAllocations => Volatile.Read(ref _live);

            protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                Interlocked.Increment(ref _live);
                bytesAllocated = length;
                return new Owner(this, new byte[length]);
            }

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly CountingAllocator _allocator;
                private byte[] _buffer;

                public Owner(CountingAllocator allocator, byte[] buffer)
                {
                    _allocator = allocator;
                    _buffer = buffer;
                }

                public Memory<byte> Memory => _buffer ?? throw new ObjectDisposedException(nameof(Owner));

                public void Dispose()
                {
                    if (Interlocked.Exchange(ref _buffer, null) != null)
                    {
                        Interlocked.Decrement(ref _allocator._live);
                    }
                }
            }
        }
    }
}
