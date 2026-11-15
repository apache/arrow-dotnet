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
using System.Threading.Tasks;
using Apache.Arrow.Memory;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class AllocatorTests
    {
        private const int Elements = 4096;

        [Fact]
        public void LifetimeDescribesEachAllocator()
        {
            Assert.Equal(AllocationLifetime.GarbageCollected, new PooledManagedAllocator().Lifetime);
            Assert.Equal(AllocationLifetime.GarbageCollected, new ManagedMemoryAllocator().Lifetime);
            Assert.Equal(AllocationLifetime.CallerOwned, new CallerOwnedNativeAllocator().Lifetime);
            Assert.Equal(AllocationLifetime.Finalized, new NativeMemoryAllocator().Lifetime);
        }

        [Fact]
        public void DefaultAllocatorIsUnchanged()
        {
            // Out-of-the-box behaviour: anything not given an allocator uses native memory.
            Assert.IsType<NativeMemoryAllocator>(MemoryAllocator.Default.Value);
        }

        [Theory]
        [MemberData(nameof(Allocators))]
        public void AllocatesZeroedMemory(MemoryAllocator allocator)
        {
            using (IMemoryOwner<byte> owner = allocator.Allocate(1024))
            {
                Assert.Equal(1024, owner.Memory.Length);
                ReadOnlySpan<byte> span = owner.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    Assert.Equal(0, span[i]);
                }
            }
        }

        [Theory]
        [MemberData(nameof(Allocators))]
        public void RoundTripsDataThroughBuilders(MemoryAllocator allocator)
        {
            Int32Array array = BuildInt32Array(allocator, Elements);

            Assert.Equal(Elements, array.Length);
            for (int i = 0; i < Elements; i++)
            {
                Assert.Equal(i, array.GetValue(i));
            }
        }

        [Theory]
        [MemberData(nameof(Allocators))]
        public void DoubleDisposeIsSafe(MemoryAllocator allocator)
        {
            IMemoryOwner<byte> owner = allocator.Allocate(256);
            owner.Dispose();
            owner.Dispose();
        }

        [Theory]
        [MemberData(nameof(Allocators))]
        public void ResizePreservesContents(MemoryAllocator allocator)
        {
            IMemoryOwner<byte> owner = allocator.Allocate(128);
            for (int i = 0; i < 128; i++)
            {
                owner.Memory.Span[i] = (byte)(i & 0xFF);
            }

            IMemoryOwner<byte> resized = allocator.Resize(owner, 512);
            try
            {
                Assert.Equal(512, resized.Memory.Length);
                ReadOnlySpan<byte> span = resized.Memory.Span;
                for (int i = 0; i < 128; i++)
                {
                    Assert.Equal((byte)(i & 0xFF), span[i]);
                }

                // Newly grown region must be zeroed.
                for (int i = 128; i < 512; i++)
                {
                    Assert.Equal(0, span[i]);
                }
            }
            finally
            {
                resized.Dispose();
            }
        }

        /// <summary>
        /// The regression test for https://github.com/apache/arrow-dotnet/issues/397: a span
        /// taken from a buffer must stay valid across a collection even when nothing else
        /// references the array it came from.
        /// </summary>
        [Theory]
        [MemberData(nameof(Allocators))]
        public void SpansSurviveCollectionWithoutKeepAlive(MemoryAllocator allocator)
        {
            AssertSpanSurvivesCollection(allocator);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void AssertSpanSurvivesCollection(MemoryAllocator allocator)
        {
            Int32Array array = BuildInt32Array(allocator, Elements);
            ReadOnlySpan<int> values = array.Values;

            // Drop the only strong reference to the array before reading the span.
            array = null;
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();

            for (int i = 0; i < Elements; i++)
            {
                Assert.Equal(i, values[i]);
            }
        }

        [Fact]
        public void PooledAllocatorReusesBuffers()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            IMemoryOwner<byte> first = allocator.Allocate(64 * 1024);
            byte[] firstArray = GetBackingArray(first);
            first.Dispose();

            IMemoryOwner<byte> second = allocator.Allocate(64 * 1024);
            byte[] secondArray = GetBackingArray(second);
            second.Dispose();

            Assert.Same(firstArray, secondArray);
        }

        [Fact]
        public void PooledAllocatorZeroesReusedBuffers()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            IMemoryOwner<byte> first = allocator.Allocate(4096);
            first.Memory.Span.Fill(0xFF);
            first.Dispose();

            using (IMemoryOwner<byte> second = allocator.Allocate(4096))
            {
                ReadOnlySpan<byte> span = second.Memory.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    Assert.Equal(0, span[i]);
                }
            }
        }

        [Fact]
        public void CallerOwnedAllocatorHonoursAlignment()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator(alignment: 64);

            using (IMemoryOwner<byte> owner = allocator.Allocate(1000))
            {
                using (MemoryHandle handle = owner.Memory.Pin())
                {
                    unsafe
                    {
                        Assert.Equal(0, (long)handle.Pointer % 64);
                    }
                }
            }
        }

        [Fact]
        public void CallerOwnedAllocatorRejectsNonPowerOfTwoAlignment()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() => new CallerOwnedNativeAllocator(alignment: 48));
        }

        [Fact]
        public void BuilderAllocatorBacksTheBuiltArray()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            Int32Array array = BuildInt32Array(allocator, 128);

            Assert.True(allocator.Statistics.Allocations > 0);
            Assert.True(MemoryMarshal.TryGetArray(array.ValueBuffer.Memory, out ArraySegment<byte> _));
        }

        [Fact]
        public void BuilderWorkingMemoryComesFromAllocator()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            using (ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(4, allocator))
            {
                long initialAllocations = allocator.Statistics.Allocations;
                Assert.Equal(1, initialAllocations);

                // Force several growth steps.
                for (int i = 0; i < 4096; i++)
                {
                    builder.Append(i);
                }

                // Growth now goes through the allocator instead of `new byte[]`.
                Assert.True(allocator.Statistics.Allocations > initialAllocations);
            }
        }

        [Fact]
        public void BitmapBuilderWorkingMemoryComesFromAllocator()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            using (ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(8, allocator))
            {
                long initialAllocations = allocator.Statistics.Allocations;

                for (int i = 0; i < 4096; i++)
                {
                    builder.Append(i % 3 == 0);
                }

                Assert.True(allocator.Statistics.Allocations > initialAllocations);
                Assert.Equal(4096, builder.Length);
            }
        }

        [Fact]
        public void BitmapBuilderGrowthZeroesNewBytes()
        {
            // The bitmap relies on grown regions being zero, so Resize must not leave garbage.
            using (ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(8, new PooledManagedAllocator()))
            {
                for (int i = 0; i < 4096; i++)
                {
                    builder.Append(false);
                }

                Assert.Equal(0, builder.SetBitCount);
                ReadOnlySpan<byte> span = builder.Span;
                for (int i = 0; i < span.Length; i++)
                {
                    Assert.Equal(0, span[i]);
                }
            }
        }

        [Fact]
        public void DisposingBuilderReturnsWorkingMemoryToPool()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            ArrowBuffer.Builder<byte> first = new ArrowBuffer.Builder<byte>(64 * 1024, allocator);
            byte[] firstArray = GetBackingArray(first.Memory);
            first.Dispose();

            ArrowBuffer.Builder<byte> second = new ArrowBuffer.Builder<byte>(64 * 1024, allocator);
            byte[] secondArray = GetBackingArray(second.Memory);
            second.Dispose();

            Assert.Same(firstArray, secondArray);
        }

        [Fact]
        public void BuiltBufferOutlivesBuilderDisposal()
        {
            // Build() usually hands the builder's working memory to the buffer, so disposing
            // the builder must release only its own reference, not the buffer's memory.
            ArrowBuffer buffer;
            using (ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(8, new PooledManagedAllocator()))
            {
                for (int i = 0; i < 512; i++)
                {
                    builder.Append(i);
                }

                buffer = builder.Build();
            }

            ReadOnlySpan<int> values = buffer.Span.CastTo<int>();
            for (int i = 0; i < 512; i++)
            {
                Assert.Equal(i, values[i]);
            }
        }

        [Fact]
        public void ManagedAllocatorSpansStayValidAfterDisposal()
        {
            IMemoryOwner<byte> owner = new ManagedMemoryAllocator().Allocate(16);
            Span<byte> span = owner.Memory.Span;
            span[0] = 42;

            owner.Dispose();

            // Nothing is reused, so even a span used after disposal still sees its own data.
            Assert.Equal(42, span[0]);
            Assert.True(owner.Memory.IsEmpty);
        }

        [Fact]
        public void PooledAllocatorDoesNotPoolLargeAllocations()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();
            int large = PooledManagedAllocatorOptions.DefaultMaxPooledLength + 1;

            IMemoryOwner<byte> first = allocator.Allocate(large);
            byte[] firstArray = GetBackingArray(first);
            first.Dispose();

            IMemoryOwner<byte> second = allocator.Allocate(large);
            Assert.NotSame(firstArray, GetBackingArray(second));
            Assert.Equal(0, allocator.RetainedBytes);
            second.Dispose();
        }

        [Fact]
        public void PooledAllocatorKeepsAtMostMaxArraysPerSize()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { MaxArraysPerSize = 2 });

            IMemoryOwner<byte>[] owners = { allocator.Allocate(1000), allocator.Allocate(1000), allocator.Allocate(1000) };
            foreach (IMemoryOwner<byte> owner in owners)
            {
                owner.Dispose();
            }

            Assert.Equal(2 * 1024, allocator.RetainedBytes);
        }

        [Fact]
        public void PooledAllocatorHonoursMaxRetainedBytes()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { MaxRetainedBytes = 3000 });

            IMemoryOwner<byte>[] owners = { allocator.Allocate(1000), allocator.Allocate(1000), allocator.Allocate(1000) };
            foreach (IMemoryOwner<byte> owner in owners)
            {
                owner.Dispose();
            }

            // Each 1000-byte allocation is a 1024-byte array; a third would exceed the limit.
            Assert.Equal(2 * 1024, allocator.RetainedBytes);
        }

        [Fact]
        public void PooledAllocatorStopsRetainingArraysWhenTheyAreReused()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator();

            allocator.Allocate(1000).Dispose();
            Assert.Equal(1024, allocator.RetainedBytes);

            using IMemoryOwner<byte> reused = allocator.Allocate(600);
            Assert.Equal(0, allocator.RetainedBytes);
        }

        [Fact]
        public void PooledAllocatorCanPoisonDisposedBuffers()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });

            IMemoryOwner<byte> owner = allocator.Allocate(100);
            owner.Memory.Span.Fill(1);
            byte[] array = GetBackingArray(owner);
            owner.Dispose();

            // A span wrongly used after disposal would now see the poison pattern...
            Assert.All(array, b => Assert.Equal(0xDE, b));

            // ...while the next buffer to reuse the array still starts zeroed.
            using IMemoryOwner<byte> next = allocator.Allocate(100);
            Assert.Same(array, GetBackingArray(next));
            Assert.All(next.Memory.ToArray(), b => Assert.Equal(0, b));
        }

        [Fact]
        public void PooledAllocatorPoolingCanBeDisabled()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { MaxPooledLength = 0 });

            allocator.Allocate(100).Dispose();

            Assert.Equal(0, allocator.RetainedBytes);
        }

        [Theory]
        [InlineData(-1, 1, 0)]
        [InlineData((1 << 30) + 1, 1, 0)]
        [InlineData(1024, -1, 0)]
        [InlineData(1024, 1, -1)]
        public void PooledAllocatorRejectsInvalidOptions(int maxPooledLength, int maxArraysPerSize, long maxRetainedBytes)
        {
            PooledManagedAllocatorOptions options = new PooledManagedAllocatorOptions
            {
                MaxPooledLength = maxPooledLength,
                MaxArraysPerSize = maxArraysPerSize,
                MaxRetainedBytes = maxRetainedBytes,
            };

            Assert.Throws<ArgumentOutOfRangeException>(() => new PooledManagedAllocator(options));
        }

        [Fact]
        public void PooledAllocatorIsSafeUnderConcurrentUse()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { MaxArraysPerSize = 4 });

            Parallel.For(0, 4000, i =>
            {
                int length = 16 << (i % 12);
                using IMemoryOwner<byte> owner = allocator.Allocate(length);
                Span<byte> span = owner.Memory.Span;
                Assert.Equal(length, span.Length);
                Assert.Equal(0, span[length - 1]);
                span.Fill(0xFF);
            });

            // At most four arrays of each of the twelve sizes used.
            long bound = 0;
            for (int i = 0; i < 12; i++)
            {
                bound += 4L * (16 << i);
            }
            Assert.InRange(allocator.RetainedBytes, 0, bound);
        }

        public static TheoryData<MemoryAllocator> Allocators => new TheoryData<MemoryAllocator>
        {
            new PooledManagedAllocator(),
            new CallerOwnedNativeAllocator(),
            new ManagedMemoryAllocator(),
        };

        private static Int32Array BuildInt32Array(MemoryAllocator allocator, int count)
        {
            using Int32Array.Builder builder = new Int32Array.Builder(0, allocator);
            for (int i = 0; i < count; i++)
            {
                builder.Append(i);
            }

            return builder.Build();
        }

        private static byte[] GetBackingArray(IMemoryOwner<byte> owner)
        {
            return GetBackingArray(owner.Memory);
        }

        private static byte[] GetBackingArray(ReadOnlyMemory<byte> memory)
        {
            Assert.True(MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment));
            return segment.Array;
        }
    }
}
