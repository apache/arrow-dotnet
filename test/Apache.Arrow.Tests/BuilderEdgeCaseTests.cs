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
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Edge cases in allocator-backed builders: allocators that do not zero their memory, appends
    /// whose source is the builder's own memory, and reuse of a buffer after the builder changes.
    /// </summary>
    public class BuilderEdgeCaseTests
    {
        [Fact]
        public void GrowingResizeMarksNewSlotsNullEvenWithoutZeroedMemory()
        {
            // Allocators are not required to zero memory (GarbageMemoryAllocator fills it with 0xA5).
            GarbageMemoryAllocator allocator = new GarbageMemoryAllocator();
            using Int32Array.Builder builder = new Int32Array.Builder(0, allocator);

            using Int32Array array = builder.Resize(3).Build();

            Assert.Equal(3, array.NullCount);
            for (int i = 0; i < 3; i++)
            {
                Assert.True(array.IsNull(i));
            }
        }

        [Fact]
        public void GrowingBitmapResizeAddsUnsetBitsEvenWithoutZeroedMemory()
        {
            using ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(0, new GarbageMemoryAllocator());
            builder.Append(true);

            builder.Resize(16);

            Assert.Equal(1, builder.SetBitCount);
            Assert.Equal(15, builder.UnsetBitCount);
        }

        [Fact]
        public void ClearAfterBuildZeroesTheBuilderEvenWithoutZeroedMemory()
        {
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, new GarbageMemoryAllocator());
            builder.Append(1);
            using ArrowBuffer built = builder.Build();

            builder.Clear();

            Assert.All(builder.Span.ToArray(), v => Assert.Equal(0, v));
        }

        [Fact]
        public void CopiedBufferPaddingIsZeroedEvenWithoutZeroedMemory()
        {
            GarbageMemoryAllocator allocator = new GarbageMemoryAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(1024, new CallerOwnedNativeAllocator());
            builder.Append(7);

            // A different target allocator forces the copying path.
            using ArrowBuffer buffer = builder.Build(allocator);

            Assert.Equal(7, buffer.Span.CastTo<int>()[0]);
            for (int i = sizeof(int); i < 64; i++)
            {
                Assert.Equal(0, buffer.Span[i]);
            }
        }

        [Fact]
        public void AppendingTheBuildersOwnSpanSurvivesGrowth()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            for (int i = 0; i < builder.Capacity; i++)
            {
                builder.Append(i);
            }
            int count = builder.Length;

            // The source is the builder's own memory, which growing would otherwise release.
            builder.Append(builder.Span.Slice(0, count));

            Assert.Equal(2 * count, builder.Length);
            for (int i = 0; i < count; i++)
            {
                Assert.Equal(i, builder.Span[i]);
                Assert.Equal(i, builder.Span[count + i]);
            }
        }

        [Fact]
        public void AppendingTheBitmapBuildersOwnSpanSurvivesGrowth()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });
            using ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(512, allocator);
            int bits = builder.Capacity;
            for (int i = 0; i < bits; i++)
            {
                builder.Append(i % 3 == 0);
            }
            int setBits = builder.SetBitCount;

            builder.Append(builder.Span.Slice(0, bits / 8), bits);

            Assert.Equal(2 * bits, builder.Length);
            Assert.Equal(2 * setBits, builder.SetBitCount);
            for (int i = 0; i < bits; i++)
            {
                Assert.Equal(i % 3 == 0, BitUtility.GetBit(builder.Span, bits + i));
            }
        }

        [Fact]
        public void AppendingASpanCapturedBeforeBuildSurvivesTheHandOver()
        {
            // After a hand-over, the builder holds the only remaining reference once the built
            // buffer is disposed; writing to the builder releases it. A span captured earlier
            // points into that memory, so it must be copied before the release.
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            builder.Append(0).Append(1).Append(2).Append(3);
            Span<int> captured = builder.Span.Slice(0, 4);
            builder.Build().Dispose();

            builder.Append(captured);

            Assert.Equal(new[] { 0, 1, 2, 3, 0, 1, 2, 3 }, builder.Span.Slice(0, 8).ToArray());
        }

        [Fact]
        public void AppendingABitmapSpanCapturedBeforeBuildSurvivesTheHandOver()
        {
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });
            using ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(64, allocator);
            for (int i = 0; i < 16; i++)
            {
                builder.Append(i % 3 == 0);
            }
            Span<byte> captured = builder.Span.Slice(0, 2);
            builder.Build().Dispose();

            builder.Append(captured, 16);

            Assert.Equal(32, builder.Length);
            Assert.Equal(12, builder.SetBitCount);
            for (int i = 0; i < 16; i++)
            {
                Assert.Equal(i % 3 == 0, BitUtility.GetBit(builder.Span, 16 + i));
            }
        }

        [Fact]
        public void RebuildingAfterShrinkingDoesNotExposeRemovedValues()
        {
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, new CallerOwnedNativeAllocator());
            for (int i = 1; i <= 16; i++)
            {
                builder.Append(i);
            }
            using ArrowBuffer first = builder.Build();

            builder.Resize(1);
            using ArrowBuffer second = builder.Build();

            // The second buffer's padding must be zero, not the values the resize removed...
            Assert.Equal(1, second.Span.CastTo<int>()[0]);
            for (int i = sizeof(int); i < second.Length; i++)
            {
                Assert.Equal(0, second.Span[i]);
            }

            // ...and the first buffer must be unchanged.
            for (int i = 0; i < 16; i++)
            {
                Assert.Equal(i + 1, first.Span.CastTo<int>()[i]);
            }
        }

        [Fact]
        public void FailedRunEndConcatenationReleasesItsWorkingMemory()
        {
            // Two Int16 run-end arrays of logical length 20,000 overflow Int16 when concatenated.
            RunEndEncodedArray first = new RunEndEncodedArray(
                new Int16Array.Builder().Append(20000).Build(),
                new StringArray.Builder().Append("A").Build());
            RunEndEncodedArray second = new RunEndEncodedArray(
                new Int16Array.Builder().Append(20000).Build(),
                new StringArray.Builder().Append("B").Build());
            GarbageMemoryAllocator allocator = new GarbageMemoryAllocator();

            Assert.ThrowsAny<Exception>(() => ArrowArrayConcatenator.Concatenate(new IArrowArray[] { first, second }, allocator));

            Assert.Equal(0, allocator.Outstanding);
        }

        [Fact]
        public void TimestampBuilderWithNullTypeAllocatesNothing()
        {
            GarbageMemoryAllocator allocator = new GarbageMemoryAllocator();

            Assert.Throws<ArgumentNullException>(() => new TimestampArray.Builder((TimestampType)null, 100, allocator));

            Assert.Equal(0, allocator.Outstanding);
        }
    }
}
