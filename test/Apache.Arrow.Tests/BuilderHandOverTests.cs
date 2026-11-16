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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests that buffer builders hand their working memory to the buffer they build instead
    /// of copying it, and that doing so never lets a later write reach a built buffer.
    /// </summary>
    public class BuilderHandOverTests
    {
        [Fact]
        public void BuilderWithFinalCapacityAllocatesOnce()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(100, allocator);

            for (int i = 0; i < 100; i++)
            {
                builder.Append(i);
            }

            ref byte working = ref MemoryMarshal.GetReference(builder.Memory.Span);
            using ArrowBuffer buffer = builder.Build();

            Assert.Equal(1, allocator.Statistics.Allocations);
            Assert.True(Unsafe.AreSame(ref working, ref MemoryMarshal.GetReference(buffer.Span)));
            Assert.Equal(448, buffer.Length); // 400 bytes padded to a multiple of 64
            AssertSequence(buffer, 100);
        }

        [Fact]
        public void PaddingIsZeroedEvenIfWorkingMemoryWasWritten()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);

            // Resize exposes the region; shrinking back leaves the values behind in working memory.
            builder.Resize(16);
            builder.Span.Fill(-1);
            builder.Resize(2);

            using ArrowBuffer buffer = builder.Build();

            ReadOnlySpan<int> values = buffer.Span.CastTo<int>();
            Assert.Equal(-1, values[0]);
            Assert.Equal(-1, values[1]);
            for (int i = 2; i < values.Length; i++)
            {
                Assert.Equal(0, values[i]);
            }
        }

        [Fact]
        public void AppendingAfterBuildDoesNotModifyBuiltBuffer()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            builder.Append(1).Append(2);

            using ArrowBuffer first = builder.Build();
            builder.Append(3);
            builder.Span[0] = 100;
            using ArrowBuffer second = builder.Build();

            Assert.Equal(new[] { 1, 2 }, first.Span.CastTo<int>().Slice(0, 2).ToArray());
            Assert.Equal(new[] { 100, 2, 3 }, second.Span.CastTo<int>().Slice(0, 3).ToArray());
            Assert.Equal(0, first.Span.CastTo<int>()[2]);
        }

        [Fact]
        public void WritingThroughSpanAfterBuildDoesNotModifyBuiltBuffer()
        {
            using ArrowBuffer.Builder<long> builder = new ArrowBuffer.Builder<long>(8, new CallerOwnedNativeAllocator());
            builder.Append(7);

            using ArrowBuffer buffer = builder.Build();
            builder.Span[0] = 8;

            Assert.Equal(7, buffer.Span.CastTo<long>()[0]);
        }

        [Fact]
        public void ClearAfterBuildDiscardsWithoutCopying()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            builder.Append(1).Append(2);

            using ArrowBuffer first = builder.Build();
            builder.Clear();
            builder.Append(5);
            using ArrowBuffer second = builder.Build();

            // One allocation for the first build's memory and one fresh one after Clear.
            Assert.Equal(2, allocator.Statistics.Allocations);
            Assert.Equal(new[] { 1, 2 }, first.Span.CastTo<int>().Slice(0, 2).ToArray());
            Assert.Equal(new[] { 5, 0 }, second.Span.CastTo<int>().Slice(0, 2).ToArray());
        }

        [Fact]
        public void BuildingTwiceWithoutWritingSharesMemory()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, allocator);
            builder.Append(42);

            ArrowBuffer first = builder.Build();
            ArrowBuffer second = builder.Build();

            Assert.Equal(1, allocator.Statistics.Allocations);
            Assert.True(Unsafe.AreSame(
                ref MemoryMarshal.GetReference(first.Span),
                ref MemoryMarshal.GetReference(second.Span)));

            // Each holder releases its own reference; the memory survives until the last.
            first.Dispose();
            builder.Dispose();
            Assert.Equal(42, second.Span.CastTo<int>()[0]);
            second.Dispose();
        }

        [Fact]
        public void BuiltBufferSurvivesBuilderDisposal()
        {
            ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(64, new CallerOwnedNativeAllocator());
            for (int i = 0; i < 64; i++)
            {
                builder.Append(i);
            }

            using ArrowBuffer buffer = builder.Build();
            builder.Dispose();

            AssertSequence(buffer, 64);
        }

        [Fact]
        public void DisposingBuiltBufferLeavesBuilderReadableWithAFinalizedAllocator()
        {
            // With a Finalized lifetime the builder holds its own reference to what it built,
            // which collection reclaims, so its contents outlive the built buffer as before.
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(8, MemoryAllocator.Default.Value);
            builder.Append(1).Append(2);

            builder.Build().Dispose();
            builder.Append(3);

            Assert.Equal(new[] { 1, 2, 3 }, builder.Span.Slice(0, 3).ToArray());
        }

        [Fact]
        public void BuilderContentsLastAsLongAsTheBuiltBufferWithOtherLifetimes()
        {
            // Other lifetimes hold no reference, so that a builder that is never disposed cannot
            // leak what it built. Its contents are the built buffer's, and go when it does.
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(8, new CallerOwnedNativeAllocator());
            builder.Append(1).Append(2);

            using (ArrowBuffer built = builder.Build())
            {
                // While the built buffer is alive, the builder can still be read and extended.
                builder.Append(3);
                Assert.Equal(new[] { 1, 2, 3 }, builder.Span.Slice(0, 3).ToArray());
                Assert.Equal(new[] { 1, 2 }, built.Span.CastTo<int>().Slice(0, 2).ToArray());
            }

            builder.Build().Dispose();

            Assert.Throws<ObjectDisposedException>(() => builder.Append(4));
            Assert.Throws<ObjectDisposedException>(() => builder.Build());

            // Clearing needs none of the old contents, so it makes the builder usable again.
            builder.Clear();
            builder.Append(5);
            using ArrowBuffer rebuilt = builder.Build();
            Assert.Equal(5, rebuilt.Span.CastTo<int>()[0]);
        }

        [Fact]
        public void DifferentAllocatorForcesCopy()
        {
            CallerOwnedNativeAllocator working = new CallerOwnedNativeAllocator();
            CallerOwnedNativeAllocator target = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(16, working);
            builder.Append(1);

            ref byte workingRef = ref MemoryMarshal.GetReference(builder.Memory.Span);
            using ArrowBuffer buffer = builder.Build(target);

            Assert.Equal(1, target.Statistics.Allocations);
            Assert.False(Unsafe.AreSame(ref workingRef, ref MemoryMarshal.GetReference(buffer.Span)));

            // The builder kept its working memory, so writing to it costs nothing.
            builder.Append(2);
            Assert.Equal(1, working.Statistics.Allocations);
        }

        [Fact]
        public void LargeReservationForSmallBufferIsCopied()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(1024, allocator);
            builder.Append(1);

            using ArrowBuffer buffer = builder.Build();

            Assert.Equal(2, allocator.Statistics.Allocations);
            Assert.Equal(64, buffer.Length);
        }

        [Fact]
        public void GrowingBuilderHandsOverItsFinalBuffer()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>(8, allocator);
            for (int i = 0; i < 1000; i++)
            {
                builder.Append(i);
            }

            long allocationsBeforeBuild = allocator.Statistics.Allocations;
            using ArrowBuffer buffer = builder.Build();

            Assert.Equal(allocationsBeforeBuild, allocator.Statistics.Allocations);
            AssertSequence(buffer, 1000);
        }

        [Fact]
        public void BitmapBuilderHandsOverAndIsolatesLaterWrites()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer.BitmapBuilder builder = new ArrowBuffer.BitmapBuilder(64, allocator);
            builder.Append(true).Append(false).Append(true);

            using ArrowBuffer buffer = builder.Build();
            Assert.Equal(1, allocator.Statistics.Allocations);

            builder.Set(1);
            builder.Toggle(0);

            Assert.Equal(0b101, buffer.Span[0]);
            Assert.Equal(0b110, builder.Span[0]);
            Assert.Equal(2, builder.SetBitCount);
        }

        [Fact]
        public void ArrayBuilderCanBeReusedAfterBuild()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            builder.Append(1).AppendNull().Append(3);

            Int32Array first = builder.Build();
            builder.Append(4).Set(0, 10);
            Int32Array second = builder.Build();
            builder.Clear().Append(99);
            Int32Array third = builder.Build();

            Assert.Equal(new int?[] { 1, null, 3 }, ToArray(first));
            Assert.Equal(new int?[] { 10, null, 3, 4 }, ToArray(second));
            Assert.Equal(new int?[] { 99 }, ToArray(third));
        }

        [Fact]
        public void CloneAllocatesOnce()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using ArrowBuffer source = new ArrowBuffer(new byte[] { 1, 2, 3 });

            using ArrowBuffer clone = source.Clone(allocator);

            // Clone builds in the target allocator, so its working memory becomes the clone.
            Assert.Equal(1, allocator.Statistics.Allocations);
            Assert.Equal(new byte[] { 1, 2, 3 }, clone.Span.Slice(0, 3).ToArray());
        }

        private static void AssertSequence(ArrowBuffer buffer, int count)
        {
            ReadOnlySpan<int> values = buffer.Span.CastTo<int>();
            for (int i = 0; i < count; i++)
            {
                Assert.Equal(i, values[i]);
            }
        }

        private static int?[] ToArray(Int32Array array)
        {
            int?[] result = new int?[array.Length];
            for (int i = 0; i < array.Length; i++)
            {
                result[i] = array.GetValue(i);
            }
            return result;
        }
    }

    /// <summary>
    /// Tests that the fixed-width array builders allocate once when given their final length,
    /// counting allocations through the allocator each builder is given.
    /// </summary>
    public class ArrayBuilderSingleAllocationTests
    {
        [Fact]
        public void CapacityConstructorWithGetSpanAllocatesOnce()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using Int32Array.Builder builder = new Int32Array.Builder(100, allocator);
            Span<int> span = builder.GetSpan(100);
            Assert.Equal(100, span.Length);
            for (int i = 0; i < span.Length; i++)
            {
                span[i] = i;
            }

            using Int32Array array = builder.Advance(100).Build();

            Assert.Equal(1, allocator.Statistics.Allocations);
            Assert.Equal(100, array.Length);
            Assert.Equal(0, array.NullCount);
            Assert.True(array.NullBitmapBuffer.IsEmpty);
            Assert.Equal(99, array.GetValue(99));
        }

        [Fact]
        public void ZeroCapacityThenReserveAllocatesOnce()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using Int64Array.Builder builder = new Int64Array.Builder(0, allocator);
            Assert.Equal(0, allocator.Statistics.Allocations);

            builder.Reserve(100);
            for (long i = 0; i < 100; i++)
            {
                builder.Append(i);
            }

            using Int64Array array = builder.Build();

            Assert.Equal(1, allocator.Statistics.Allocations);
            Assert.Equal(99L, array.GetValue(99));
        }

        [Fact]
        public void TypedBuildersBuildFromTheirAllocator()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();

            using TimestampArray.Builder timestampBuilder = new TimestampArray.Builder(TimestampType.Default, 16, allocator);
            using Date32Array.Builder dateBuilder = new Date32Array.Builder(16, allocator);
            using Time64Array.Builder timeBuilder = new Time64Array.Builder(Time64Type.Default, 16, allocator);
            using DurationArray.Builder durationBuilder = new DurationArray.Builder(DurationType.Millisecond, 16, allocator);
            using DayTimeIntervalArray.Builder intervalBuilder = new DayTimeIntervalArray.Builder(16, allocator);

            using TimestampArray timestamps = timestampBuilder.Append(DateTimeOffset.FromUnixTimeMilliseconds(1000)).Build();
            using Date32Array dates = dateBuilder.Append(new DateTime(2000, 1, 1)).Build();
            using Time64Array times = timeBuilder.Append(5).Build();
            using DurationArray durations = durationBuilder.Append(7).Build();
            using DayTimeIntervalArray intervals = intervalBuilder.Append(new Scalars.DayTimeInterval(1, 2)).Build();

            Assert.Equal(5, allocator.Statistics.Allocations);
            Assert.Equal(DateTimeOffset.FromUnixTimeMilliseconds(1000), timestamps.GetTimestamp(0));
            Assert.Equal(new DateTime(2000, 1, 1), dates.GetDateTime(0));
            Assert.Equal(5L, times.GetValue(0));
            Assert.Equal(7L, durations.GetValue(0));
        }

        [Fact]
        public void FirstNullCreatesBitmapWithEarlierValuesValid()
        {
            Int32Array array = new Int32Array.Builder()
                .Append(1)
                .Append(2)
                .AppendNull()
                .Append(4)
                .Build();

            Assert.Equal(1, array.NullCount);
            Assert.Equal(new int?[] { 1, 2, null, 4 }, ToArray(array));
        }

        [Fact]
        public void AdvanceAfterNullMarksValuesValid()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            builder.AppendNull();
            Span<int> span = builder.GetSpan(2);
            span[0] = 10;
            span[1] = 11;
            builder.Advance(2);

            Assert.Equal(new int?[] { null, 10, 11 }, ToArray(builder.Build()));
        }

        [Fact]
        public void GrowingResizeStillAddsNulls()
        {
            Int32Array array = new Int32Array.Builder().Resize(3).Set(1, 5).Build();

            Assert.Equal(new int?[] { null, 5, null }, ToArray(array));
        }

        [Fact]
        public void SetAndSwapRejectIndexesPastLength()
        {
            Int32Array.Builder builder = new Int32Array.Builder(16).Append(1);

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.Set(1, 5));
            Assert.Throws<ArgumentOutOfRangeException>(() => builder.Swap(0, 1));
        }

        [Fact]
        public void AdvancePastCapacityThrows()
        {
            Int32Array.Builder builder = new Int32Array.Builder();
            builder.GetSpan(4);

            Assert.Throws<ArgumentOutOfRangeException>(() => builder.Advance(1000));
            Assert.Throws<ArgumentOutOfRangeException>(() => builder.Advance(-1));
        }

        [Fact]
        public void ExplicitAllocatorIsUsedForValuesAndBitmap()
        {
            CallerOwnedNativeAllocator allocator = new CallerOwnedNativeAllocator();
            using Int32Array.Builder builder = new Int32Array.Builder(16, allocator);
            using Int32Array array = builder.Append(1).AppendNull().Build();

            Assert.Equal(2, allocator.Statistics.Allocations); // values and bitmap
            Assert.Equal(1, array.NullCount);
        }

        private static int?[] ToArray(Int32Array array)
        {
            int?[] result = new int?[array.Length];
            for (int i = 0; i < array.Length; i++)
            {
                result[i] = array.GetValue(i);
            }
            return result;
        }
    }
}
