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
using System.Reflection;
using Apache.Arrow.Memory;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class NativeBufferTests
    {
        [Fact]
        public void AllocWriteReadRoundTrip()
        {
            using var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);

            Assert.Equal(4, buf.Length);

            buf.Span[0] = 10;
            buf.Span[1] = 20;
            buf.Span[2] = 30;
            buf.Span[3] = 40;

            Assert.Equal(10, buf.Span[0]);
            Assert.Equal(20, buf.Span[1]);
            Assert.Equal(30, buf.Span[2]);
            Assert.Equal(40, buf.Span[3]);
        }

        [Fact]
        public void ZeroFillInitializesToZero()
        {
            using var buf = new NativeBuffer<long, NoOpAllocationTracker>(8, zeroFill: true);

            for (int i = 0; i < buf.Length; i++)
            {
                Assert.Equal(0L, buf.Span[i]);
            }
        }

        [Fact]
        public void GrowPreservesExistingData()
        {
            using var buf = new NativeBuffer<int, NoOpAllocationTracker>(3);

            buf.Span[0] = 100;
            buf.Span[1] = 200;
            buf.Span[2] = 300;

            buf.Grow(10);

            Assert.True(buf.Length >= 10);
            Assert.Equal(100, buf.Span[0]);
            Assert.Equal(200, buf.Span[1]);
            Assert.Equal(300, buf.Span[2]);
        }

        [Fact]
        public void GrowWithSmallerOrEqualCountIsNoOp()
        {
            using var buf = new NativeBuffer<int, NoOpAllocationTracker>(5);

            buf.Span[0] = 42;

            buf.Grow(5);
            Assert.Equal(5, buf.Length);
            Assert.Equal(42, buf.Span[0]);

            buf.Grow(3);
            Assert.Equal(5, buf.Length);
            Assert.Equal(42, buf.Span[0]);
        }

        [Fact]
        public void BuildTransfersOwnershipToArrowBuffer()
        {
            var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
            buf.Span[0] = 1;
            buf.Span[1] = 2;

            using ArrowBuffer arrow = buf.Build();

            Assert.True(arrow.Length > 0);
            var span = arrow.Span.CastTo<int>();
            Assert.Equal(1, span[0]);
            Assert.Equal(2, span[1]);
        }

        [Fact]
        public void BuildMakesBufferUnusable()
        {
            var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
            using ArrowBuffer arrow = buf.Build();

            Assert.Throws<ObjectDisposedException>(() => buf.Grow(10));
        }

        [Fact]
        public void DoubleDisposeDoesNotThrow()
        {
            var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
            buf.Dispose();
            buf.Dispose();
        }

        [Fact]
        public void GrowAfterDisposeThrows()
        {
            var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
            buf.Dispose();

            Assert.Throws<ObjectDisposedException>(() => buf.Grow(10));
        }

        [Fact]
        public void BuildAfterDisposeThrows()
        {
            var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
            buf.Dispose();

            Assert.Throws<ObjectDisposedException>(() => buf.Build());
        }

        [Fact]
        public void ZeroElementBufferIsValid()
        {
            using var buf = new NativeBuffer<int, NoOpAllocationTracker>(0);

            Assert.Equal(0, buf.Length);
            Assert.Equal(0, buf.ByteSpan.Length);
        }

        [Fact]
        public void ByteSpanReflectsTypedSize()
        {
            using var buf = new NativeBuffer<long, NoOpAllocationTracker>(3);

            Assert.Equal(3, buf.Length);
            Assert.Equal(3 * sizeof(long), buf.ByteSpan.Length);
        }

        [Fact]
        public void MemoryPressureTrackerNotifiesGC()
        {
            // Smoke test: just verify it doesn't throw.
            // We can't directly observe GC memory pressure, but we verify the
            // alloc/dealloc cycle completes without error.
            var buf = new NativeBuffer<byte, MemoryPressureAllocationTracker>(1024);
            buf.Span[0] = 0xFF;
            buf.Dispose();
        }

#if !NET6_0_OR_GREATER
        /// <summary>
        /// Helper that forces <c>AlignedNative.s_hasCrt</c> to the given value via reflection,
        /// runs <paramref name="action"/>, then restores the original value. This is only used
        /// on net462 and net472, where we know it will work.
        /// </summary>
        private static void WithForcedFallback(Action action)
        {
            var type = typeof(Apache.Arrow.Memory.AlignedNative);
            var field = type.GetField("s_hasCrt", BindingFlags.NonPublic | BindingFlags.Static);
            bool original = (bool)field.GetValue(null);
            try
            {
                field.SetValue(null, false);
                action();
            }
            finally
            {
                field.SetValue(null, original);
            }
        }

        [Fact]
        public void FallbackAllocWriteReadRoundTrip()
        {
            WithForcedFallback(() =>
            {
                using var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);

                buf.Span[0] = 10;
                buf.Span[1] = 20;
                buf.Span[2] = 30;
                buf.Span[3] = 40;

                Assert.Equal(10, buf.Span[0]);
                Assert.Equal(20, buf.Span[1]);
                Assert.Equal(30, buf.Span[2]);
                Assert.Equal(40, buf.Span[3]);
            });
        }

        [Fact]
        public void FallbackGrowPreservesExistingData()
        {
            WithForcedFallback(() =>
            {
                using var buf = new NativeBuffer<int, NoOpAllocationTracker>(3);

                buf.Span[0] = 100;
                buf.Span[1] = 200;
                buf.Span[2] = 300;

                buf.Grow(10);

                Assert.True(buf.Length >= 10);
                Assert.Equal(100, buf.Span[0]);
                Assert.Equal(200, buf.Span[1]);
                Assert.Equal(300, buf.Span[2]);
            });
        }

        [Fact]
        public void FallbackBuildTransfersOwnership()
        {
            WithForcedFallback(() =>
            {
                var buf = new NativeBuffer<int, NoOpAllocationTracker>(4);
                buf.Span[0] = 1;
                buf.Span[1] = 2;

                using ArrowBuffer arrow = buf.Build();

                var span = arrow.Span.CastTo<int>();
                Assert.Equal(1, span[0]);
                Assert.Equal(2, span[1]);
            });
        }

        [Fact]
        public void FallbackMultipleGrowsPreserveData()
        {
            WithForcedFallback(() =>
            {
                using var buf = new NativeBuffer<long, NoOpAllocationTracker>(2);

                buf.Span[0] = 111;
                buf.Span[1] = 222;

                buf.Grow(5);
                buf.Span[2] = 333;
                buf.Span[3] = 444;
                buf.Span[4] = 555;

                buf.Grow(20);

                Assert.Equal(111, buf.Span[0]);
                Assert.Equal(222, buf.Span[1]);
                Assert.Equal(333, buf.Span[2]);
                Assert.Equal(444, buf.Span[3]);
                Assert.Equal(555, buf.Span[4]);
            });
        }
#endif
    }
}
