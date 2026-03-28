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
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Tests.Fixtures;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowBufferTests
    {
        public class Allocate :
            IClassFixture<DefaultMemoryAllocatorFixture>
        {
            private readonly DefaultMemoryAllocatorFixture _memoryPoolFixture;

            public Allocate(DefaultMemoryAllocatorFixture memoryPoolFixture)
            {
                _memoryPoolFixture = memoryPoolFixture;
            }

            /// <summary>
            /// Ensure Arrow buffers are allocated in multiples of 64 bytes.
            /// </summary>
            /// <param name="size">number of bytes to allocate</param>
            /// <param name="expectedCapacity">expected buffer capacity after allocation</param>
            [Theory]
            [InlineData(0, 0)]
            [InlineData(1, 64)]
            [InlineData(8, 64)]
            [InlineData(9, 64)]
            [InlineData(65, 128)]
            public void AllocatesWithExpectedPadding(int size, int expectedCapacity)
            {
                var builder = new ArrowBuffer.Builder<byte>(size);
                for (int i = 0; i < size; i++)
                {
                    builder.Append(0);
                }
                var buffer = builder.Build();

                Assert.Equal(expectedCapacity, buffer.Length);
            }

            /// <summary>
            /// Ensure allocated buffers are aligned to multiples of 64.
            /// </summary>
            [Theory]
            [InlineData(1)]
            [InlineData(8)]
            [InlineData(64)]
            [InlineData(128)]
            public unsafe void AllocatesAlignedToMultipleOf64(int size)
            {
                var builder = new ArrowBuffer.Builder<byte>(size);
                for (int i = 0; i < size; i++)
                {
                    builder.Append(0);
                }
                var buffer = builder.Build();

                fixed (byte* ptr = &buffer.Span.GetPinnableReference())
                {
                    Assert.True(new IntPtr(ptr).ToInt64() % 64 == 0);
                }
            }

            /// <summary>
            /// Ensure padding in arrow buffers is initialized with zeroes.
            /// </summary>
            [Fact]
            public void HasZeroPadding()
            {
                var buffer = new ArrowBuffer.Builder<byte>(10).Append(0).Build();

                foreach (var b in buffer.Span)
                {
                    Assert.Equal(0, b);
                }
            }

        }

        [Fact]
        public void TestExternalMemoryWrappedAsArrowBuffer()
        {
            Memory<byte> memory = new byte[sizeof(int) * 3];
            Span<byte> spanOfBytes = memory.Span;
            var span = spanOfBytes.CastTo<int>();
            span[0] = 0;
            span[1] = 1;
            span[2] = 2;

            ArrowBuffer buffer = new ArrowBuffer(memory);
            Assert.Equal(2, buffer.Span.CastTo<int>()[2]);

            span[2] = 10;
            Assert.Equal(10, buffer.Span.CastTo<int>()[2]);
        }

        public class Retain
        {
            [Fact]
            public void RetainedBufferSharesMemory()
            {
                ArrowBuffer original = new ArrowBuffer.Builder<int>(3)
                    .Append(1).Append(2).Append(3).Build();

                ArrowBuffer retained = original.Retain();

                Assert.True(original.Span.SequenceEqual(retained.Span));
                Assert.Equal(original.Length, retained.Length);

                // Verify they share the same underlying memory by checking pointer identity
                unsafe
                {
                    fixed (byte* pOriginal = original.Span)
                    fixed (byte* pRetained = retained.Span)
                    {
                        Assert.True(pOriginal == pRetained);
                    }
                }

                original.Dispose();
                retained.Dispose();
            }

            [Fact]
            public void RetainedBufferSurvivesOriginalDispose()
            {
                ArrowBuffer retained;
                var expected = new int[] { 10, 20, 30 };

                using (ArrowBuffer original = new ArrowBuffer.Builder<int>(3)
                    .Append(10).Append(20).Append(30).Build())
                {
                    retained = original.Retain();
                }

                // Original is disposed, but retained should still be valid
                var span = retained.Span.CastTo<int>();
                Assert.Equal(10, span[0]);
                Assert.Equal(20, span[1]);
                Assert.Equal(30, span[2]);

                retained.Dispose();
            }

            [Fact]
            public void MultipleRetainsAllShareMemory()
            {
                ArrowBuffer original = new ArrowBuffer.Builder<byte>(4)
                    .Append(0xAA).Append(0xBB).Append(0xCC).Append(0xDD).Build();

                ArrowBuffer r1 = original.Retain();
                ArrowBuffer r2 = original.Retain();
                ArrowBuffer r3 = r1.Retain();

                // All share the same data
                Assert.True(original.Span.SequenceEqual(r1.Span));
                Assert.True(original.Span.SequenceEqual(r2.Span));
                Assert.True(original.Span.SequenceEqual(r3.Span));

                // Dispose in arbitrary order; last one standing should still work
                original.Dispose();
                r2.Dispose();

                Assert.Equal(0xAA, r1.Span[0]);
                Assert.Equal(0xDD, r3.Span[3]);

                r1.Dispose();

                Assert.Equal(0xDD, r3.Span[3]);

                r3.Dispose();
            }

            [Fact]
            public void RetainOnManagedMemorySharesMemory()
            {
                byte[] data = { 1, 2, 3, 4 };
                ArrowBuffer original = new ArrowBuffer(data);

                ArrowBuffer retained = original.Retain();

                Assert.True(original.Span.SequenceEqual(retained.Span));

                // Managed memory buffers share via ReadOnlyMemory, so pointer identity holds
                unsafe
                {
                    fixed (byte* pOriginal = original.Span)
                    fixed (byte* pRetained = retained.Span)
                    {
                        Assert.True(pOriginal == pRetained);
                    }
                }

                original.Dispose();
                retained.Dispose();
            }

            [Fact]
            public void RetainOnEmptyBuffer()
            {
                ArrowBuffer empty = ArrowBuffer.Empty;

                ArrowBuffer retained = empty.Retain();

                Assert.True(retained.IsEmpty);
                Assert.Equal(0, retained.Length);

                empty.Dispose();
                retained.Dispose();
            }

            [Fact]
            public void ConcurrentRetainAndDispose()
            {
                ArrowBuffer original = new ArrowBuffer.Builder<long>(100)
                    .AppendRange(new long[100])
                    .Build();

                const int threadCount = 8;
                const int iterations = 1000;
                int errors = 0;

                var threads = new Thread[threadCount];
                for (int t = 0; t < threadCount; t++)
                {
                    threads[t] = new Thread(() =>
                    {
                        for (int i = 0; i < iterations; i++)
                        {
                            try
                            {
                                ArrowBuffer retained = original.Retain();
                                _ = retained.Length;
                                retained.Dispose();
                            }
                            catch
                            {
                                Interlocked.Increment(ref errors);
                            }
                        }
                    });
                }

                foreach (var thread in threads) thread.Start();
                foreach (var thread in threads) thread.Join();

                Assert.Equal(0, errors);
                original.Dispose();
            }
        }

        public class SliceSharedTests
        {
            [Fact]
            public void SliceSharedSurvivesOriginalDispose()
            {
                Int32Array original = new Int32Array.Builder()
                    .AppendRange(new[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 })
                    .Build();

                ArrayData sliced = original.Data.SliceShared(2, 5);

                // Dispose the original array
                original.Dispose();

                // Sliced data should still be usable
                var array = new Int32Array(sliced);
                Assert.Equal(5, array.Length);
                Assert.Equal(2, array.GetValue(0));
                Assert.Equal(6, array.GetValue(4));

                sliced.Dispose();
            }

            [Fact]
            public void DoubleSliceSharedSurvivesDispose()
            {
                Int32Array original = new Int32Array.Builder()
                    .AppendRange(new[] { 0, 1, 2, 3, 4, 5 })
                    .Build();

                ArrayData slice1 = original.Data.SliceShared(1, 4);
                ArrayData slice2 = slice1.SliceShared(1, 2);

                original.Dispose();
                slice1.Dispose();

                // Double-sliced data should still work
                var array = new Int32Array(slice2);
                Assert.Equal(2, array.Length);
                Assert.Equal(2, array.GetValue(0));
                Assert.Equal(3, array.GetValue(1));

                slice2.Dispose();
            }

            [Fact]
            public void ArraySliceSharedSurvivesOriginalDispose()
            {
                Int32Array original = new Int32Array.Builder()
                    .AppendRange(new[] { 10, 20, 30, 40, 50 })
                    .Build();

                Array sliced = original.SliceShared(1, 3);

                original.Dispose();

                var typedSlice = (Int32Array)sliced;
                Assert.Equal(3, typedSlice.Length);
                Assert.Equal(20, typedSlice.GetValue(0));
                Assert.Equal(40, typedSlice.GetValue(2));

                sliced.Dispose();
            }

            [Fact]
            public void RecordBatchSliceSharedSurvivesOriginalDispose()
            {
                var batch = new RecordBatch.Builder()
                    .Append("values", false, col => col.Int32(b => b.AppendRange(new[] { 0, 1, 2, 3, 4 })))
                    .Build();

                RecordBatch sliced = batch.SliceShared(1, 3);

                batch.Dispose();

                Assert.Equal(3, sliced.Length);
                var column = (Int32Array)sliced.Column(0);
                Assert.Equal(1, column.GetValue(0));
                Assert.Equal(3, column.GetValue(2));

                sliced.Dispose();
            }
        }
    }
}
