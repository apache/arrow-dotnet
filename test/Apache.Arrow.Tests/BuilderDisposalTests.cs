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
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests that an explicitly passed allocator controls allocation exactly: every buffer of the
    /// result comes from it, and disposing the builders, arrays, readers and writers involved
    /// releases everything it handed out. This is what makes caller-owned allocators usable.
    /// </summary>
    public class BuilderDisposalTests
    {
        [Fact]
        public void PrimitiveBuilderAndArrayReleaseEverything()
        {
            Run(allocator =>
            {
                Int32Array array;
                using (Int32Array.Builder builder = new Int32Array.Builder(0, allocator))
                {
                    array = builder.Append(1).AppendNull().Append(3).Build();
                }

                allocator.AssertOwns(array);
                Assert.Equal(3, array.GetValue(2));
                array.Dispose();
            });
        }

        [Fact]
        public void BuilderCanBeDisposedBeforeOrAfterArray()
        {
            Run(allocator =>
            {
                Int64Array.Builder builder = new Int64Array.Builder(8, allocator).Append(1).Append(2);
                Int64Array array = builder.Build();

                array.Dispose();
                Assert.True(allocator.LiveAllocations > 0, "The builder still holds the shared memory.");
                builder.Dispose();
            });
        }

        [Fact]
        public void WrappedBuilderReleasesInnerBuilder()
        {
            Run(allocator =>
            {
                using TimestampArray.Builder builder = new TimestampArray.Builder(TimestampType.Default, 4, allocator);
                using TimestampArray array = builder.Append(DateTimeOffset.FromUnixTimeMilliseconds(0)).Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void StringBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using StringArray.Builder builder = new StringArray.Builder(allocator);
                using StringArray array = builder.Append("a").AppendNull().Append("bcd").Build();
                allocator.AssertOwns(array);
                Assert.Equal("bcd", array.GetString(2));
            });
        }

        [Fact]
        public void BooleanBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using BooleanArray.Builder builder = new BooleanArray.Builder(allocator);
                using BooleanArray array = builder.Append(true).AppendNull().Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void DecimalBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using Decimal128Array.Builder builder = new Decimal128Array.Builder(new Decimal128Type(10, 2), allocator);
                using Decimal128Array array = builder.Append(1.25m).AppendNull().Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void ListBuilderPassesItsAllocatorToItsChild()
        {
            Run(allocator =>
            {
                using ListArray.Builder builder = new ListArray.Builder(Int32Type.Default, allocator);
                Int32Array.Builder values = (Int32Array.Builder)builder.ValueBuilder;
                builder.Append();
                values.Append(1).Append(2);
                builder.AppendNull();

                using ListArray array = builder.Build();
                allocator.AssertOwns(array);
                Assert.Equal(2, array.GetValueLength(0));
            });
        }

        [Fact]
        public void MapBuilderPassesItsAllocatorToItsChildren()
        {
            Run(allocator =>
            {
                using MapArray.Builder builder = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default), allocator);
                builder.Append();
                ((StringArray.Builder)builder.KeyBuilder).Append("k");
                ((Int32Array.Builder)builder.ValueBuilder).Append(1);

                using MapArray array = builder.Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void ListOfTimestampsKeepsTheElementType()
        {
            // The builder factory used to ignore a timestamp element type's unit and time zone.
            TimestampType elementType = new TimestampType(TimeUnit.Microsecond, "UTC");
            using ListArray.Builder builder = new ListArray.Builder(elementType);
            builder.Append();
            ((TimestampArray.Builder)builder.ValueBuilder).Append(DateTimeOffset.FromUnixTimeMilliseconds(1));

            using ListArray array = builder.Build();

            TimestampType actual = Assert.IsType<TimestampType>(array.Values.Data.DataType);
            Assert.Equal(TimeUnit.Microsecond, actual.Unit);
            Assert.Equal("UTC", actual.Timezone);
        }

        [Fact]
        public void RecordBatchColumnHelpersUseTheBatchAllocator()
        {
            Run(allocator =>
            {
                using RecordBatch batch = new RecordBatch.Builder(allocator)
                    .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                    .Append("strings", true, col => col.String(b => b.Append("x").AppendNull()))
                    .Append("times", true, col => col.Timestamp(b => b.Append(DateTimeOffset.FromUnixTimeMilliseconds(5))))
                    .Build();

                allocator.AssertOwns(batch);
                Assert.Equal(2, batch.Length);
            });
        }

        [Fact]
        public void ConcatenationUsesItsAllocator()
        {
            Run(allocator =>
            {
                using Int32Array.Builder firstBuilder = new Int32Array.Builder(0, allocator);
                using Int32Array.Builder secondBuilder = new Int32Array.Builder(0, allocator);
                using Int32Array first = firstBuilder.Append(1).AppendNull().Build();
                using Int32Array second = secondBuilder.Append(3).Build();

                using IArrowArray result = ArrowArrayConcatenator.Concatenate(new IArrowArray[] { first, second }, allocator);
                allocator.AssertOwns(result);
                Assert.Equal(3, result.Length);
            });
        }

        [Fact]
        public void CloneUsesItsAllocator()
        {
            Run(allocator =>
            {
                using ArrowBuffer source = new ArrowBuffer(new byte[] { 1, 2, 3 });
                using ArrowBuffer clone = source.Clone(allocator);
                allocator.AssertOwns(clone);
                Assert.Equal(3, clone.Span[2]);
            });
        }

        [Fact]
        public void RecordBatchCloneUsesItsAllocator()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).AppendNull()))
                .Append("strings", true, col => col.String(b => b.Append("x")))
                .Build();

            Run(allocator =>
            {
                using RecordBatch clone = original.Clone(allocator);
                allocator.AssertOwns(clone);
            });
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void IpcContextsControlReadAndWriteAllocations(bool fileFormat, bool compress)
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2).Append(3)))
                .Append("strings", true, col => col.String(b => b.Append("x").AppendNull().Append("yz")))
                .Build();

            IpcOptions options = compress ? new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame } : null;
            MemoryStream stream = new MemoryStream();

            // The codec factory comes from the context, not the options: the writer falls back to it.
            Run(writeAllocator =>
            {
                ArrowContext context = new ArrowContext(writeAllocator, new PassThroughCodecFactory());
                using ArrowStreamWriter writer = fileFormat
                    ? new ArrowFileWriter(context, stream, original.Schema, leaveOpen: true, options)
                    : new ArrowStreamWriter(context, stream, original.Schema, leaveOpen: true, options);
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }, expectAllocations: compress);

            Run(readAllocator =>
            {
                ArrowContext context = new ArrowContext(readAllocator, new PassThroughCodecFactory());

                // Wrapping the MemoryStream stops the reader from slicing its buffer directly,
                // so that message bodies are read into memory from the context's allocator.
                stream.Position = 0;
                using Stream wrapped = new BufferedStream(stream);
                using ArrowStreamReader reader = fileFormat
                    ? new ArrowFileReader(context, wrapped, leaveOpen: true)
                    : new ArrowStreamReader(context, wrapped, leaveOpen: true);
                using RecordBatch batch = reader.ReadNextRecordBatch();

                readAllocator.AssertOwns(batch);
                Assert.Equal(3, batch.Length);
                Assert.Equal("yz", ((StringArray)batch.Column(1)).GetString(2));
            });
        }

        [Fact]
        public void InMemoryReaderUsesContextAllocatorForDecompression()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                .Build();

            MemoryStream stream = new MemoryStream();
            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = new PassThroughCodecFactory(),
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, original.Schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            Run(allocator =>
            {
                ArrowContext context = new ArrowContext(allocator, new PassThroughCodecFactory());
                using ArrowStreamReader reader = new ArrowStreamReader(context, new ReadOnlyMemory<byte>(stream.ToArray()));
                using RecordBatch batch = reader.ReadNextRecordBatch();

                allocator.AssertOwns(batch);
                Assert.Equal(2, ((Int32Array)batch.Column(0)).GetValue(1));
            });
        }

        [Fact]
        public void FailedDecompressionReleasesItsOutputBuffer()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                .Build();

            MemoryStream stream = new MemoryStream();
            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = new PassThroughCodecFactory(),
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, original.Schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            // The in-memory reader allocates nothing but decompression output, so anything left
            // outstanding after the failure is a leaked output buffer.
            Run(allocator =>
            {
                ArrowContext context = new ArrowContext(allocator, new FailingCodecFactory());
                using ArrowStreamReader reader = new ArrowStreamReader(context, new ReadOnlyMemory<byte>(stream.ToArray()));

                Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
            });
        }

        [Fact]
        public void CodecFactoryInOptionsTakesPrecedenceOverContext()
        {
            PassThroughCodecFactory fromOptions = new PassThroughCodecFactory();
            PassThroughCodecFactory fromContext = new PassThroughCodecFactory();
            using RecordBatch batch = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1)))
                .Build();

            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = fromOptions,
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(
                new ArrowContext(compressionCodecFactory: fromContext), new MemoryStream(), batch.Schema, options: options))
            {
                writer.WriteRecordBatch(batch);
            }

            Assert.True(fromOptions.CodecsCreated > 0);
            Assert.Equal(0, fromContext.CodecsCreated);
        }

        [Fact]
        public void WriterWithoutAnyCodecFactoryStillRejectsCompression()
        {
            Schema schema = new Schema.Builder().Field(f => f.Name("x").DataType(Int32Type.Default)).Build();
            IpcOptions options = new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame };

            Assert.Throws<ArgumentException>(() => new ArrowStreamWriter(new ArrowContext(), new MemoryStream(), schema, options: options));
        }

        [Fact]
        public void WriterContextConstructorsRejectNullContext()
        {
            Schema schema = new Schema.Builder().Field(f => f.Name("x").DataType(Int32Type.Default)).Build();

            Assert.Throws<ArgumentNullException>(() => new ArrowStreamWriter((ArrowContext)null, new MemoryStream(), schema));
            Assert.Throws<ArgumentNullException>(() => new ArrowFileWriter((ArrowContext)null, new MemoryStream(), schema));
        }

        /// <summary>
        /// Runs <paramref name="test"/> with a fresh caller-owned allocator, then asserts that it
        /// was used and that every allocation it handed out has been released.
        /// </summary>
        private static void Run(Action<TrackingAllocator> test, bool expectAllocations = true)
        {
            TrackingAllocator allocator = new TrackingAllocator();
            test(allocator);

            if (expectAllocations)
            {
                Assert.True(allocator.Statistics.Allocations > 0, "The test should have allocated.");
            }
            Assert.Equal(0, allocator.LiveAllocations);
        }

        /// <summary>
        /// A caller-owned allocator that records the arrays it hands out and counts those not yet
        /// released. Its memory is managed, so a leak fails the test rather than the process.
        /// </summary>
        private sealed class TrackingAllocator : MemoryAllocator
        {
            private readonly HashSet<byte[]> _handedOut = new HashSet<byte[]>(ReferenceComparer.Instance);
            private int _live;

            public int LiveAllocations => Volatile.Read(ref _live);

            public override AllocationLifetime Lifetime => AllocationLifetime.CallerOwned;

            protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                byte[] buffer = new byte[length];
                lock (_handedOut)
                {
                    _handedOut.Add(buffer);
                }
                Interlocked.Increment(ref _live);
                bytesAllocated = length;
                return new Owner(this, buffer);
            }

            public void AssertOwns(RecordBatch batch)
            {
                for (int i = 0; i < batch.ColumnCount; i++)
                {
                    AssertOwns(batch.Column(i));
                }
            }

            public void AssertOwns(IArrowArray array) => AssertOwns(array.Data);

            public void AssertOwns(ArrayData data)
            {
                foreach (ArrowBuffer buffer in data.Buffers)
                {
                    AssertOwns(buffer);
                }
                foreach (ArrayData child in data.Children ?? System.Array.Empty<ArrayData>())
                {
                    AssertOwns(child);
                }
            }

            public void AssertOwns(ArrowBuffer buffer)
            {
                if (buffer.IsEmpty)
                {
                    return;
                }

                Assert.True(MemoryMarshal.TryGetArray(buffer.Memory, out ArraySegment<byte> segment),
                    "Buffer memory is not from this allocator (it is not a managed array).");
                lock (_handedOut)
                {
                    Assert.True(_handedOut.Contains(segment.Array), "Buffer memory is not from this allocator.");
                }
            }

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly TrackingAllocator _allocator;
                private byte[] _buffer;

                public Owner(TrackingAllocator allocator, byte[] buffer)
                {
                    _allocator = allocator;
                    _buffer = buffer;
                }

                public Memory<byte> Memory =>
                    _buffer ?? throw new ObjectDisposedException(nameof(Owner));

                public void Dispose()
                {
                    if (Interlocked.Exchange(ref _buffer, null) != null)
                    {
                        Interlocked.Decrement(ref _allocator._live);
                    }
                }
            }
        }

        private sealed class ReferenceComparer : IEqualityComparer<byte[]>
        {
            public static readonly ReferenceComparer Instance = new ReferenceComparer();

            public bool Equals(byte[] x, byte[] y) => ReferenceEquals(x, y);

            public int GetHashCode(byte[] obj) => RuntimeHelpers.GetHashCode(obj);
        }

        /// <summary>
        /// A "compression" codec that copies its input unchanged, so that tests can exercise the
        /// compressed IPC code paths without a dependency on a real codec.
        /// </summary>
        private sealed class PassThroughCodecFactory : ICompressionCodecFactory
        {
            private int _codecsCreated;

            public int CodecsCreated => Volatile.Read(ref _codecsCreated);

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType)
            {
                Interlocked.Increment(ref _codecsCreated);
                return new PassThroughCodec();
            }

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);
        }

        private sealed class FailingCodecFactory : ICompressionCodecFactory
        {
            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType) => new FailingCodec();

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);
        }

        /// <summary>
        /// A codec whose decompression fails on any data, as it would on corrupt input. Empty
        /// buffers still succeed, so that the failure happens after an output buffer is allocated.
        /// </summary>
        private sealed class FailingCodec : ICompressionCodec
        {
            public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination) =>
                source.IsEmpty ? 0 : throw new InvalidDataException("Corrupt compressed data.");

            public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                throw new NotSupportedException();

            public void Dispose()
            {
            }
        }

        private sealed class PassThroughCodec : ITryCompressionCodec
        {
            public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
            {
                source.CopyTo(destination);
                return source.Length;
            }

            public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                destination.Write(source.ToArray(), 0, source.Length);

            public bool TryCompress(ReadOnlyMemory<byte> source, Memory<byte> destination, out int bytesWritten)
            {
                if (source.Length > destination.Length)
                {
                    bytesWritten = 0;
                    return false;
                }

                source.CopyTo(destination);
                bytesWritten = source.Length;
                return true;
            }

            public void Dispose()
            {
            }
        }
    }
}
