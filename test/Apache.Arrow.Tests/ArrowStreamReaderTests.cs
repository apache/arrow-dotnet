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
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ArrowStreamReaderTests
    {
        [Fact]
        public void Ctor_LeaveOpenDefault_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenFalse_StreamClosedOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream, leaveOpen: false).Dispose();
            Assert.Throws<ObjectDisposedException>(() => stream.Position);
        }

        [Fact]
        public void Ctor_LeaveOpenTrue_StreamValidOnDispose()
        {
            var stream = new MemoryStream();
            new ArrowStreamReader(stream, leaveOpen: true).Dispose();
            Assert.Equal(0, stream.Position);
        }

        [Theory]
        [InlineData(true, true, 2)]
        [InlineData(true, false, 1)]
        [InlineData(false, true, 2)]
        [InlineData(false, false, 1)]
        public async Task Ctor_MemoryPool_AllocatesFromPool(bool shouldLeaveOpen, bool createDictionaryArray, int expectedAllocations)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                stream.Position = 0;

                var memoryPool = new TestMemoryAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(stream, memoryPool, shouldLeaveOpen);
                using (RecordBatch readBatch = reader.ReadNextRecordBatch())
                {
                    Assert.Equal(expectedAllocations, memoryPool.Statistics.Allocations);
                    Assert.True(memoryPool.Statistics.BytesAllocated > 0);
                    Assert.Equal(expectedAllocations, memoryPool.Rented);
                }

                reader.Dispose();

                if (!createDictionaryArray)
                {
                    Assert.Equal(0, memoryPool.Rented);
                }

                if (shouldLeaveOpen)
                {
                    Assert.True(stream.Position > 0);
                }
                else
                {
                    Assert.Throws<ObjectDisposedException>(() => stream.Position);
                }
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatch_Memory(bool writeEnd)
        {
            await TestReaderFromMemory((reader, originalBatch) =>
            {
                Assert.NotNull(reader.Schema);

                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, writeEnd);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatchAsync_Memory(bool writeEnd)
        {
            await TestReaderFromMemory(ArrowReaderVerifier.VerifyReaderAsync, writeEnd);
        }

        [Fact]
        public async Task ReadRecordBatch_Memory_ExactLengthSlice()
        {
            await TestReaderFromMemoryExactLength((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            });
        }

        [Fact]
        public async Task ReadRecordBatchAsync_Memory_ExactLengthSlice()
        {
            await TestReaderFromMemoryExactLength(ArrowReaderVerifier.VerifyReaderAsync);
        }

        private static async Task TestReaderFromMemory(
            Func<ArrowStreamReader, RecordBatch, Task> verificationFunc,
            bool writeEnd)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                if (writeEnd)
                {
                    await writer.WriteEndAsync();
                }
                buffer = stream.GetBuffer();
            }

            ArrowStreamReader reader = new ArrowStreamReader(buffer);
            await verificationFunc(reader, originalBatch);
        }

        private static async Task TestReaderFromMemoryExactLength(
            Func<ArrowStreamReader, RecordBatch, Task> verificationFunc)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100);

            ReadOnlyMemory<byte> buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                buffer = stream.GetBuffer().AsMemory(0, checked((int)stream.Length));
            }

            ArrowStreamReader reader = new ArrowStreamReader(buffer);
            await verificationFunc(reader, originalBatch);
        }

        [Fact]
        public void ReadRecordBatch_EmptyStream()
        {
            using (MemoryStream stream = new())
            {
                ArrowStreamReader reader = new(stream);
                RecordBatch readBatch = reader.ReadNextRecordBatch();
                Assert.Null(readBatch);
            }
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ReadRecordBatch_Stream(bool writeEnd, bool createDictionaryArray)
        {
            await TestReaderFromStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, writeEnd, createDictionaryArray);
        }

        [Fact]
        public async Task ReadRecordBatchAsync_EmptyStream()
        {
            using (MemoryStream stream = new())
            {
                ArrowStreamReader reader = new(stream);
                RecordBatch readBatch = await reader.ReadNextRecordBatchAsync();
                Assert.Null(readBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatchAsync_PassesCancellationTokenToSchemaRead()
        {
            using var stream = new RequiresCancelableReadStream();
            using var reader = new ArrowStreamReader(stream);
            using var cancellation = new CancellationTokenSource();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await reader.ReadNextRecordBatchAsync(cancellation.Token));

            Assert.True(stream.SawCancelableToken);
        }

        [Fact]
        public async Task ReadRecordBatchAsync_Stream_DictionaryFixtureWithoutRee()
        {
            using RecordBatch originalBatch = TestData.CreateSampleRecordBatch(
                length: 100,
                columnSetCount: 5,
                excludedTypes: new System.Collections.Generic.HashSet<ArrowTypeId> { ArrowTypeId.RunEndEncoded });

            using var stream = new MemoryStream();
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true))
            {
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
            }

            stream.Position = 0;

            using var reader = new ArrowStreamReader(stream);
            await ArrowReaderVerifier.VerifyReaderAsync(reader, originalBatch);
        }

        [Theory]
        [InlineData(true, true)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(false, false)]
        public async Task ReadRecordBatchAsync_Stream(bool writeEnd, bool createDictionaryArray)
        {
            await TestReaderFromStream(ArrowReaderVerifier.VerifyReaderAsync, writeEnd, createDictionaryArray);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatchAsync_NonPubliclyVisibleMemoryStream(bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                buffer = stream.ToArray();
            }

            using (MemoryStream stream = new MemoryStream(buffer))
            {
                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await ArrowReaderVerifier.VerifyReaderAsync(reader, originalBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatchAsync_NonPubliclyVisibleMemoryStream_UsesExplicitAllocator()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: false);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                buffer = stream.ToArray();
            }

            var allocator = new TestMemoryAllocator();
            using (MemoryStream stream = new MemoryStream(buffer))
            using (var reader = new ArrowStreamReader(stream, allocator))
            {
                using (RecordBatch readBatch = await reader.ReadNextRecordBatchAsync())
                {
                    ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);
                }

                Assert.True(allocator.Statistics.Allocations > 0);
                Assert.Equal(0, allocator.Rented);
                Assert.Null(await reader.ReadNextRecordBatchAsync());
            }
        }

        [Fact]
        public async Task ReadRecordBatchAsync_NonPubliclyVisibleMemoryStream_UsesDefaultAllocator()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: false);

            byte[] buffer;
            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();
                buffer = stream.ToArray();
            }

            long allocationsBeforeRead = MemoryAllocator.Default.Value.Statistics.Allocations;

            using (MemoryStream stream = new MemoryStream(buffer))
            using (var reader = new ArrowStreamReader(stream, MemoryAllocator.Default.Value))
            using (RecordBatch readBatch = await reader.ReadNextRecordBatchAsync())
            {
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);
            }

            Assert.True(MemoryAllocator.Default.Value.Statistics.Allocations > allocationsBeforeRead);
        }

        private static async Task TestReaderFromStream(
            Func<ArrowStreamReader, RecordBatch, Task> verificationFunc,
            bool writeEnd, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                if (writeEnd)
                {
                    await writer.WriteEndAsync();
                }

                stream.Position = 0;

                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatch_ExposedMemoryStream_BatchRemainsUsableAfterDispose()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: false);
            RecordBatch readBatch;

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                stream.Position = 0;

                using (ArrowStreamReader reader = new ArrowStreamReader(stream, leaveOpen: true))
                {
                    readBatch = reader.ReadNextRecordBatch();
                }
            }

            using (readBatch)
            {
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatch_ExposedMemoryStream_BatchDoesNotAliasMutableStreamBuffer()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: false);
            RecordBatch readBatch;
            byte[] streamBuffer;

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                streamBuffer = stream.GetBuffer();
                stream.Position = 0;

                using (ArrowStreamReader reader = new ArrowStreamReader(stream, leaveOpen: true))
                {
                    readBatch = reader.ReadNextRecordBatch();
                }
            }

            System.Array.Clear(streamBuffer, 0, streamBuffer.Length);

            using (readBatch)
            {
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);
            }
        }

        [Fact]
        public async Task ReadRecordBatchAsync_ExposedMemoryStream_BatchDoesNotAliasMutableStreamBuffer()
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: false);
            RecordBatch readBatch;
            byte[] streamBuffer;

            using (MemoryStream stream = new MemoryStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema, leaveOpen: true);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                streamBuffer = stream.GetBuffer();
                stream.Position = 0;

                using (ArrowStreamReader reader = new ArrowStreamReader(stream, leaveOpen: true))
                {
                    readBatch = await reader.ReadNextRecordBatchAsync();
                }
            }

            System.Array.Clear(streamBuffer, 0, streamBuffer.Length);

            using (readBatch)
            {
                ArrowReaderVerifier.CompareBatches(originalBatch, readBatch);
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatch_PartialReadStream(bool createDictionaryArray)
        {
            await TestReaderFromPartialReadStream((reader, originalBatch) =>
            {
                ArrowReaderVerifier.VerifyReader(reader, originalBatch);
                return Task.CompletedTask;
            }, createDictionaryArray);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ReadRecordBatchAsync_PartialReadStream(bool createDictionaryArray)
        {
            await TestReaderFromPartialReadStream(ArrowReaderVerifier.VerifyReaderAsync, createDictionaryArray);
        }

        /// <summary>
        /// Verifies that the stream reader reads multiple times when a stream
        /// only returns a subset of the data from each Read.
        /// </summary>
        private static async Task TestReaderFromPartialReadStream(Func<ArrowStreamReader, RecordBatch, Task> verificationFunc, bool createDictionaryArray)
        {
            RecordBatch originalBatch = TestData.CreateSampleRecordBatch(length: 100, createDictionaryArray: createDictionaryArray);

            using (PartialReadStream stream = new PartialReadStream())
            {
                ArrowStreamWriter writer = new ArrowStreamWriter(stream, originalBatch.Schema);
                await writer.WriteRecordBatchAsync(originalBatch);
                await writer.WriteEndAsync();

                stream.Position = 0;

                ArrowStreamReader reader = new ArrowStreamReader(stream);
                await verificationFunc(reader, originalBatch);
            }
        }

        /// <summary>
        /// A stream class that only returns a part of the data at a time.
        /// </summary>
        private class PartialReadStream : Stream
        {
            private readonly MemoryStream _innerStream = new MemoryStream();

            // by default return 20 bytes at a time
            public int PartialReadLength { get; set; } = 20;

            public override bool CanRead => _innerStream.CanRead;
            public override bool CanSeek => _innerStream.CanSeek;
            public override bool CanWrite => _innerStream.CanWrite;
            public override long Length => _innerStream.Length;
            public override long Position { get => _innerStream.Position; set => _innerStream.Position = value; }

            public override void Flush() => _innerStream.Flush();
            public override long Seek(long offset, SeekOrigin origin) => _innerStream.Seek(offset, origin);
            public override void SetLength(long value) => _innerStream.SetLength(value);
            public override void Write(byte[] buffer, int offset, int count) => _innerStream.Write(buffer, offset, count);

            public override int Read(byte[] buffer, int offset, int count)
            {
                return _innerStream.Read(buffer, offset, Math.Min(count, PartialReadLength));
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken = default)
            {
                return _innerStream.ReadAsync(buffer, offset, Math.Min(count, PartialReadLength), cancellationToken);
            }

#if NET5_0_OR_GREATER
            public override int Read(Span<byte> destination)
            {
                if (destination.Length > PartialReadLength)
                {
                    destination = destination.Slice(0, PartialReadLength);
                }

                return _innerStream.Read(destination);
            }

            public override ValueTask<int> ReadAsync(Memory<byte> destination, CancellationToken cancellationToken = default)
            {
                if (destination.Length > PartialReadLength)
                {
                    destination = destination.Slice(0, PartialReadLength);
                }

                return _innerStream.ReadAsync(destination, cancellationToken);
            }
#endif
        }

        private class RequiresCancelableReadStream : Stream
        {
            public bool SawCancelableToken { get; private set; }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

#if NET5_0_OR_GREATER
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            {
                SawCancelableToken = cancellationToken.CanBeCanceled;
                if (!SawCancelableToken)
                {
                    throw new InvalidOperationException("Expected the caller's cancellation token during schema read.");
                }

                throw new OperationCanceledException(cancellationToken);
            }
#else
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                SawCancelableToken = cancellationToken.CanBeCanceled;
                if (!SawCancelableToken)
                {
                    throw new InvalidOperationException("Expected the caller's cancellation token during schema read.");
                }

                throw new OperationCanceledException(cancellationToken);
            }
#endif
        }

        [Fact]
        public unsafe void MalformedColumnNameLength()
        {
            const int FieldNameLengthOffset = 108;
            const int FakeFieldNameLength = 165535;

            byte[] buffer;
            using (var stream = new MemoryStream())
            {
                Schema schema = new(
                    [new Field("index", Int32Type.Default, nullable: false)],
                    metadata: []);
                using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
                {
                    writer.WriteStart();
                    writer.WriteEnd();
                }
                buffer = stream.ToArray();
            }

            Span<int> length = buffer.AsSpan().Slice(FieldNameLengthOffset, sizeof(int)).CastTo<int>();
            Assert.Equal(5, length[0]);
            length[0] = FakeFieldNameLength;

            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (var stream = new MemoryStream(buffer))
                using (var reader = new ArrowStreamReader(stream))
                {
                    reader.ReadNextRecordBatch();
                }
            });
        }

        [Fact]
        public async Task EmptyStreamNoSyncRead()
        {
            using (var stream = new EmptyAsyncOnlyStream())
            {
                var reader = new ArrowStreamReader(stream);
                var schema = await reader.GetSchema();
                Assert.Null(schema);
            }
        }

        private class EmptyAsyncOnlyStream : Stream
        {
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => 0;
            public override long Position { get => 0; set => throw new NotSupportedException(); }
            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => Task.FromResult(0);
        }
    }
}
