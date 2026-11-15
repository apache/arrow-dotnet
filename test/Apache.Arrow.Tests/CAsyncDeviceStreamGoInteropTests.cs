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

#if NET5_0_OR_GREATER

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Interop tests for the C Async Device Stream Interface against arrow-go.
    /// See test/arrow-go-interop for the Go side.
    /// </summary>
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public class CAsyncDeviceStreamGoInteropTests : IClassFixture<ArrowGoInteropFixture>
    {
        // Must match injectedErrorMessage in test/arrow-go-interop/main.go.
        private const string GoInjectedErrorMessage = "arrow-go-interop: injected producer failure";

        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

        private readonly ArrowGoInteropFixture _go;

        public CAsyncDeviceStreamGoInteropTests(ArrowGoInteropFixture go)
        {
            _go = go;
            _go.EnsureAvailable();
        }

        // ---- arrow-go producer -> .NET consumer ----

        [SkippableFact]
        public async Task GoProducerToDotNetConsumer()
        {
            List<RecordBatch> batches = CreateBatches(3);

            await WithDotNetConsumer(async (received, produceWith) =>
            {
                Task produce = produceWith(new BatchListStream(batches), -1);

                await AssertStreamEquals(batches, received);
                await produce.WaitAsync(Timeout);
            });
        }

        [SkippableFact]
        public async Task GoProducerToDotNetConsumerEmptyStream()
        {
            Schema schema = CreateBatches(1)[0].Schema;

            await WithDotNetConsumer(async (received, produceWith) =>
            {
                Task produce = produceWith(new BatchListStream(schema, new List<RecordBatch>()), -1);

                Assert.Null(await received.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout));
                SchemaComparer.Compare(schema, received.Schema);
                await produce.WaitAsync(Timeout);
            });
        }

        [SkippableFact]
        public async Task GoProducerErrorReachesDotNetConsumer()
        {
            List<RecordBatch> batches = CreateBatches(3);

            await WithDotNetConsumer(async (received, produceWith) =>
            {
                Task produce = produceWith(new BatchListStream(batches), 1);

                RecordBatch first = await received.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout);
                Assert.NotNull(first);
                ArrowReaderVerifier.CompareBatches(batches[0], first, strictCompare: false);
                first.Dispose();

                Exception readError = await Assert.ThrowsAnyAsync<Exception>(
                    () => received.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout));
                Assert.Contains(GoInjectedErrorMessage, readError.Message);

                // arrow-go's producer reports the same error to its caller.
                ArrowGoInteropException produceError = await Assert.ThrowsAsync<ArrowGoInteropException>(
                    () => produce.WaitAsync(Timeout));
                Assert.Contains(GoInjectedErrorMessage, produceError.Message);
            });
        }

        [SkippableFact]
        public async Task DotNetConsumerDisposeCancelsGoProducer()
        {
            RecordBatch batch = CreateBatches(1)[0];

            await WithDotNetConsumer(async (received, produceWith) =>
            {
                Task produce = produceWith(new EndlessStream(batch), -1);

                for (int i = 0; i < 5; i++)
                {
                    RecordBatch result = await received.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout);
                    Assert.NotNull(result);
                    result.Dispose();
                }

                // Disposing calls the producer's cancel; arrow-go must then stop,
                // release the handler, and return without an error.
                received.Dispose();
                await produce.WaitAsync(Timeout);
            });
        }

        // ---- .NET producer -> arrow-go consumer ----

        // arrow-go requests queueSize batches up front, then one per batch consumed,
        // so small queue sizes exercise the producer's handling of repeated request() calls.
        [SkippableTheory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(16)]
        public async Task DotNetProducerToGoConsumer(int queueSize)
        {
            List<RecordBatch> batches = CreateBatches(8);

            await WithGoConsumer((ulong)queueSize, new BatchListStream(batches), async result =>
            {
                await AssertStreamEquals(batches, result);
            });
        }

        [SkippableFact]
        public async Task GoConsumerCancelStopsDotNetProducer()
        {
            RecordBatch batch = CreateBatches(1)[0];

            await WithGoConsumer(2, new EndlessStream(batch), async (result, consumer) =>
            {
                for (int i = 0; i < 5; i++)
                {
                    RecordBatch received = await result.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout);
                    Assert.NotNull(received);
                    received.Dispose();
                }

                // arrow-go calls the producer's cancel; the .NET producer must then
                // release the handler, which ends arrow-go's stream. Any batches
                // already in flight may still arrive first.
                consumer.Cancel();
                while (await result.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout) is RecordBatch extra)
                {
                    extra.Dispose();
                }
            });
        }

        [SkippableFact]
        public async Task GoConsumerRejectingSchemaReleasesCleanly()
        {
            // Act as a producer by hand, sending a schema arrow-go can't import. After
            // a failed on_schema the spec says the producer calls only release, which
            // must not crash (arrow-go v18.8.0 frees its handler state in on_schema
            // but leaves its release callback installed; apache/arrow-go#1573).
            IntPtr handlerPtr;
            IntPtr producerPtr = IntPtr.Zero;
            ArrowGoInteropFixture.GoConsumer consumer;
            unsafe
            {
                CArrowAsyncDeviceStreamHandler* handler = CArrowAsyncDeviceStreamHandler.Create();
                handlerPtr = (IntPtr)handler;
                consumer = _go.CreateConsumer(handler, 1);
            }

            try
            {
                Task<IArrowArrayStream> resultTask = consumer.ToStreamAsync();

                unsafe
                {
                    var handler = (CArrowAsyncDeviceStreamHandler*)handlerPtr;
                    CArrowAsyncProducer* producer = CArrowAsyncProducer.Create();
                    producerPtr = (IntPtr)producer;
                    producer->device_type = ArrowDeviceType.Cpu;
                    producer->request = &NoOpRequest;
                    producer->cancel = &NoOpCancel;
                    handler->producer = producer;

                    CArrowSchema* schema = CArrowSchema.Create();
                    CArrowSchemaExporter.ExportType(Int32Type.Default, schema);
                    byte* exportedFormat = schema->format;
                    schema->format = StringUtil.ToCStringUtf8("~bogus");
                    Marshal.FreeHGlobal((IntPtr)exportedFormat);

                    int errno = handler->on_schema(handler, schema);
                    Assert.NotEqual(0, errno);

                    // As the spec requires after a failed on_schema: only release.
                    handler->release(handler);
                    CArrowSchema.Free(schema);
                }

                // arrow-go reports the import failure instead of a stream.
                await Assert.ThrowsAsync<ArrowGoInteropException>(() => resultTask.WaitAsync(Timeout));
            }
            finally
            {
                bool released = await WaitForHandlerReleaseAsync(handlerPtr);
                consumer.Dispose();
                if (released)
                {
                    unsafe
                    {
                        CArrowAsyncDeviceStreamHandler.Free((CArrowAsyncDeviceStreamHandler*)handlerPtr);
                        CArrowAsyncProducer.Free((CArrowAsyncProducer*)producerPtr);
                    }
                }
            }
        }

        [UnmanagedCallersOnly]
        private static unsafe void NoOpRequest(CArrowAsyncProducer* producer, long n)
        {
        }

        [UnmanagedCallersOnly]
        private static unsafe void NoOpCancel(CArrowAsyncProducer* producer)
        {
        }

        [SkippableFact]
        public async Task DotNetProducerToGoConsumerEmptyStream()
        {
            Schema schema = CreateBatches(1)[0].Schema;

            await WithGoConsumer(4, new BatchListStream(schema, new List<RecordBatch>()), async result =>
            {
                SchemaComparer.Compare(schema, result.Schema);
                Assert.Null(await result.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout));
            });
        }

        [SkippableFact]
        public async Task DotNetProducerErrorReachesGoConsumer()
        {
            List<RecordBatch> batches = CreateBatches(3);
            const string message = "dotnet source failure";

            await WithGoConsumer(1, new BatchListStream(batches, failAfter: 1, failureMessage: message), async result =>
            {
                RecordBatch first = await result.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout);
                Assert.NotNull(first);
                ArrowReaderVerifier.CompareBatches(batches[0], first, strictCompare: false);
                first.Dispose();

                Exception readError = await Assert.ThrowsAnyAsync<Exception>(
                    () => result.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout));
                Assert.Contains(message, readError.Message);
            });
        }

        // ---- helpers ----

        private delegate Task ProduceWith(IArrowArrayStream source, long failAfter);

        /// <summary>
        /// Runs <paramref name="body"/> with a .NET consumer whose handler arrow-go
        /// produces into. The consumer frees the handler itself when arrow-go
        /// releases it.
        /// </summary>
        private async Task WithDotNetConsumer(Func<IArrowArrayStream, ProduceWith, Task> body)
        {
            IntPtr handlerPtr;
            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                handlerPtr = (IntPtr)handler;
            }

            Task produce = null;
            try
            {
                await body(received, (source, failAfter) =>
                {
                    unsafe
                    {
                        produce = _go.ProduceFromStreamAsync(source, (CArrowAsyncDeviceStreamHandler*)handlerPtr, failAfter);
                    }
                    return produce;
                });
            }
            finally
            {
                received.Dispose();

                // Don't leave arrow-go's producer running into the next test. Tests
                // that care about its outcome have already asserted on it.
                if (produce != null)
                {
                    try
                    {
                        await produce.WaitAsync(Timeout);
                    }
                    catch (Exception)
                    {
                    }
                }
            }
        }

        /// <summary>
        /// Runs <paramref name="body"/> with what arrow-go consumes when the .NET
        /// producer produces <paramref name="source"/> into it.
        /// </summary>
        private Task WithGoConsumer(ulong queueSize, IArrowArrayStream source, Func<IArrowArrayStream, Task> body)
        {
            return WithGoConsumer(queueSize, source, (result, _) => body(result));
        }

        private async Task WithGoConsumer(ulong queueSize, IArrowArrayStream source, Func<IArrowArrayStream, ArrowGoInteropFixture.GoConsumer, Task> body)
        {
            IntPtr handlerPtr;
            ArrowGoInteropFixture.GoConsumer consumer;
            unsafe
            {
                CArrowAsyncDeviceStreamHandler* handler = CArrowAsyncDeviceStreamHandler.Create();
                handlerPtr = (IntPtr)handler;
                consumer = _go.CreateConsumer(handler, queueSize);
            }

            bool released = false;
            try
            {
                // arrow-go's on_schema blocks until someone is waiting for the stream.
                Task<IArrowArrayStream> resultTask = consumer.ToStreamAsync();
                unsafe
                {
                    CArrowAsyncDeviceStreamProducer.Produce(source, (CArrowAsyncDeviceStreamHandler*)handlerPtr);
                }

                using (IArrowArrayStream result = await resultTask.WaitAsync(Timeout))
                {
                    await body(result, consumer);
                }
            }
            finally
            {
                // If release never happens, leak the handler rather than free memory
                // the producer may still use.
                released = await WaitForHandlerReleaseAsync(handlerPtr);
                consumer.Dispose();
                if (released)
                {
                    unsafe
                    {
                        CArrowAsyncDeviceStreamHandler.Free((CArrowAsyncDeviceStreamHandler*)handlerPtr);
                    }
                }
            }

            // Only reached if the body succeeded, so this doesn't mask its failure.
            Assert.True(released, "The .NET producer never released the handler.");
        }

        /// <summary>
        /// The .NET producer releases the handler after it signals end-of-stream,
        /// so the consumer can finish reading before release happens. The shim
        /// clears the release callback as its very last access to the handler.
        /// </summary>
        private static async Task<bool> WaitForHandlerReleaseAsync(IntPtr handlerPtr)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < Timeout)
            {
                if (IsReleased(handlerPtr))
                {
                    return true;
                }
                await Task.Delay(10);
            }
            return false;
        }

        private static unsafe bool IsReleased(IntPtr handlerPtr)
        {
            // Pairs with the shim's release-ordered store, so nothing it wrote to the
            // handler can land after we see the callback cleared and free it.
            var handler = (CArrowAsyncDeviceStreamHandler*)handlerPtr;
            return Volatile.Read(ref *(IntPtr*)&handler->release) == IntPtr.Zero;
        }

        private static async Task AssertStreamEquals(List<RecordBatch> expected, IArrowArrayStream actual)
        {
            for (int i = 0; i < expected.Count; i++)
            {
                RecordBatch batch = await actual.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout);
                Assert.True(batch != null, $"Stream ended after {i} of {expected.Count} batches.");
                using (batch)
                {
                    ArrowReaderVerifier.CompareBatches(expected[i], batch, strictCompare: false);
                }
            }

            Assert.Null(await actual.ReadNextRecordBatchAsync().AsTask().WaitAsync(Timeout));
        }

        /// <summary>
        /// Batches sharing one schema with many column types. Each batch has a
        /// different length so out-of-order delivery is detected.
        /// </summary>
        private static List<RecordBatch> CreateBatches(int count)
        {
            RecordBatch first = TestData.CreateSampleRecordBatch(length: 10);
            var batches = new List<RecordBatch> { first };
            for (int i = 1; i < count; i++)
            {
                batches.Add(TestData.CreateSampleRecordBatch(first.Schema, length: 10 + i));
            }
            return batches;
        }

        /// <summary>
        /// Stream that never ends, so only cancellation can stop the producer.
        /// </summary>
        private sealed class EndlessStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;

            public EndlessStream(RecordBatch batch)
            {
                _batch = batch;
            }

            public Schema Schema => _batch.Schema;

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                // The exporters take ownership of (and dispose) each batch they're given.
                return new ValueTask<RecordBatch>(_batch.Clone());
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Stream over a list of batches that can fail partway through. It hands
        /// out clones, so tests can compare against the originals.
        /// </summary>
        private sealed class BatchListStream : IArrowArrayStream
        {
            private readonly IReadOnlyList<RecordBatch> _batches;
            private readonly int _failAfter;
            private readonly string _failureMessage;
            private int _next;

            public BatchListStream(IReadOnlyList<RecordBatch> batches, int failAfter = -1, string failureMessage = null)
                : this(batches[0].Schema, batches, failAfter, failureMessage)
            {
            }

            public BatchListStream(Schema schema, IReadOnlyList<RecordBatch> batches, int failAfter = -1, string failureMessage = null)
            {
                Schema = schema;
                _batches = batches;
                _failAfter = failAfter;
                _failureMessage = failureMessage;
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_failAfter >= 0 && _next == _failAfter)
                {
                    throw new InvalidOperationException(_failureMessage);
                }

                // The exporters take ownership of (and dispose) each batch they're
                // given, so hand out clones and keep the originals for comparison.
                RecordBatch batch = _next < _batches.Count ? _batches[_next++].Clone() : null;
                return new ValueTask<RecordBatch>(batch);
            }

            public void Dispose()
            {
            }
        }
    }
}

#endif
