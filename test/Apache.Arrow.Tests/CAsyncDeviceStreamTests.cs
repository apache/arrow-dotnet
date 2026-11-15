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
using System.Collections.Generic;
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
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public class CAsyncDeviceStreamTests
    {
        private RecordBatch CreateBatch(int startValue, int count)
        {
            var builder = new Int32Array.Builder();
            for (int i = 0; i < count; i++)
            {
                builder.Append(startValue + i);
            }

            return new RecordBatch.Builder()
                .Append("values", false, col => col.Int32(b =>
                {
                    for (int i = 0; i < count; i++)
                    {
                        b.Append(startValue + i);
                    }
                }))
                .Build();
        }

        [Fact]
        public async Task RoundTripSingleBatch()
        {
            RecordBatch batch = CreateBatch(1, 5);
            IArrowArrayStream source = new TestArrayStream(batch.Schema, batch);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                RecordBatch result = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(result);
                Assert.Equal(5, result.Length);

                Int32Array values = (Int32Array)result.Column(0);
                Assert.Equal(1, values.GetValue(0));
                Assert.Equal(5, values.GetValue(4));

                result.Dispose();

                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);
            }
        }

        [Fact]
        public async Task RoundTripMultipleBatches()
        {
            RecordBatch batch1 = CreateBatch(0, 3);
            RecordBatch batch2 = CreateBatch(10, 3);
            RecordBatch batch3 = CreateBatch(20, 3);
            IArrowArrayStream source = new TestArrayStream(batch1.Schema, batch1, batch2, batch3);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                RecordBatch r1 = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(r1);
                Assert.Equal(0, ((Int32Array)r1.Column(0)).GetValue(0));
                r1.Dispose();

                RecordBatch r2 = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(r2);
                Assert.Equal(10, ((Int32Array)r2.Column(0)).GetValue(0));
                r2.Dispose();

                RecordBatch r3 = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(r3);
                Assert.Equal(20, ((Int32Array)r3.Column(0)).GetValue(0));
                r3.Dispose();

                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);
            }
        }

        [Fact]
        public async Task EmptyStream()
        {
            Schema schema = new Schema.Builder()
                .Field(f => f.Name("x").DataType(Int32Type.Default))
                .Build();
            IArrowArrayStream source = new TestArrayStream(schema);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                Assert.Equal("x", received.Schema.FieldsList[0].Name);

                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);
            }
        }

        [Fact]
        public async Task SchemaIsPreserved()
        {
            Schema schema = new Schema.Builder()
                .Field(f => f.Name("col_a").DataType(Int64Type.Default).Nullable(true))
                .Field(f => f.Name("col_b").DataType(StringType.Default).Nullable(false))
                .Build();
            IArrowArrayStream source = new TestArrayStream(schema);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                // Wait for end of stream so schema is definitely available
                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);

                Assert.Equal(2, received.Schema.FieldsList.Count);
                Assert.Equal("col_a", received.Schema.FieldsList[0].Name);
                Assert.Equal(ArrowTypeId.Int64, received.Schema.FieldsList[0].DataType.TypeId);
                Assert.Equal("col_b", received.Schema.FieldsList[1].Name);
                Assert.Equal(ArrowTypeId.String, received.Schema.FieldsList[1].DataType.TypeId);
            }
        }

        [Fact]
        public unsafe void InitializeStructsZeroed()
        {
            CArrowAsyncTask task = default;
            Assert.True(task.extract_data == default);
            Assert.True(task.private_data == null);

            CArrowAsyncProducer* producer = CArrowAsyncProducer.Create();
            Assert.Equal((ArrowDeviceType)0, producer->device_type);
            Assert.True(producer->request == default);
            Assert.True(producer->cancel == default);
            Assert.True(producer->additional_metadata == null);
            Assert.True(producer->private_data == null);
            CArrowAsyncProducer.Free(producer);

            CArrowAsyncDeviceStreamHandler* handler = CArrowAsyncDeviceStreamHandler.Create();
            Assert.True(handler->on_schema == default);
            Assert.True(handler->on_next_task == default);
            Assert.True(handler->on_error == default);
            Assert.True(handler->release == default);
            Assert.True(handler->producer == null);
            Assert.True(handler->private_data == null);
            CArrowAsyncDeviceStreamHandler.Free(handler);
        }

        [Fact]
        public async Task ProducerSetsDeviceTypeCpu()
        {
            RecordBatch batch = CreateBatch(1, 1);
            IArrowArrayStream source = new TestArrayStream(batch.Schema, batch);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                RecordBatch r = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(r);
                // If we got data, the producer was using CPU device type
                // (verified by the fact that CArrowDeviceArrayImporter accepted it)
                r.Dispose();

                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);
            }
        }

        [Fact]
        public async Task CancelledReadDoesNotLoseBatch()
        {
            RecordBatch batch = CreateBatch(42, 2);
            var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var source = new GatedArrayStream(batch.Schema, gate.Task, batch);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                // Start a read while the producer is blocked, then cancel it.
                using (var cts = new CancellationTokenSource())
                {
                    Task<RecordBatch> pending = received.ReadNextRecordBatchAsync(cts.Token).AsTask();
                    cts.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
                }

                // Let the producer deliver; the batch must go to the next read,
                // not to the cancelled one.
                gate.SetResult(true);

                RecordBatch result = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(result);
                Assert.Equal(42, ((Int32Array)result.Column(0)).GetValue(0));
                result.Dispose();

                RecordBatch end = await received.ReadNextRecordBatchAsync();
                Assert.Null(end);
            }
        }

        [Fact]
        public async Task SchemaExportFailureSignalsErrorAndReleases()
        {
            var source = new ThrowingSchemaArrayStream("schema export failed");

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                Exception readError = await Assert.ThrowsAnyAsync<Exception>(
                    () => received.ReadNextRecordBatchAsync().AsTask());
                Assert.Contains("schema export failed", readError.Message);

                Exception schemaError = Assert.ThrowsAny<Exception>(() => received.Schema);
                Assert.Contains("schema export failed", schemaError.Message);

                // The producer finished, so it must have disposed its source.
                Assert.True(source.IsDisposed);
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(3)]
        [InlineData(64)]
        public async Task ManyBatchesArriveInOrder(long queueSize)
        {
            // Small queue sizes make the consumer call request() repeatedly while the
            // producer is running, which used to start overlapping produce loops.
            const int count = 200;
            var batches = new RecordBatch[count];
            for (int i = 0; i < count; i++)
            {
                batches[i] = CreateBatch(i * 10, 1 + i % 7);
            }
            var source = new YieldingArrayStream(batches[0].Schema, batches);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler, queueSize);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            using (received)
            {
                for (int i = 0; i < count; i++)
                {
                    RecordBatch result = await received.ReadNextRecordBatchAsync();
                    Assert.NotNull(result);
                    Assert.Equal(batches[i].Length, result.Length);
                    Assert.Equal(i * 10, ((Int32Array)result.Column(0)).GetValue(0));
                    result.Dispose();
                }

                Assert.Null(await received.ReadNextRecordBatchAsync());
            }

            // The producer must never read its source from two loops at once.
            Assert.Equal(1, source.MaxConcurrentReads);
        }

        [Fact]
        public async Task DisposingConsumerCancelsAndReleases()
        {
            RecordBatch batch = CreateBatch(0, 4);
            var source = new EndlessArrayStream(batch);

            IArrowArrayStream received;
            unsafe
            {
                received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler, queueSize: 2);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
            }

            for (int i = 0; i < 5; i++)
            {
                RecordBatch result = await received.ReadNextRecordBatchAsync();
                Assert.NotNull(result);
                result.Dispose();
            }

            received.Dispose();

            // Cancelling must lead to release (and no on_error). The producer
            // disposes its source right after calling release.
            Assert.True(await WaitUntilAsync(() => source.IsDisposed), "Producer did not release after cancel.");
        }

        [Fact]
        public async Task DisposingConsumerBeforeReadingReleases()
        {
            var source = new EndlessArrayStream(CreateBatch(0, 1));

            unsafe
            {
                IArrowArrayStream received = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
                CArrowAsyncDeviceStreamProducer.Produce(source, handler);
                received.Dispose();
            }

            Assert.True(await WaitUntilAsync(() => source.IsDisposed), "Producer did not release after cancel.");
        }

        [Theory]
        [InlineData(0L)]
        [InlineData(-1L)]
        [InlineData(long.MinValue)]
        public async Task InvalidRequestReportsErrorAndReleases(long n)
        {
            var source = new TestArrayStream(CreateBatch(0, 3).Schema, CreateBatch(0, 3), CreateBatch(3, 3));
            var consumer = new RecordingConsumer(requestInOnSchema: n);

            unsafe
            {
                CArrowAsyncDeviceStreamProducer.Produce(source, consumer.Handler);
            }

            Task released = consumer.Released;
            Assert.True(await Task.WhenAny(released, Task.Delay(TimeSpan.FromSeconds(10))) == released, "Handler was not released.");

            // The spec says n <= 0 is invalid and the producer should call on_error;
            // after on_error comes release, and nothing is delivered.
            IReadOnlyList<string> events = consumer.Events;
            Assert.Equal(3, events.Count);
            Assert.Equal("schema", events[0]);
            Assert.StartsWith("error:22:", events[1]); // EINVAL
            Assert.Contains("must be positive", events[1]);
            Assert.Equal("release", events[2]);
            Assert.False(consumer.SawOverlappingCallbacks);
            Assert.True(source.IsDisposed);
        }

        /// <summary>
        /// A consumer written directly against the C callbacks, so tests can drive
        /// the producer in ways <see cref="CArrowAsyncDeviceStreamConsumer"/> won't.
        /// It records each callback it receives as a string.
        /// </summary>
        private sealed unsafe class RecordingConsumer
        {
            private readonly long _requestInOnSchema;
            private readonly List<string> _events = new List<string>();
            private readonly TaskCompletionSource<bool> _released =
                new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            private int _inCallback;
            private Schema _schema;

            public RecordingConsumer(long requestInOnSchema)
            {
                _requestInOnSchema = requestInOnSchema;

                Handler = CArrowAsyncDeviceStreamHandler.Create();
#if NET5_0_OR_GREATER
                Handler->on_schema = &OnSchema;
                Handler->on_next_task = &OnNextTask;
                Handler->on_error = &OnError;
                Handler->release = &Release;
#else
                Handler->on_schema = Marshal.GetFunctionPointerForDelegate(s_onSchema);
                Handler->on_next_task = Marshal.GetFunctionPointerForDelegate(s_onNextTask);
                Handler->on_error = Marshal.GetFunctionPointerForDelegate(s_onError);
                Handler->release = Marshal.GetFunctionPointerForDelegate(s_release);
#endif
                Handler->private_data = (void*)GCHandle.ToIntPtr(GCHandle.Alloc(this));
            }

            public CArrowAsyncDeviceStreamHandler* Handler { get; }

            public bool SawOverlappingCallbacks { get; private set; }

            public IReadOnlyList<string> Events
            {
                get
                {
                    lock (_events)
                    {
                        return _events.ToArray();
                    }
                }
            }

            /// <summary>Completes when the producer calls release.</summary>
            public Task Released => _released.Task;

            private void Record(string evt)
            {
                lock (_events)
                {
                    _events.Add(evt);
                }
            }

            private static RecordingConsumer Enter(CArrowAsyncDeviceStreamHandler* handler)
            {
                var self = (RecordingConsumer)GCHandle.FromIntPtr((IntPtr)handler->private_data).Target;
                if (Interlocked.Increment(ref self._inCallback) != 1)
                {
                    self.SawOverlappingCallbacks = true;
                }
                return self;
            }

            private void Exit()
            {
                Interlocked.Decrement(ref _inCallback);
            }

#if NET5_0_OR_GREATER
            [UnmanagedCallersOnly]
#endif
            private static int OnSchema(CArrowAsyncDeviceStreamHandler* handler, CArrowSchema* schema)
            {
                RecordingConsumer self = Enter(handler);
                try
                {
                    // Takes ownership of (and releases) the C schema.
                    self._schema = CArrowSchemaImporter.ImportSchema(schema);
                    self.Record("schema");

                    // Calling request() synchronously from on_schema is allowed.
                    CArrowAsyncProducer* producer = handler->producer;
#if NET5_0_OR_GREATER
                    producer->request(producer, self._requestInOnSchema);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.RequestDelegate>(producer->request)(producer, self._requestInOnSchema);
#endif
                    return 0;
                }
                catch (Exception)
                {
                    return 131;
                }
                finally
                {
                    self.Exit();
                }
            }

#if NET5_0_OR_GREATER
            [UnmanagedCallersOnly]
#endif
            private static int OnNextTask(CArrowAsyncDeviceStreamHandler* handler, CArrowAsyncTask* task, byte* metadata)
            {
                RecordingConsumer self = Enter(handler);
                try
                {
                    if (task == null)
                    {
                        self.Record("end");
                        return 0;
                    }

                    self.Record("task");
                    CArrowDeviceArray array = default;
#if NET5_0_OR_GREATER
                    int errno = task->extract_data(task, &array);
#else
                    int errno = NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.ExtractDataDelegate>(task->extract_data)(task, &array);
#endif
                    if (errno == 0 && array.array.release != default)
                    {
                        // Importing takes ownership of the array; disposing frees it.
                        CArrowDeviceArrayImporter.ImportRecordBatch(&array, self._schema).Dispose();
                    }
                    return 0;
                }
                catch (Exception)
                {
                    return 131;
                }
                finally
                {
                    self.Exit();
                }
            }

#if NET5_0_OR_GREATER
            [UnmanagedCallersOnly]
#endif
            private static void OnError(CArrowAsyncDeviceStreamHandler* handler, int code, byte* message, byte* metadata)
            {
                RecordingConsumer self = Enter(handler);
                try
                {
                    string text = message == null ? "" : Marshal.PtrToStringAnsi((IntPtr)message);
                    self.Record($"error:{code}:{text}");
                }
                finally
                {
                    self.Exit();
                }
            }

#if NET5_0_OR_GREATER
            [UnmanagedCallersOnly]
#endif
            private static void Release(CArrowAsyncDeviceStreamHandler* handler)
            {
                RecordingConsumer self = Enter(handler);
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)handler->private_data);
                try
                {
                    self.Record("release");
                }
                finally
                {
                    self.Exit();
                    gch.Free();
                    CArrowAsyncDeviceStreamHandler.Free(handler);
                    self._released.TrySetResult(true);
                }
            }

#if !NET5_0_OR_GREATER
            // Deliberately not the library's own delegate types: on .NET Framework the
            // producer must cope with callbacks exported through any delegate type (#491).
            private delegate int OnSchemaDelegate(CArrowAsyncDeviceStreamHandler* handler, CArrowSchema* schema);
            private delegate int OnNextTaskDelegate(CArrowAsyncDeviceStreamHandler* handler, CArrowAsyncTask* task, byte* metadata);
            private delegate void OnErrorDelegate(CArrowAsyncDeviceStreamHandler* handler, int code, byte* message, byte* metadata);
            private delegate void ReleaseDelegate(CArrowAsyncDeviceStreamHandler* handler);

            // Kept in static fields so the delegates outlive the function pointers.
            private static readonly OnSchemaDelegate s_onSchema = OnSchema;
            private static readonly OnNextTaskDelegate s_onNextTask = OnNextTask;
            private static readonly OnErrorDelegate s_onError = OnError;
            private static readonly ReleaseDelegate s_release = Release;
#endif
        }

        private static async Task<bool> WaitUntilAsync(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
            while (DateTime.UtcNow < deadline)
            {
                if (condition())
                {
                    return true;
                }
                await Task.Delay(5);
            }
            return condition();
        }

        /// <summary>
        /// Stream that yields before each batch, giving overlapping readers a chance
        /// to interleave, and records the most reads ever in progress at once.
        /// </summary>
        private sealed class YieldingArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch[] _batches;
            private int _next;
            private int _active;
            private int _maxActive;

            public YieldingArrayStream(Schema schema, RecordBatch[] batches)
            {
                Schema = schema;
                _batches = batches;
            }

            public Schema Schema { get; }

            public int MaxConcurrentReads => Volatile.Read(ref _maxActive);

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                int active = Interlocked.Increment(ref _active);
                int max;
                while (active > (max = Volatile.Read(ref _maxActive)) &&
                       Interlocked.CompareExchange(ref _maxActive, active, max) != max)
                {
                }

                try
                {
                    await Task.Yield();
                    return _next < _batches.Length ? _batches[_next++] : null;
                }
                finally
                {
                    Interlocked.Decrement(ref _active);
                }
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Stream that never ends, so only cancellation can stop the producer.
        /// </summary>
        private sealed class EndlessArrayStream : IArrowArrayStream
        {
            private readonly RecordBatch _batch;
            private int _disposed;

            public EndlessArrayStream(RecordBatch batch)
            {
                _batch = batch;
            }

            public Schema Schema => _batch.Schema;

            public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                await Task.Yield();
                // The producer takes ownership of (and disposes) each batch it's given.
                return _batch.Clone();
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _disposed, 1);
            }
        }

        /// <summary>
        /// Stream whose first read waits for a gate before returning its batches.
        /// </summary>
        private sealed class GatedArrayStream : IArrowArrayStream
        {
            private readonly Task _gate;
            private readonly Queue<RecordBatch> _batches;

            public GatedArrayStream(Schema schema, Task gate, params RecordBatch[] batches)
            {
                Schema = schema;
                _gate = gate;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public async ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                await _gate.ConfigureAwait(false);
                return _batches.Count > 0 ? _batches.Dequeue() : null;
            }

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Stream whose schema can't be obtained, standing in for a schema the
        /// exporter rejects.
        /// </summary>
        private sealed class ThrowingSchemaArrayStream : IArrowArrayStream
        {
            private readonly string _message;
            private int _disposed;

            public ThrowingSchemaArrayStream(string message)
            {
                _message = message;
            }

            public Schema Schema => throw new NotSupportedException(_message);

            public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                throw new InvalidOperationException("Should not be read");
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _disposed, 1);
            }
        }

        /// <summary>
        /// Simple IArrowArrayStream implementation for testing.
        /// </summary>
        private class TestArrayStream : IArrowArrayStream
        {
            private readonly Queue<RecordBatch> _batches;
            private int _disposed;

            public TestArrayStream(Schema schema, params RecordBatch[] batches)
            {
                Schema = schema;
                _batches = new Queue<RecordBatch>(batches);
            }

            public Schema Schema { get; }

            public bool IsDisposed => Volatile.Read(ref _disposed) != 0;

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (Volatile.Read(ref _disposed) != 0)
                {
                    throw new ObjectDisposedException(nameof(TestArrayStream));
                }

                if (_batches.Count > 0)
                {
                    return new ValueTask<RecordBatch>(_batches.Dequeue());
                }
                return new ValueTask<RecordBatch>((RecordBatch)null);
            }

            public void Dispose()
            {
                Interlocked.Exchange(ref _disposed, 1);
            }
        }
    }
}
