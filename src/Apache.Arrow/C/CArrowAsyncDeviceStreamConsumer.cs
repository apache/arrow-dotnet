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
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    /// <summary>
    /// Consumes data from an async device stream producer by providing an
    /// <see cref="CArrowAsyncDeviceStreamHandler"/>. Returns an <see cref="IArrowArrayStream"/>
    /// that yields the received batches.
    /// </summary>
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public static class CArrowAsyncDeviceStreamConsumer
    {
        /// <summary>
        /// The default number of batches the consumer keeps requested ahead of the reader.
        /// </summary>
        public const long DefaultQueueSize = 16;

        private const int EOTHER = 131;

#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, CArrowSchema*, int> OnSchemaPtr => &OnSchema;
        private static unsafe delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, CArrowAsyncTask*, byte*, int> OnNextTaskPtr => &OnNextTask;
        private static unsafe delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, int, byte*, byte*, void> OnErrorPtr => &OnError;
        private static unsafe delegate* unmanaged<CArrowAsyncDeviceStreamHandler*, void> ReleasePtr => &Release;
#else
        internal unsafe delegate int OnSchemaDelegate(CArrowAsyncDeviceStreamHandler* handler, CArrowSchema* schema);
        private static unsafe readonly NativeDelegate<OnSchemaDelegate> s_onSchema = new NativeDelegate<OnSchemaDelegate>(OnSchema);
        private static unsafe IntPtr OnSchemaPtr => s_onSchema.Pointer;
        internal unsafe delegate int OnNextTaskDelegate(CArrowAsyncDeviceStreamHandler* handler, CArrowAsyncTask* task, byte* metadata);
        private static unsafe readonly NativeDelegate<OnNextTaskDelegate> s_onNextTask = new NativeDelegate<OnNextTaskDelegate>(OnNextTask);
        private static unsafe IntPtr OnNextTaskPtr => s_onNextTask.Pointer;
        internal unsafe delegate void OnErrorDelegate(CArrowAsyncDeviceStreamHandler* handler, int code, byte* message, byte* metadata);
        private static unsafe readonly NativeDelegate<OnErrorDelegate> s_onError = new NativeDelegate<OnErrorDelegate>(OnError);
        private static unsafe IntPtr OnErrorPtr => s_onError.Pointer;
        internal unsafe delegate void ReleaseHandlerDelegate(CArrowAsyncDeviceStreamHandler* handler);
        private static unsafe readonly NativeDelegate<ReleaseHandlerDelegate> s_release = new NativeDelegate<ReleaseHandlerDelegate>(Release);
        private static unsafe IntPtr ReleasePtr => s_release.Pointer;
#endif

        /// <summary>
        /// Create a consumer handler and return an <see cref="IArrowArrayStream"/>
        /// that will yield batches pushed by the producer.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The caller must pass <paramref name="handler"/> to a producer. The producer
        /// populates the handler's producer field and calls on_schema; the consumer
        /// then requests <paramref name="queueSize"/> batches and requests more as
        /// the reader consumes them, so at most about that many batches are buffered.
        /// </para>
        /// <para>
        /// The consumer owns the handler's memory and frees it when the producer
        /// calls the handler's release callback, so the caller must not free it and
        /// must not use the pointer after handing it to the producer. A handler that
        /// is never handed to a producer is never released, and leaks.
        /// </para>
        /// <para>
        /// Disposing the returned stream before the end cancels the producer, which
        /// may release the handler some time later.
        /// </para>
        /// </remarks>
        /// <example>
        /// <code>
        /// IArrowArrayStream stream = CArrowAsyncDeviceStreamConsumer.CreateStream(out CArrowAsyncDeviceStreamHandler* handler);
        /// foreign_produce_function(handler);
        /// RecordBatch batch = await stream.ReadNextRecordBatchAsync();
        /// </code>
        /// </example>
        /// <param name="handler">Receives the newly allocated handler to pass to a producer.</param>
        /// <param name="queueSize">The number of batches to keep requested ahead of the reader.</param>
        /// <returns>An IArrowArrayStream that yields the received batches.</returns>
        public static unsafe IArrowArrayStream CreateStream(out CArrowAsyncDeviceStreamHandler* handler, long queueSize = DefaultQueueSize)
        {
            if (queueSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(queueSize), queueSize, "The queue size must be positive.");
            }

            CArrowAsyncDeviceStreamHandler* newHandler = CArrowAsyncDeviceStreamHandler.Create();
            var state = new ConsumerState(newHandler, queueSize);

            newHandler->on_schema = OnSchemaPtr;
            newHandler->on_next_task = OnNextTaskPtr;
            newHandler->on_error = OnErrorPtr;
            newHandler->release = ReleasePtr;

            GCHandle gch = GCHandle.Alloc(state);
            newHandler->private_data = (void*)GCHandle.ToIntPtr(gch);

            handler = newHandler;
            return state;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int OnSchema(CArrowAsyncDeviceStreamHandler* handler, CArrowSchema* schema)
        {
            ConsumerState state = null;
            try
            {
                state = ConsumerState.FromPointer(handler->private_data);
                Schema importedSchema = CArrowSchemaImporter.ImportSchema(schema);
                state.SetSchema(importedSchema);

                // Calling request() synchronously from on_schema is explicitly allowed.
                state.StartRequesting();
                return 0;
            }
            catch (Exception ex)
            {
                // After a failed on_schema the producer only calls release, so record
                // the failure here or the reader would just see an empty stream.
                state?.SignalError(ex);
                return EOTHER;
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int OnNextTask(CArrowAsyncDeviceStreamHandler* handler, CArrowAsyncTask* task, byte* metadata)
        {
            try
            {
                var state = ConsumerState.FromPointer(handler->private_data);

                if (task == null)
                {
                    // End of stream
                    state.SignalComplete();
                    return 0;
                }

                // The task is only valid during this call, so extract it now. Even an
                // unwanted task must be extracted so the producer can clean up. An
                // output array is always passed, even though the spec allows NULL,
                // because not every producer handles NULL (arrow-go v18.8.0 doesn't).
                CArrowDeviceArray cDeviceArray = new CArrowDeviceArray();
#if NET5_0_OR_GREATER
                int errno = task->extract_data(task, &cDeviceArray);
#else
                int errno = NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.ExtractDataDelegate>(task->extract_data)(task, &cDeviceArray);
#endif
                if (errno != 0)
                {
                    // The producer follows a failed extract with on_error.
                    return 0;
                }

                if (cDeviceArray.array.release == default)
                {
                    return 0;
                }

                if (state.IsDisposed)
                {
                    // Cancelled; drop any batches still in flight.
                    ReleaseArray(&cDeviceArray.array);
                    return 0;
                }

                RecordBatch batch;
                try
                {
                    batch = CArrowDeviceArrayImporter.ImportRecordBatch(&cDeviceArray, state.Schema);
                }
                catch (Exception ex)
                {
                    ReleaseArray(&cDeviceArray.array);
                    state.SignalError(ex);
                    return EOTHER;
                }

                state.EnqueueBatch(batch);
                return 0;
            }
            catch (Exception)
            {
                return EOTHER;
            }
        }

        private unsafe static void ReleaseArray(CArrowArray* array)
        {
            if (array->release != default)
            {
#if NET5_0_OR_GREATER
                array->release(array);
#else
                NativeDelegate.GetDelegateForFunctionPointer<CArrowArrayExporter.ReleaseArrowArray>(array->release)(array);
#endif
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void OnError(CArrowAsyncDeviceStreamHandler* handler, int code, byte* message, byte* metadata)
        {
            try
            {
                var state = ConsumerState.FromPointer(handler->private_data);
                string msg = message != null ? StringUtil.PtrToStringUtf8(message) : $"Async stream error. Code: {code}";
                state.SignalError(new Exception(msg));
            }
            catch (Exception)
            {
                // Cannot propagate errors from on_error
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void Release(CArrowAsyncDeviceStreamHandler* handler)
        {
            try
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)handler->private_data);
                var state = (ConsumerState)gch.Target;

                // After this, the state no longer touches the handler.
                state.SignalReleased();
                gch.Free();
            }
            catch (Exception)
            {
                // Cannot propagate errors from release
            }

            // release is the last call the producer makes on the handler, and the
            // consumer allocated it in CreateStream, so it can be freed here.
            CArrowAsyncDeviceStreamHandler.Free(handler);
        }

        private sealed unsafe class ConsumerState : IArrowArrayStream
        {
            private readonly CArrowAsyncDeviceStreamHandler* _handler;
            private readonly long _replenishThreshold;

            // Guards the batch queue and waiters.
            private readonly object _gate = new object();
            private readonly Queue<RecordBatch> _buffered = new Queue<RecordBatch>();
            private readonly Queue<Waiter> _waiters = new Queue<Waiter>();
            private readonly TaskCompletionSource<Schema> _schemaReady;
            private bool _completed;
            private Exception _error;
            private long _consumedSinceRequest;

            // Guards every use of the handler and producer structs: the handler is
            // freed in release, and the producer may free its struct as soon as
            // release returns. Held while calling request/cancel, so release
            // (which takes it too) can't complete in the middle of one.
            // Separate from _gate so a producer that blocks in request() while
            // delivering a batch can't deadlock against EnqueueBatch.
            private readonly object _producerGate = new object();
            private bool _released;
            private bool _cancelled;
            private bool _cancelSent;

            private volatile bool _disposed;

            public ConsumerState(CArrowAsyncDeviceStreamHandler* handler, long queueSize)
            {
                _handler = handler;
                QueueSize = queueSize;
                // Re-request in chunks rather than one batch at a time, so the
                // producer sees fewer request() calls.
                _replenishThreshold = Math.Max(1, queueSize / 2);
                _schemaReady = new TaskCompletionSource<Schema>(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public long QueueSize { get; }

            public bool IsDisposed => _disposed;

            public Schema Schema
            {
                get
                {
                    // Block until schema is available
                    return _schemaReady.Task.GetAwaiter().GetResult();
                }
            }

            public static ConsumerState FromPointer(void* ptr)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)ptr);
                return (ConsumerState)gch.Target;
            }

            public void SetSchema(Schema schema)
            {
                _schemaReady.TrySetResult(schema);
            }

            /// <summary>
            /// Called from on_schema, once the producer is attached: make the initial
            /// request, or, if the stream was disposed before a producer attached, send
            /// the cancellation that couldn't be sent then. Otherwise the producer would
            /// get neither demand nor cancel and never release the handler.
            /// </summary>
            public void StartRequesting()
            {
                lock (_producerGate)
                {
                    CArrowAsyncProducer* producer = _handler->producer;
                    if (_released || producer == null)
                    {
                        return;
                    }

                    if (_cancelled)
                    {
                        if (!_cancelSent)
                        {
                            _cancelSent = true;
#if NET5_0_OR_GREATER
                            producer->cancel(producer);
#else
                            NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.CancelDelegate>(producer->cancel)(producer);
#endif
                        }
                        return;
                    }

#if NET5_0_OR_GREATER
                    producer->request(producer, QueueSize);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.RequestDelegate>(producer->request)(producer, QueueSize);
#endif
                }
            }

            public void RequestFromProducer(long n)
            {
                lock (_producerGate)
                {
                    if (_released || _cancelled || _handler->producer == null)
                    {
                        return;
                    }

                    CArrowAsyncProducer* producer = _handler->producer;
#if NET5_0_OR_GREATER
                    producer->request(producer, n);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.RequestDelegate>(producer->request)(producer, n);
#endif
                }
            }

            private void CancelProducer()
            {
                lock (_producerGate)
                {
                    if (_released || _cancelled)
                    {
                        return;
                    }
                    _cancelled = true;

                    CArrowAsyncProducer* producer = _handler->producer;
                    if (producer == null)
                    {
                        // No producer attached yet; StartRequesting sends the cancel
                        // once one is.
                        return;
                    }

                    _cancelSent = true;
#if NET5_0_OR_GREATER
                    producer->cancel(producer);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamProducer.CancelDelegate>(producer->cancel)(producer);
#endif
                }
            }

            /// <summary>
            /// Called whenever a batch is handed to the reader; asks the producer for
            /// more once enough of the queue has drained.
            /// </summary>
            private void OnBatchConsumed()
            {
                long toRequest = 0;
                lock (_gate)
                {
                    if (++_consumedSinceRequest >= _replenishThreshold)
                    {
                        toRequest = _consumedSinceRequest;
                        _consumedSinceRequest = 0;
                    }
                }

                if (toRequest > 0)
                {
                    RequestFromProducer(toRequest);
                }
            }

            public void EnqueueBatch(RecordBatch batch)
            {
                Waiter waiter = null;
                bool rejected = false;
                lock (_gate)
                {
                    // Dispose sets _disposed before it drains the queue under this
                    // lock, so a batch either gets drained or is rejected here.
                    if (_disposed)
                    {
                        rejected = true;
                    }
                    else
                    {
                        // Skip waiters whose read was cancelled; handing the batch to
                        // one of them would lose it.
                        while (_waiters.Count > 0)
                        {
                            Waiter candidate = _waiters.Dequeue();
                            if (candidate.Completion.TrySetResult(batch))
                            {
                                waiter = candidate;
                                break;
                            }
                        }

                        if (waiter == null)
                        {
                            _buffered.Enqueue(batch);
                        }
                    }
                }

                if (rejected)
                {
                    batch.Dispose();
                    return;
                }

                if (waiter != null)
                {
                    waiter.Registration.Dispose();
                    // request() may be called synchronously from on_next_task.
                    OnBatchConsumed();
                }
            }

            public void SignalComplete()
            {
                List<Waiter> waiters;
                lock (_gate)
                {
                    _completed = true;
                    waiters = DrainWaiters();
                }
                foreach (Waiter waiter in waiters)
                {
                    waiter.Completion.TrySetResult(null);
                    waiter.Registration.Dispose();
                }
            }

            public void SignalError(Exception ex)
            {
                List<Waiter> waiters;
                lock (_gate)
                {
                    if (_error == null)
                    {
                        _error = ex;
                    }
                    _completed = true;
                    _schemaReady.TrySetException(ex);
                    waiters = DrainWaiters();
                }
                foreach (Waiter waiter in waiters)
                {
                    waiter.Completion.TrySetException(ex);
                    waiter.Registration.Dispose();
                }
            }

            public void SignalReleased()
            {
                lock (_producerGate)
                {
                    // After this, the producer struct may be freed at any time.
                    _released = true;
                }

                // Don't leave Schema blocking forever if the producer released
                // the handler without ever delivering a schema.
                _schemaReady.TrySetException(new InvalidOperationException(
                    "The async stream was released before a schema was received."));
                SignalComplete();
            }

            private List<Waiter> DrainWaiters()
            {
                var waiters = new List<Waiter>(_waiters);
                _waiters.Clear();
                return waiters;
            }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                if (_disposed)
                {
                    throw new ObjectDisposedException(nameof(ConsumerState));
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return new ValueTask<RecordBatch>(Task.FromCanceled<RecordBatch>(cancellationToken));
                }

                RecordBatch buffered = null;
                lock (_gate)
                {
                    if (_buffered.Count > 0)
                    {
                        buffered = _buffered.Dequeue();
                    }
                    else if (_error != null)
                    {
                        return new ValueTask<RecordBatch>(Task.FromException<RecordBatch>(_error));
                    }
                    else if (_completed)
                    {
                        return new ValueTask<RecordBatch>((RecordBatch)null);
                    }
                    else
                    {
                        var waiter = new Waiter();
                        if (cancellationToken.CanBeCanceled)
                        {
                            // The callback doesn't take _gate: a cancelled waiter is
                            // simply skipped when the next batch arrives. This also keeps
                            // Registration.Dispose (which waits for a running callback)
                            // from deadlocking against the lock.
                            waiter.Registration = cancellationToken.Register(
                                () => waiter.Completion.TrySetCanceled(cancellationToken));
                        }
                        _waiters.Enqueue(waiter);
                        return new ValueTask<RecordBatch>(waiter.Completion.Task);
                    }
                }

                OnBatchConsumed();
                return new ValueTask<RecordBatch>(buffered);
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }
                _disposed = true;

                // If the producer is still active, ask it to stop. It may keep
                // delivering for a while; OnNextTask drops those batches.
                CancelProducer();

                List<RecordBatch> buffered;
                List<Waiter> waiters;
                lock (_gate)
                {
                    buffered = new List<RecordBatch>(_buffered);
                    _buffered.Clear();
                    waiters = DrainWaiters();
                }
                foreach (RecordBatch batch in buffered)
                {
                    batch.Dispose();
                }

                // Don't leave a read that's still waiting hanging forever.
                var disposedException = new ObjectDisposedException(nameof(ConsumerState));
                foreach (Waiter waiter in waiters)
                {
                    waiter.Completion.TrySetException(disposedException);
                    waiter.Registration.Dispose();
                }
            }

            private sealed class Waiter
            {
                public readonly TaskCompletionSource<RecordBatch> Completion =
                    new TaskCompletionSource<RecordBatch>(TaskCreationOptions.RunContinuationsAsynchronously);
                public CancellationTokenRegistration Registration;
            }
        }
    }
}
