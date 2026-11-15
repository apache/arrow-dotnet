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
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.C
{
    /// <summary>
    /// Produces data from an <see cref="IArrowArrayStream"/> by pushing it through
    /// an <see cref="CArrowAsyncDeviceStreamHandler"/>.
    /// </summary>
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public static class CArrowAsyncDeviceStreamProducer
    {
        private const int EINVAL = 22;
        private const int EOTHER = 131;

#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowAsyncProducer*, long, void> RequestPtr => &OnRequest;
        private static unsafe delegate* unmanaged<CArrowAsyncProducer*, void> CancelPtr => &OnCancel;
        private static unsafe delegate* unmanaged<CArrowAsyncTask*, CArrowDeviceArray*, int> ExtractDataPtr => &ExtractData;
#else
        internal unsafe delegate void RequestDelegate(CArrowAsyncProducer* producer, long n);
        private static unsafe readonly NativeDelegate<RequestDelegate> s_request = new NativeDelegate<RequestDelegate>(OnRequest);
        private static unsafe IntPtr RequestPtr => s_request.Pointer;
        internal unsafe delegate void CancelDelegate(CArrowAsyncProducer* producer);
        private static unsafe readonly NativeDelegate<CancelDelegate> s_cancel = new NativeDelegate<CancelDelegate>(OnCancel);
        private static unsafe IntPtr CancelPtr => s_cancel.Pointer;
        internal unsafe delegate int ExtractDataDelegate(CArrowAsyncTask* task, CArrowDeviceArray* deviceArray);
        private static unsafe readonly NativeDelegate<ExtractDataDelegate> s_extractData = new NativeDelegate<ExtractDataDelegate>(ExtractData);
        private static unsafe IntPtr ExtractDataPtr => s_extractData.Pointer;
#endif

        /// <summary>
        /// Produce data from an <see cref="IArrowArrayStream"/> by pushing it through
        /// the provided handler. The producer populates the handler's producer field
        /// and calls on_schema before this method returns; batches are then delivered
        /// asynchronously as the consumer requests them.
        /// </summary>
        /// <remarks>
        /// Handler callbacks are never invoked concurrently, and the handler's release
        /// callback is called exactly once, as the last call on the handler. The
        /// source stream is disposed after release. As with the C stream exporters,
        /// the producer takes ownership of each batch the source returns and disposes
        /// it once the consumer has released (or declined) the exported data.
        /// </remarks>
        /// <param name="arrayStream">The source stream to produce from.</param>
        /// <param name="handler">The consumer-allocated handler to push data through.</param>
        public static unsafe void Produce(IArrowArrayStream arrayStream, CArrowAsyncDeviceStreamHandler* handler)
        {
            if (arrayStream == null)
            {
                throw new ArgumentNullException(nameof(arrayStream));
            }
            if (handler == null)
            {
                throw new ArgumentNullException(nameof(handler));
            }

            var state = new ProducerState(arrayStream, handler);
            state.Start();
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void OnRequest(CArrowAsyncProducer* producer, long n)
        {
            // Exceptions must not escape an unmanaged callback. request() has no
            // return value; errors it detects are reported later via on_error.
            try
            {
                var state = ProducerState.FromPointer(producer->private_data);
                state.Request(n);
            }
            catch (Exception)
            {
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static void OnCancel(CArrowAsyncProducer* producer)
        {
            // Exceptions must not escape an unmanaged callback.
            try
            {
                var state = ProducerState.FromPointer(producer->private_data);
                state.Cancel();
            }
            catch (Exception)
            {
            }
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private unsafe static int ExtractData(CArrowAsyncTask* task, CArrowDeviceArray* deviceArray)
        {
            // Consumers may extract after the producer has finished and released
            // the handler (arrow-go does), so this must only depend on the task.
            TaskState taskState = null;
            try
            {
                // extract_data is called exactly once per task, so the task's
                // GCHandle is always freed here. Clearing private_data makes an
                // erroneous second call fail cleanly instead of touching a freed handle.
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)task->private_data);
                task->private_data = null;
                try
                {
                    taskState = (TaskState)gch.Target;
                    if (deviceArray != null)
                    {
                        // Like the C stream exporters, take over the source stream's
                        // reference: the exported array disposes the batch when released
                        // (or right away if the export fails).
                        CArrowDeviceArrayExporter.ExportRecordBatch(taskState.Batch, deviceArray, transferOwnership: true);
                    }
                    else
                    {
                        // The consumer doesn't want the data.
                        taskState.Batch.Dispose();
                    }
                    return 0;
                }
                finally
                {
                    gch.Free();
                }
            }
            catch (Exception ex)
            {
                // The spec says a failed extract_data is followed by on_error.
                taskState?.Producer.ReportError(EOTHER, ex.Message);
                return EOTHER;
            }
        }

        /// <summary>
        /// What the produce loop should do next.
        /// </summary>
        private enum Step
        {
            /// <summary>No demand: the loop exits until the next request().</summary>
            Idle,
            /// <summary>Read a batch from the source and deliver it.</summary>
            Produce,
            /// <summary>Report a pending error, then release.</summary>
            Error,
            /// <summary>Cancelled: release without further callbacks.</summary>
            Release,
        }

        /// <summary>
        /// State for one produced stream.
        /// </summary>
        /// <remarks>
        /// All handler callbacks after on_schema are made from a single produce loop,
        /// and at most one loop runs at a time (<see cref="_running"/>), so callbacks
        /// are never concurrent. request() and cancel() only update state under
        /// <see cref="_gate"/> and start the loop if it isn't running; they never call
        /// back into the handler themselves, as the spec requires.
        /// </remarks>
        private sealed unsafe class ProducerState
        {
            private readonly IArrowArrayStream _arrayStream;
            private readonly CArrowAsyncDeviceStreamHandler* _handler;
            private CArrowAsyncProducer* _producer;

            private readonly object _gate = new object();
            private long _demand;
            private bool _schemaDelivered;
            private bool _running;
            private bool _cancelled;
            // Set once the loop has committed to its final callbacks (end of stream,
            // on_error or release); later request/cancel/errors are ignored, so a
            // decision made under _gate can't be overtaken.
            private bool _completing;
            private bool _finished;
            private bool _hasPendingError;
            private int _pendingErrorCode;
            private string _pendingErrorMessage;

            // Cancelled by cancel() so a source read that's waiting for input wakes
            // up. Never disposed: it has no timer, and a late Cancel() must not throw.
            private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();

            public ProducerState(IArrowArrayStream arrayStream, CArrowAsyncDeviceStreamHandler* handler)
            {
                _arrayStream = arrayStream;
                _handler = handler;
            }

            public IArrowArrayStream ArrayStream => _arrayStream;

            public CancellationToken CancellationToken => _cancellation.Token;

            public static ProducerState FromPointer(void* ptr)
            {
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)ptr);
                return (ProducerState)gch.Target;
            }

            public void Start()
            {
                // Allocate and populate the producer struct
                _producer = CArrowAsyncProducer.Create();
                _producer->device_type = ArrowDeviceType.Cpu;
                _producer->request = RequestPtr;
                _producer->cancel = CancelPtr;
                _producer->additional_metadata = null;

                GCHandle gch = GCHandle.Alloc(this);
                _producer->private_data = (void*)GCHandle.ToIntPtr(gch);

                // Set the producer on the handler before calling on_schema
                _handler->producer = _producer;

                // Deliver the schema. If it can't be exported, the consumer still
                // has to hear about it: on_error followed by release.
                CArrowSchema cSchema = new CArrowSchema();
                try
                {
                    CArrowSchemaExporter.ExportSchema(_arrayStream.Schema, &cSchema);
                }
                catch (Exception ex)
                {
                    FailWithError(EOTHER, ex.Message);
                    return;
                }

                // The consumer may call request() or cancel() from within on_schema.
                // That only records state: the loop doesn't start until on_schema
                // has returned, so on_next_task can't overlap it.
#if NET5_0_OR_GREATER
                int errno = _handler->on_schema(_handler, &cSchema);
#else
                int errno = NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamConsumer.OnSchemaDelegate>(_handler->on_schema)(_handler, &cSchema);
#endif

                // A consumer that fails may not have taken ownership of the schema.
                if (cSchema.release != default)
                {
#if NET5_0_OR_GREATER
                    cSchema.release(&cSchema);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowSchemaExporter.ReleaseArrowSchema>(cSchema.release)(&cSchema);
#endif
                }

                if (errno != 0)
                {
                    // Per the spec: no on_error after a failed on_schema, just release.
                    Finish();
                    return;
                }

                bool startLoop;
                lock (_gate)
                {
                    _schemaDelivered = true;
                    startLoop = TryClaimLoopLocked();
                }
                if (startLoop)
                {
                    StartLoop();
                }
            }

            public void Request(long n)
            {
                bool startLoop;
                lock (_gate)
                {
                    // After cancel, request must be a no-op.
                    if (_cancelled || _completing || _finished)
                    {
                        return;
                    }

                    if (n <= 0)
                    {
                        SetPendingErrorLocked(EINVAL, $"request() called with invalid n = {n}; n must be positive.");
                    }
                    else
                    {
                        _demand = n > long.MaxValue - _demand ? long.MaxValue : _demand + n;
                    }

                    startLoop = TryClaimLoopLocked();
                }
                if (startLoop)
                {
                    StartLoop();
                }
            }

            public void Cancel()
            {
                bool startLoop;
                lock (_gate)
                {
                    // cancel must be idempotent.
                    if (_cancelled || _completing || _finished)
                    {
                        return;
                    }

                    _cancelled = true;
                    startLoop = TryClaimLoopLocked();
                }
                if (startLoop)
                {
                    StartLoop();
                }

                // Wake a source read that's waiting for input. Done on the thread pool
                // because cancelling can run the read's continuation inline, and that
                // would call release from inside the consumer's call to cancel().
                Task.Run(() =>
                {
                    try
                    {
                        _cancellation.Cancel();
                    }
                    catch (Exception)
                    {
                        // A source's cancellation callback threw; the loop will still
                        // see _cancelled and release.
                    }
                });
            }

            /// <summary>
            /// Report an error to the consumer from outside the produce loop
            /// (currently, a failed extract_data). Ignored once the stream is done.
            /// </summary>
            public void ReportError(int code, string message)
            {
                bool startLoop;
                lock (_gate)
                {
                    // Too late to report: nothing may follow release, and an on_error
                    // after end-of-stream would contradict it. Consumers may extract
                    // after either (arrow-go does), so they can't rely on on_error for
                    // a failed extract; extract_data's return value tells them.
                    if (_completing || _finished)
                    {
                        return;
                    }

                    SetPendingErrorLocked(code, message);
                    startLoop = TryClaimLoopLocked();
                }
                if (startLoop)
                {
                    StartLoop();
                }
            }

            private void SetPendingErrorLocked(int code, string message)
            {
                if (!_hasPendingError)
                {
                    _hasPendingError = true;
                    _pendingErrorCode = code;
                    _pendingErrorMessage = message;
                }
            }

            /// <summary>
            /// Returns true if the caller should start the produce loop, in which case
            /// <see cref="_running"/> has been set and the caller owns starting it.
            /// </summary>
            private bool TryClaimLoopLocked()
            {
                if (_running || _finished || !_schemaDelivered)
                {
                    return false;
                }

                if (_cancelled || _hasPendingError || _demand > 0)
                {
                    _running = true;
                    return true;
                }

                return false;
            }

            private void StartLoop()
            {
                // A separate task keeps request() from ever calling back into the
                // handler synchronously.
                Task.Run(() => RunProduceLoopAsync(this));
            }

            /// <summary>
            /// Decide the loop's next step. When there's nothing to do, this marks
            /// the loop as stopped under the same lock that request() takes, so a
            /// concurrent request() either is seen here or starts a new loop.
            /// </summary>
            public Step TakeNextStep(out int errorCode, out string errorMessage)
            {
                errorCode = 0;
                errorMessage = null;
                lock (_gate)
                {
                    Step step = TakeTerminalStepLocked(out errorCode, out errorMessage);
                    if (step != Step.Produce)
                    {
                        return step;
                    }

                    if (_demand == 0)
                    {
                        _running = false;
                        return Step.Idle;
                    }

                    _demand--;
                    return Step.Produce;
                }
            }

            /// <summary>
            /// If a pending error or cancellation must end the stream, commit to that
            /// (setting <see cref="_completing"/>) and return the step; otherwise
            /// return <see cref="Step.Produce"/>. A pending error wins over cancel.
            /// </summary>
            private Step TakeTerminalStepLocked(out int errorCode, out string errorMessage)
            {
                errorCode = 0;
                errorMessage = null;

                if (_hasPendingError)
                {
                    _completing = true;
                    errorCode = _pendingErrorCode;
                    errorMessage = _pendingErrorMessage;
                    return Step.Error;
                }

                if (_cancelled)
                {
                    _completing = true;
                    return Step.Release;
                }

                return Step.Produce;
            }

            /// <summary>
            /// Handle the result of a source read: deliver the batch, or end the
            /// stream. Errors or cancellation that arrived while the read was in
            /// progress take precedence. Returns false if the stream is now finished.
            /// Separated from the async loop because unsafe pointer operations
            /// cannot appear in async methods.
            /// </summary>
            public bool DeliverBatch(RecordBatch batch)
            {
                Step step;
                int errorCode;
                string errorMessage;
                lock (_gate)
                {
                    step = TakeTerminalStepLocked(out errorCode, out errorMessage);
                    if (step == Step.Produce && batch == null)
                    {
                        _completing = true;
                    }
                }

                switch (step)
                {
                    case Step.Error:
                        batch?.Dispose();
                        FailWithError(errorCode, errorMessage);
                        return false;
                    case Step.Release:
                        // Cancelled while reading: drop the batch and just release.
                        batch?.Dispose();
                        Finish();
                        return false;
                }

                if (batch == null)
                {
                    SignalEndOfStream();
                    Finish();
                    return false;
                }

                var taskState = new TaskState(this, batch);
                GCHandle taskGch = GCHandle.Alloc(taskState);
                CArrowAsyncTask cTask = new CArrowAsyncTask();
                cTask.extract_data = ExtractDataPtr;
                cTask.private_data = (void*)GCHandle.ToIntPtr(taskGch);

#if NET5_0_OR_GREATER
                int errno = _handler->on_next_task(_handler, &cTask, null);
#else
                int errno = NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamConsumer.OnNextTaskDelegate>(_handler->on_next_task)(_handler, &cTask, null);
#endif
                if (errno != 0)
                {
                    // Per the spec: no on_error after a failed on_next_task, just release.
                    // The task's GCHandle can't be freed safely here: the consumer may
                    // have extracted from a copy of the task, which already freed it.
                    Finish();
                    return false;
                }

                return true;
            }

            /// <summary>
            /// Handle a source read that threw. If the stream was cancelled meanwhile
            /// (including by our own cancellation token), just release; a pending
            /// error is reported in preference to the read's own exception.
            /// </summary>
            public void HandleReadFailure(Exception ex)
            {
                Step step;
                int errorCode;
                string errorMessage;
                lock (_gate)
                {
                    step = TakeTerminalStepLocked(out errorCode, out errorMessage);
                    _completing = true;
                }

                switch (step)
                {
                    case Step.Error:
                        FailWithError(errorCode, errorMessage);
                        break;
                    case Step.Release:
                        Finish();
                        break;
                    default:
                        FailWithError(EOTHER, ex.Message);
                        break;
                }
            }

            public void FailWithError(int code, string message)
            {
                lock (_gate)
                {
                    // Nothing may follow release.
                    if (_finished)
                    {
                        return;
                    }
                }

                SignalError(code, message);
                Finish();
            }

            private void SignalEndOfStream()
            {
#if NET5_0_OR_GREATER
                _handler->on_next_task(_handler, null, null);
#else
                NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamConsumer.OnNextTaskDelegate>(_handler->on_next_task)(_handler, null, null);
#endif
            }

            private void SignalError(int code, string message)
            {
                byte* msgPtr = null;
                try
                {
                    if (message != null)
                    {
                        msgPtr = StringUtil.ToCStringUtf8(message);
                    }
#if NET5_0_OR_GREATER
                    _handler->on_error(_handler, code, msgPtr, null);
#else
                    NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamConsumer.OnErrorDelegate>(_handler->on_error)(_handler, code, msgPtr, null);
#endif
                }
                finally
                {
                    if (msgPtr != null)
                    {
                        Marshal.FreeHGlobal((IntPtr)msgPtr);
                    }
                }
            }

            /// <summary>
            /// Release the handler and free the producer. Only called from Start or
            /// the produce loop, never concurrently, and at most once.
            /// </summary>
            public void Finish()
            {
                lock (_gate)
                {
                    if (_finished)
                    {
                        return;
                    }
                    _finished = true;
                }

#if NET5_0_OR_GREATER
                _handler->release(_handler);
#else
                NativeDelegate.GetDelegateForFunctionPointer<CArrowAsyncDeviceStreamConsumer.ReleaseHandlerDelegate>(_handler->release)(_handler);
#endif

                // The spec only requires the producer struct to stay valid until
                // release is called, so it can be freed now.
                GCHandle gch = GCHandle.FromIntPtr((IntPtr)_producer->private_data);
                gch.Free();
                CArrowAsyncProducer.Free(_producer);
                _producer = null;

                _arrayStream.Dispose();
            }
        }

        /// <summary>
        /// The produce loop, kept outside ProducerState so that it is not in an
        /// unsafe context (await is not allowed in unsafe contexts).
        /// </summary>
        private static async Task RunProduceLoopAsync(ProducerState state)
        {
            try
            {
                while (true)
                {
                    switch (state.TakeNextStep(out int errorCode, out string errorMessage))
                    {
                        case Step.Idle:
                            return;
                        case Step.Error:
                            state.FailWithError(errorCode, errorMessage);
                            return;
                        case Step.Release:
                            state.Finish();
                            return;
                    }

                    RecordBatch batch;
                    try
                    {
                        batch = await state.ArrayStream.ReadNextRecordBatchAsync(state.CancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        state.HandleReadFailure(ex);
                        return;
                    }

                    if (!state.DeliverBatch(batch))
                    {
                        return;
                    }
                }
            }
            catch (Exception ex)
            {
                // Something unexpected (e.g. an exporter bug). Finish is idempotent,
                // so this is safe even if the stream had already finished.
                try
                {
                    state.FailWithError(EOTHER, ex.Message);
                }
                catch (Exception)
                {
                }
            }
        }

        private sealed class TaskState
        {
            public TaskState(ProducerState producer, RecordBatch batch)
            {
                Producer = producer;
                Batch = batch;
            }

            public ProducerState Producer { get; }

            public RecordBatch Batch { get; }
        }
    }
}
