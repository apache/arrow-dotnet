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
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Loads the arrow-go interop shim (test/arrow-go-interop), a c-shared
    /// library wrapping arrow-go's C Async Device Stream implementation.
    /// </summary>
    /// <remarks>
    /// The library path comes from the ARROW_GO_INTEROP_LIB environment variable;
    /// ci/scripts/build_go_interop.sh builds it. The Go runtime can't be unloaded,
    /// so the library stays loaded for the life of the process.
    /// </remarks>
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public sealed unsafe class ArrowGoInteropFixture
    {
        public const string LibraryEnvironmentVariable = "ARROW_GO_INTEROP_LIB";

        private readonly string _unavailableReason;

        private readonly delegate* unmanaged[Cdecl]<byte*, void> _freeError;
        private readonly delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowAsyncDeviceStreamHandler*, long, byte*> _produceFromStream;
        private readonly delegate* unmanaged[Cdecl]<CArrowAsyncDeviceStreamHandler*, ulong, nuint*, byte*> _consumerCreate;
        private readonly delegate* unmanaged[Cdecl]<nuint, CArrowArrayStream*, byte*> _consumerToStream;
        private readonly delegate* unmanaged[Cdecl]<nuint, void> _consumerCancel;
        private readonly delegate* unmanaged[Cdecl]<nuint, void> _consumerFree;

        public ArrowGoInteropFixture()
        {
            string path = Environment.GetEnvironmentVariable(LibraryEnvironmentVariable);
            if (string.IsNullOrEmpty(path))
            {
                _unavailableReason = $"{LibraryEnvironmentVariable} not set";
                return;
            }

            IntPtr library;
            try
            {
                library = NativeLibrary.Load(path);
            }
            catch (Exception ex)
            {
                _unavailableReason = $"could not load {path}: {ex.Message}";
                return;
            }

            _freeError = (delegate* unmanaged[Cdecl]<byte*, void>)NativeLibrary.GetExport(library, "ArrowGoAsync_FreeError");
            _produceFromStream = (delegate* unmanaged[Cdecl]<CArrowArrayStream*, CArrowAsyncDeviceStreamHandler*, long, byte*>)NativeLibrary.GetExport(library, "ArrowGoAsync_ProduceFromStream");
            _consumerCreate = (delegate* unmanaged[Cdecl]<CArrowAsyncDeviceStreamHandler*, ulong, nuint*, byte*>)NativeLibrary.GetExport(library, "ArrowGoAsync_ConsumerCreate");
            _consumerToStream = (delegate* unmanaged[Cdecl]<nuint, CArrowArrayStream*, byte*>)NativeLibrary.GetExport(library, "ArrowGoAsync_ConsumerToStream");
            _consumerCancel = (delegate* unmanaged[Cdecl]<nuint, void>)NativeLibrary.GetExport(library, "ArrowGoAsync_ConsumerCancel");
            _consumerFree = (delegate* unmanaged[Cdecl]<nuint, void>)NativeLibrary.GetExport(library, "ArrowGoAsync_ConsumerFree");

            // The test host swallows stderr, so a Go fatal error otherwise just
            // looks like "test host process crashed".
            string crashLog = Environment.GetEnvironmentVariable(CrashLogEnvironmentVariable);
            if (!string.IsNullOrEmpty(crashLog))
            {
                var setCrashOutput = (delegate* unmanaged[Cdecl]<byte*, byte*>)NativeLibrary.GetExport(library, "ArrowGoAsync_SetCrashOutput");
                IntPtr crashLogPtr = Marshal.StringToCoTaskMemUTF8(crashLog);
                try
                {
                    ThrowIfError(setCrashOutput((byte*)crashLogPtr));
                }
                finally
                {
                    Marshal.FreeCoTaskMem(crashLogPtr);
                }
            }
        }

        public const string CrashLogEnvironmentVariable = "ARROW_GO_INTEROP_CRASH_LOG";

        /// <summary>
        /// Set to "1" (as the go-interop CI job does) to make a missing shim fail the
        /// tests instead of skipping them.
        /// </summary>
        public const string RequiredEnvironmentVariable = "ARROW_GO_INTEROP_REQUIRED";

        /// <summary>
        /// Skips the calling test if the shim isn't available, unless
        /// <see cref="RequiredEnvironmentVariable"/> says it must be. Unlike the
        /// Python tests, these don't run in every CI job: building the shim needs Go
        /// and cgo, so it has its own job.
        /// </summary>
        public void EnsureAvailable()
        {
            if (_unavailableReason == null)
            {
                return;
            }

            bool required = Environment.GetEnvironmentVariable(RequiredEnvironmentVariable) == "1";
            Skip.IfNot(required, $"{_unavailableReason}; skipping arrow-go interop tests.");

            throw new Exception($"{_unavailableReason}; cannot run arrow-go interop tests.");
        }

        /// <summary>
        /// Has arrow-go produce every batch of <paramref name="source"/> through
        /// <paramref name="handler"/>. Completes after arrow-go has released the handler.
        /// </summary>
        /// <param name="failAfter">If non-negative, arrow-go reports an error after this many batches.</param>
        public Task ProduceFromStreamAsync(IArrowArrayStream source, CArrowAsyncDeviceStreamHandler* handler, long failAfter = -1)
        {
            // Export on the calling thread so failures surface synchronously.
            CArrowArrayStream* cStream = CArrowArrayStream.Create();
            CArrowArrayStreamExporter.ExportArrayStream(source, cStream);

            IntPtr streamPtr = (IntPtr)cStream;
            IntPtr handlerPtr = (IntPtr)handler;
            return Task.Factory.StartNew(() =>
            {
                CArrowArrayStream* stream = (CArrowArrayStream*)streamPtr;
                try
                {
                    // arrow-go's async producer blocks until the stream is done.
                    ThrowIfError(_produceFromStream(stream, (CArrowAsyncDeviceStreamHandler*)handlerPtr, failAfter));
                }
                finally
                {
                    // arrow-go moved the stream out; Free releases it only if it didn't.
                    CArrowArrayStream.Free(stream);
                }
            }, TaskCreationOptions.LongRunning);
        }

        /// <summary>
        /// Populates <paramref name="handler"/> with arrow-go's async consumer.
        /// </summary>
        /// <param name="queueSize">Batches arrow-go requests up front; it then requests one per batch consumed.</param>
        public GoConsumer CreateConsumer(CArrowAsyncDeviceStreamHandler* handler, ulong queueSize)
        {
            nuint id;
            ThrowIfError(_consumerCreate(handler, queueSize, &id));
            return new GoConsumer(this, id);
        }

        private void ThrowIfError(byte* error)
        {
            if (error == null)
            {
                return;
            }

            string message = Marshal.PtrToStringUTF8((IntPtr)error);
            _freeError(error);
            throw new ArrowGoInteropException(message);
        }

        /// <summary>
        /// An arrow-go async consumer attached to a handler.
        /// </summary>
        public sealed class GoConsumer : IDisposable
        {
            private readonly ArrowGoInteropFixture _fixture;
            private readonly nuint _id;
            private bool _disposed;

            internal GoConsumer(ArrowGoInteropFixture fixture, nuint id)
            {
                _fixture = fixture;
                _id = id;
            }

            /// <summary>
            /// Waits for the producer to deliver the schema, then returns what
            /// arrow-go consumes as a synchronous stream. Errors the producer
            /// reports later surface from ReadNextRecordBatchAsync.
            /// </summary>
            /// <remarks>
            /// arrow-go's on_schema blocks until this has been called, so start it
            /// before (or concurrently with) the producer.
            /// </remarks>
            public Task<IArrowArrayStream> ToStreamAsync()
            {
                return Task.Factory.StartNew(() =>
                {
                    CArrowArrayStream* cStream = CArrowArrayStream.Create();
                    try
                    {
                        _fixture.ThrowIfError(_fixture._consumerToStream(_id, cStream));
                        return CArrowArrayStreamImporter.ImportArrayStream(cStream);
                    }
                    finally
                    {
                        CArrowArrayStream.Free(cStream);
                    }
                }, TaskCreationOptions.LongRunning);
            }

            public void Cancel() => _fixture._consumerCancel(_id);

            public void Dispose()
            {
                if (!_disposed)
                {
                    _disposed = true;
                    _fixture._consumerFree(_id);
                }
            }
        }
    }

    public sealed class ArrowGoInteropException : Exception
    {
        public ArrowGoInteropException(string message) : base(message)
        {
        }
    }
}

#endif
