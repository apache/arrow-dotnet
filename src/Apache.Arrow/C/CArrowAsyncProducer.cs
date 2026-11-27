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

namespace Apache.Arrow.C
{
    /// <summary>
    /// An Arrow C Async Device Stream Interface ArrowAsyncProducer, which provides
    /// flow control for the consumer to request data or cancel the stream.
    /// </summary>
    /// <remarks>
    /// This matches the layout of the ArrowAsyncProducer struct described in
    /// https://arrow.apache.org/docs/format/CDeviceDataInterface.html.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public unsafe struct CArrowAsyncProducer
    {
        /// <summary>
        /// The device type that this stream produces data on.
        /// </summary>
        public ArrowDeviceType device_type;

        /// <summary>
        /// Callback for the consumer to request n additional data batches.
        /// Must not synchronously call on_next_task or on_error to avoid recursion.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncProducer*, long, void> request;
#else
        internal IntPtr request;
#endif

        /// <summary>
        /// Callback for the consumer to signal cancellation.
        /// Must be idempotent and thread-safe.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncProducer*, void> cancel;
#else
        internal IntPtr cancel;
#endif

        /// <summary>
        /// Optional stream-level metadata, encoded like Arrow schema metadata.
        /// May be null. Must be valid for the lifetime of this object.
        /// </summary>
        public byte* additional_metadata;

        public void* private_data;

        /// <summary>
        /// Allocate and zero-initialize an unmanaged pointer of this type.
        /// </summary>
        /// <remarks>
        /// This pointer must later be freed by <see cref="Free"/>.
        /// </remarks>
        public static CArrowAsyncProducer* Create()
        {
            var ptr = (CArrowAsyncProducer*)Marshal.AllocHGlobal(sizeof(CArrowAsyncProducer));

            *ptr = default;

            return ptr;
        }

        /// <summary>
        /// Free a pointer that was allocated in <see cref="Create"/>.
        /// </summary>
        /// <remarks>
        /// Do not call this on a pointer that was allocated elsewhere.
        /// </remarks>
        public static void Free(CArrowAsyncProducer* producer)
        {
            Marshal.FreeHGlobal((IntPtr)producer);
        }
    }
}
