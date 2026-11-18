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
    /// An Arrow C Async Device Stream Interface ArrowAsyncTask, which represents
    /// a single data batch that can be extracted by the consumer.
    /// </summary>
    /// <remarks>
    /// This matches the layout of the ArrowAsyncTask struct described in
    /// https://arrow.apache.org/docs/format/CDeviceDataInterface.html.
    /// </remarks>
    [StructLayout(LayoutKind.Sequential)]
    [Experimental("ArrowAsyncDeviceStreamApi")]
    public unsafe struct CArrowAsyncTask
    {
        /// <summary>
        /// Callback to extract the data for this task into an ArrowDeviceArray.
        /// The consumer must call this even if it does not want the data (passing
        /// a null out pointer is acceptable) so that the producer can clean up.
        ///
        /// Return value: 0 if successful, an errno-compatible error code otherwise.
        /// </summary>
#if NET5_0_OR_GREATER
        internal delegate* unmanaged<CArrowAsyncTask*, CArrowDeviceArray*, int> extract_data;
#else
        internal IntPtr extract_data;
#endif

        public void* private_data;
    }
}
