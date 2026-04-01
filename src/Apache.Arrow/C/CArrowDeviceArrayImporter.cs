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
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public static class CArrowDeviceArrayImporter
    {
        /// <summary>
        /// Import C pointer as an <see cref="IArrowArray"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback once all of the buffers in the returned
        /// IArrowArray are disposed. Only CPU device arrays are supported.
        /// </remarks>
        /// <param name="ptr">The pointer to the device array being imported</param>
        /// <param name="type">The type of the array being imported</param>
        /// <returns>The imported C# array</returns>
        public static unsafe IArrowArray ImportArray(CArrowDeviceArray* ptr, IArrowType type)
        {
            if (ptr == null)
            {
                throw new ArgumentNullException(nameof(ptr));
            }
            if (ptr->device_type != ArrowDeviceType.Cpu)
            {
                throw new NotSupportedException(
                    $"Importing arrays from device type {ptr->device_type} is not supported. Only CPU arrays can be imported.");
            }

            return CArrowArrayImporter.ImportArray(&ptr->array, type);
        }

        /// <summary>
        /// Import C pointer as a <see cref="RecordBatch"/>.
        /// </summary>
        /// <remarks>
        /// This will call the release callback once all of the buffers in the returned
        /// RecordBatch are disposed. Only CPU device arrays are supported.
        /// </remarks>
        /// <param name="ptr">The pointer to the device array being imported</param>
        /// <param name="schema">The schema of the record batch being imported</param>
        /// <returns>The imported C# record batch</returns>
        public static unsafe RecordBatch ImportRecordBatch(CArrowDeviceArray* ptr, Schema schema)
        {
            if (ptr == null)
            {
                throw new ArgumentNullException(nameof(ptr));
            }
            if (ptr->device_type != ArrowDeviceType.Cpu)
            {
                throw new NotSupportedException(
                    $"Importing record batches from device type {ptr->device_type} is not supported. Only CPU arrays can be imported.");
            }

            return CArrowArrayImporter.ImportRecordBatch(&ptr->array, schema);
        }
    }
}
