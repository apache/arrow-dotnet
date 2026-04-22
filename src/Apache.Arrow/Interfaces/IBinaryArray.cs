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

namespace Apache.Arrow
{
    internal interface IBinaryArray
    {
        /// <summary>
        /// The number of values in the array
        /// </summary>
        int Length { get; }

        /// <summary>
        /// Get the collection of bytes, as a read-only span, at a given index in the array.
        /// </summary>
        /// <param name="index">Index at which to get bytes.</param>
        /// <param name="isNull">Set to <see langword="true"/> if the value at the given index is null.</param>
        /// <returns>Returns a <see cref="ReadOnlySpan{Byte}"/> object.</returns>
        /// <exception cref="ArgumentOutOfRangeException">If the index is negative or beyond the length of the array.
        /// </exception>
        public ReadOnlySpan<byte> GetBytes(int index, out bool isNull);
    }
}
