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

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Controls how much memory a <see cref="PooledManagedAllocator"/> keeps for reuse.
    /// </summary>
    /// <remarks>
    /// The pool keeps arrays in sizes that are powers of two. With the defaults it holds at most
    /// 16 arrays of each size up to 1 MiB, which bounds the memory it retains to about 32 MiB.
    /// Larger allocations are not pooled: they get a new array, which is collected normally once
    /// it is no longer used.
    /// </remarks>
    public sealed class PooledManagedAllocatorOptions
    {
        public const int DefaultMaxPooledLength = 1024 * 1024;
        public const int DefaultMaxArraysPerSize = 16;

        /// <summary>
        /// The largest allocation, in bytes, that is served from the pool. Larger allocations
        /// get a new, unpooled array.
        /// </summary>
        public int MaxPooledLength { get; set; } = DefaultMaxPooledLength;

        /// <summary>
        /// The most arrays of any one size that the pool keeps for reuse.
        /// </summary>
        public int MaxArraysPerSize { get; set; } = DefaultMaxArraysPerSize;

        /// <summary>
        /// The most bytes the pool keeps for reuse, across all sizes. Unlimited by default, since
        /// <see cref="MaxPooledLength"/> and <see cref="MaxArraysPerSize"/> already bound it.
        /// </summary>
        public long MaxRetainedBytes { get; set; } = long.MaxValue;

        /// <summary>
        /// Overwrites each buffer with a recognizable pattern (0xDE) when it is disposed. This
        /// makes a span that is wrongly used after its buffer was disposed read obvious garbage,
        /// rather than data that looks plausible. Intended for finding such bugs; off by default.
        /// </summary>
        public bool PoisonDisposedBuffers { get; set; }

        internal void Validate()
        {
            if (MaxPooledLength < 0 || MaxPooledLength > PooledManagedAllocator.LargestPoolableLength)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxPooledLength),
                    $"Must be between 0 and {PooledManagedAllocator.LargestPoolableLength}.");
            }

            if (MaxArraysPerSize < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxArraysPerSize), "Must not be negative.");
            }

            if (MaxRetainedBytes < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(MaxRetainedBytes), "Must not be negative.");
            }
        }
    }
}
