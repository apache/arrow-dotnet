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
using System.Buffers;

namespace Apache.Arrow.Memory
{
    /// <summary>
    /// Allocates plain managed arrays, with no pooling.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the simplest allocator to use safely. Buffers are reclaimed by the garbage
    /// collector, a span over a buffer keeps its array alive while the span is in use, and
    /// disposal releases nothing, so even a span used after its buffer has been disposed still
    /// sees that buffer's data. It avoids the hazard described in
    /// https://github.com/apache/arrow-dotnet/issues/397 without requiring disposal.
    /// </para>
    /// <para>
    /// The costs are those of ordinary managed allocation: every buffer is a new array, arrays
    /// of 85,000 bytes or more are allocated on the large object heap, and memory is aligned
    /// only to the pointer size rather than to the 64 bytes the Arrow format recommends. Use
    /// <see cref="PooledManagedAllocator"/> to reuse arrays, at the price of having to stop
    /// using a buffer's spans once it is disposed.
    /// </para>
    /// </remarks>
    public sealed class ManagedMemoryAllocator : MemoryAllocator
    {
        /// <summary>
        /// The instance used as temporary memory by builders that are not given an allocator.
        /// </summary>
        internal static readonly ManagedMemoryAllocator Instance = new ManagedMemoryAllocator();

        public ManagedMemoryAllocator()
            : base(IntPtr.Size)
        {
        }

        public override AllocationLifetime Lifetime => AllocationLifetime.GarbageCollected;

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            bytesAllocated = length;
            return new ArrayOwner(new byte[length]);
        }

        private sealed class ArrayOwner : IMemoryOwner<byte>
        {
            public ArrayOwner(byte[] array) => Memory = array;

            public Memory<byte> Memory { get; private set; }

            // The array is collected by the GC; disposal only stops further use of this owner.
            public void Dispose() => Memory = Memory<byte>.Empty;
        }
    }
}
