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
using System.Runtime.InteropServices;

namespace Apache.Arrow.Memory
{
    public class NativeMemoryAllocator : MemoryAllocator
    {
        internal static readonly INativeAllocationOwner ExclusiveOwner = new NativeAllocationOwner(DefaultAlignment);

        // Releases memory and removes exactly the pressure that was added for it, which includes
        // this allocator's alignment padding (https://github.com/apache/arrow-dotnet/issues/50).
        private readonly NativeAllocationOwner _owner;

        public NativeMemoryAllocator(int alignment = DefaultAlignment)
            : base(alignment)
        {
            _owner = alignment == DefaultAlignment
                ? (NativeAllocationOwner)ExclusiveOwner
                : new NativeAllocationOwner(alignment);
        }

        /// <summary>The number of bytes of memory pressure reported for an allocation.</summary>
        internal static int PressureBytes(int length, int alignment) => length + alignment;

        internal int ReleasedPressureBytes(int length) => _owner.PressureBytes(length);

        /// <summary>
        /// Buffers from this allocator are released by <see cref="NativeMemoryManager"/>'s
        /// finalizer when they are not disposed, which means a span over one of them can be
        /// invalidated while still in use. See
        /// https://github.com/apache/arrow-dotnet/issues/397.
        /// </summary>
        public override AllocationLifetime Lifetime => AllocationLifetime.Finalized;

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            // TODO: Ensure memory is released if exception occurs.

            // TODO: Optimize storage overhead; native memory manager stores a pointer
            // to allocated memory, offset, and the allocation size. 

            // TODO: Should the allocation be moved to NativeMemory?

            int size = length + Alignment;
            IntPtr ptr = Marshal.AllocHGlobal(size);
            int offset = (int)(Alignment - (ptr.ToInt64() & (Alignment - 1)));
            var manager = new NativeMemoryManager(_owner, ptr, offset, length);

            bytesAllocated = PressureBytes(length, Alignment);

            GC.AddMemoryPressure(bytesAllocated);

            // Ensure all allocated memory is zeroed.
            manager.Memory.Span.Fill(0);

            return manager;
        }

        private sealed class NativeAllocationOwner : INativeAllocationOwner
        {
            private readonly int _alignment;

            public NativeAllocationOwner(int alignment) => _alignment = alignment;

            public int PressureBytes(int length) => NativeMemoryAllocator.PressureBytes(length, _alignment);

            public void Release(IntPtr ptr, int offset, int length)
            {
                Marshal.FreeHGlobal(ptr);
                GC.RemoveMemoryPressure(PressureBytes(length));
            }
        }
    }
}
