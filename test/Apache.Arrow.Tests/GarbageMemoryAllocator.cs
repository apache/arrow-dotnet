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
using System.Threading;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// An allocator that fills new memory with a nonzero pattern, to catch code that
    /// assumes allocated memory is zeroed. It also counts outstanding allocations.
    /// </summary>
    public class GarbageMemoryAllocator : MemoryAllocator
    {
        public const byte Garbage = 0xA5;

        private int _outstanding;
        public int Outstanding => _outstanding;

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            byte[] array = ArrayPool<byte>.Shared.Rent(length);
            array.AsSpan().Fill(Garbage);
            bytesAllocated = array.Length;
            Interlocked.Increment(ref _outstanding);
            return new GarbageMemoryOwner(this, array, length);
        }

        private sealed class GarbageMemoryOwner : IMemoryOwner<byte>
        {
            private readonly GarbageMemoryAllocator _allocator;
            private byte[] _array;
            private readonly int _length;

            public GarbageMemoryOwner(GarbageMemoryAllocator allocator, byte[] array, int length)
            {
                _allocator = allocator;
                _array = array;
                _length = length;
            }

            public Memory<byte> Memory => _array == null
                ? throw new ObjectDisposedException(nameof(GarbageMemoryOwner))
                : _array.AsMemory(0, _length);

            public void Dispose()
            {
                byte[] array = Interlocked.Exchange(ref _array, null);
                if (array != null)
                {
                    ArrayPool<byte>.Shared.Return(array);
                    Interlocked.Decrement(ref _allocator._outstanding);
                }
            }
        }
    }
}
