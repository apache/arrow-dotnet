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

namespace Apache.Arrow.Memory
{
    internal sealed class SharedMemoryOwner
    {
        private readonly IMemoryOwner<byte> _inner;
        private readonly Memory<byte> _memory;
        private int _refCount;

        public SharedMemoryOwner(IMemoryOwner<byte> inner)
            : this(inner, finalizeWhenUnreachable: true)
        {
        }

        public SharedMemoryOwner(IMemoryOwner<byte> inner, bool finalizeWhenUnreachable)
        {
            FinalizeWhenUnreachable = finalizeWhenUnreachable;
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _memory = inner.Memory;
            _refCount = 1;
        }

        public Memory<byte> Memory => _memory;

        /// <summary>
        /// Whether handles to this memory release it from a finalizer when they are not disposed.
        /// </summary>
        public bool FinalizeWhenUnreachable { get; }

        public SharedMemoryHandle Retain() => Retain(0, _memory.Length);

        /// <summary>
        /// Adds a reference, returning a handle to the given slice of the memory.
        /// </summary>
        public SharedMemoryHandle Retain(int start, int length)
        {
            return TryRetain(start, length) ?? throw new ObjectDisposedException(nameof(SharedMemoryOwner));
        }

        /// <summary>
        /// Adds a reference to the whole memory, or returns <c>null</c> if it has already been
        /// released. Once released, memory can never be retained again.
        /// </summary>
        public SharedMemoryHandle TryRetain() => TryRetain(0, _memory.Length);

        private SharedMemoryHandle TryRetain(int start, int length)
        {
            while (true)
            {
                int current = Volatile.Read(ref _refCount);
                if (current <= 0)
                {
                    return null;
                }

                if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
                {
                    return new SharedMemoryHandle(this, start, length);
                }
            }
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                _inner.Dispose();
            }
        }
    }
}
