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
    internal sealed class SharedMemoryHandle : IMemoryOwner<byte>
    {
        private SharedMemoryOwner _owner;
        private readonly Memory<byte> _memory;
        private readonly int _start;

        public SharedMemoryHandle(SharedMemoryOwner owner)
            : this(owner, 0, owner?.Memory.Length ?? 0)
        {
        }

        /// <summary>
        /// Creates a handle to a slice of the owner's memory. The handle holds a reference to all
        /// of it.
        /// </summary>
        public SharedMemoryHandle(SharedMemoryOwner owner, int start, int length)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            _memory = owner.Memory.Slice(start, length);
            _start = start;

            if (!owner.FinalizeWhenUnreachable)
            {
                // The memory came from an allocator whose Lifetime is not Finalized, which
                // opts out of release by collection: see the finalizer below.
                GC.SuppressFinalize(this);
            }
        }

        public Memory<byte> Memory
        {
            get
            {
                if (_owner == null)
                {
                    ThrowDisposed();
                }
                return _memory;
            }
        }

        public SharedMemoryHandle Retain()
        {
            if (_owner == null)
            {
                ThrowDisposed();
            }

            return _owner.Retain(_start, _memory.Length);
        }

        private static void ThrowDisposed() => throw new ObjectDisposedException(nameof(SharedMemoryHandle));

        public void Dispose()
        {
            Release();
            GC.SuppressFinalize(this);
        }

        // Releases buffers that were never disposed, as it always has. Doing so lets the GC
        // decide when memory is freed or recycled, which can invalidate a span that is still in
        // use (https://github.com/apache/arrow-dotnet/issues/397), so allocators can opt out by
        // reporting a Lifetime other than Finalized; existing allocators keep this behaviour.
        ~SharedMemoryHandle()
        {
            Release();
        }

        private void Release()
        {
            _owner?.Release();
            _owner = null;
        }
    }
}
