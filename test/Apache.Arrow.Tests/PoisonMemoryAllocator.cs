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
using System.Collections.Generic;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Tests
{
    public class PoisonMemoryAllocator : MemoryAllocator, IDisposable
    {
        private readonly List<IMemoryOwner<byte>> _allocatedOwners = new List<IMemoryOwner<byte>>();
        private bool _disposed;

        public PoisonMemoryAllocator(int alignment = DefaultAlignment) : base(alignment)
        {
        }

        protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
        {
            var innerOwner = NativeMemoryAllocator.Default.Value.Allocate(length);
            bytesAllocated = length;
            lock (_allocatedOwners)
            {
                _allocatedOwners.Add(innerOwner);
            }
            return new PoisonMemoryOwner(innerOwner);
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }
            _disposed = true;
            lock (_allocatedOwners)
            {
                foreach (IMemoryOwner<byte> owner in _allocatedOwners)
                {
                    owner.Dispose();
                }
                _allocatedOwners.Clear();
            }
        }

        private sealed class PoisonMemoryOwner : IMemoryOwner<byte>
        {
            private readonly IMemoryOwner<byte> _inner;
            private bool _disposed;

            public PoisonMemoryOwner(IMemoryOwner<byte> inner)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            }

            public Memory<byte> Memory
            {
                get
                {
                    if (_disposed)
                    {
                        throw new ObjectDisposedException(nameof(PoisonMemoryOwner));
                    }
                    return _inner.Memory;
                }
            }

            public void Dispose()
            {
                if (_disposed)
                {
                    return;
                }
                _disposed = true;
                _inner.Memory.Span.Fill(0xFF);
            }
        }
    }
}
