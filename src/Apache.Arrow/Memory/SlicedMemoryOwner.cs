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
    /// Exposes a prefix of another owner's memory without copying it.
    /// </summary>
    /// <remarks>
    /// Used when a builder hands its working memory to an <see cref="ArrowBuffer"/>: growth is
    /// geometric, so the working buffer is usually larger than the array needs, and the buffer
    /// must report only the bytes that belong to it.
    /// </remarks>
    internal sealed class SlicedMemoryOwner : IMemoryOwner<byte>
    {
        private readonly IMemoryOwner<byte> _inner;
        private readonly int _length;

        public SlicedMemoryOwner(IMemoryOwner<byte> inner, int length)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _length = length;
        }

        public Memory<byte> Memory => _inner.Memory.Slice(0, _length);

        public void Dispose() => _inner.Dispose();
    }
}
