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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class StreamExtensionsTests
    {
        /// <summary>
        /// A stream whose Read/ReadAsync overrides throw if ever invoked, standing in for a
        /// socket-backed stream (e.g. NetworkStream) whose zero-length ReadAsync/Read does not
        /// complete immediately the way MemoryStream's does — it blocks as though waiting for the
        /// peer to send more data. Used to prove ReadFullBufferAsync/ReadFullBuffer never call
        /// into the underlying stream for a zero-length request.
        /// </summary>
        private sealed class ThrowsIfReadStream : Stream
        {
            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => throw new NotSupportedException();
            public override long Position
            {
                get => throw new NotSupportedException();
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count) =>
                throw new InvalidOperationException("Read should not be called for a zero-length buffer.");

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
                throw new InvalidOperationException("ReadAsync should not be called for a zero-length buffer.");

#if NETCOREAPP
            // Stream.ReadAsync(Memory<byte>, CancellationToken) is only overridable on
            // netcoreapp targets — net462/net472 don't declare it as virtual on Stream.
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) =>
                throw new InvalidOperationException("ReadAsync should not be called for a zero-length buffer.");
#endif

            public override void Flush() => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        }

        [Fact]
        public async Task ReadFullBufferAsync_ZeroLengthBuffer_ReturnsWithoutTouchingStream()
        {
            var stream = new ThrowsIfReadStream();
            int bytesRead = await stream.ReadFullBufferAsync(Memory<byte>.Empty);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public void ReadFullBuffer_ZeroLengthBuffer_ReturnsWithoutTouchingStream()
        {
            var stream = new ThrowsIfReadStream();
            int bytesRead = stream.ReadFullBuffer(Memory<byte>.Empty);
            Assert.Equal(0, bytesRead);
        }

        [Fact]
        public async Task ReadFullBufferAsync_NonEmptyBuffer_ReadsFromStream()
        {
            var data = new byte[] { 1, 2, 3, 4 };
            using var stream = new MemoryStream(data);
            var buffer = new byte[4];
            int bytesRead = await stream.ReadFullBufferAsync(buffer);
            Assert.Equal(4, bytesRead);
            Assert.Equal(data, buffer);
        }

        [Fact]
        public void ReadFullBuffer_NonEmptyBuffer_ReadsFromStream()
        {
            var data = new byte[] { 1, 2, 3, 4 };
            using var stream = new MemoryStream(data);
            var buffer = new byte[4];
            int bytesRead = stream.ReadFullBuffer(buffer);
            Assert.Equal(4, bytesRead);
            Assert.Equal(data, buffer);
        }
    }
}
