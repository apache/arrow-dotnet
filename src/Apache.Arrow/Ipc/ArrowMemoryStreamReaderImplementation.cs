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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Memory;

namespace Apache.Arrow.Ipc
{
    /// <summary>
    /// Reads Arrow IPC streams from a <see cref="MemoryStream"/> whose backing buffer is publicly visible.
    /// </summary>
    /// <remarks>
    /// Message metadata can be read directly from the exposed stream buffer, but record batch bodies are
    /// still copied into allocator-owned buffers to preserve <see cref="ArrowStreamReader"/> ownership semantics.
    /// </remarks>
    internal sealed class ArrowMemoryStreamReaderImplementation : ArrowStreamReaderImplementation
    {
        private readonly MemoryStream _stream;

        public ArrowMemoryStreamReaderImplementation(
            MemoryStream stream,
            MemoryAllocator allocator,
            ICompressionCodecFactory compressionCodecFactory,
            bool leaveOpen,
            ExtensionTypeRegistry extensionRegistry)
            : base(stream, allocator, compressionCodecFactory, leaveOpen, extensionRegistry)
        {
            _stream = stream;
        }

        public override ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                return new ValueTask<RecordBatch>(ReadNextRecordBatch());
            }
            catch (Exception ex)
            {
                return new ValueTask<RecordBatch>(Task.FromException<RecordBatch>(ex));
            }
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            ReadSchema();

            ReadResult result = default;
            do
            {
                result = ReadMessageFromExposedMemoryStream();
            } while (result.Batch == null && result.MessageLength > 0);

            return result.Batch;
        }

        public override ValueTask<Schema> ReadSchemaAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (HasReadSchema)
            {
                return new ValueTask<Schema>(_schema);
            }

            try
            {
                ReadSchema();
                return new ValueTask<Schema>(_schema);
            }
            catch (Exception ex)
            {
                return new ValueTask<Schema>(Task.FromException<Schema>(ex));
            }
        }

        public override void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            int schemaMessageLength = ReadMessageLengthFromExposedMemoryStream(throwOnFullRead: true, returnOnEmptyStream: true);
            if (schemaMessageLength == 0)
            {
                return;
            }

            Memory<byte> schemaBuffer = ReadExposedMemory(schemaMessageLength);
            _schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(CreateByteBuffer(schemaBuffer)), ref _dictionaryMemo, _extensionRegistry);
        }

        private ReadResult ReadMessageFromExposedMemoryStream()
        {
            int messageLength = ReadMessageLengthFromExposedMemoryStream(throwOnFullRead: false, returnOnEmptyStream: false);
            if (messageLength == 0)
            {
                return default;
            }

            Memory<byte> messageBuffer = ReadExposedMemory(messageLength);
            Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuffer));

            if (message.BodyLength > int.MaxValue)
            {
                throw new OverflowException(
                    $"Arrow IPC message body length ({message.BodyLength}) is larger than " +
                    $"the maximum supported message size ({int.MaxValue})");
            }

            int bodyLength = (int)message.BodyLength;
            Memory<byte> sourceBodyBuffer = ReadExposedMemory(bodyLength);
            IMemoryOwner<byte> bodyBufferOwner = AllocateMessageBodyBuffer(bodyLength);
            Memory<byte> bodyBuffer = bodyBufferOwner.Memory.Slice(0, bodyLength);
            sourceBodyBuffer.CopyTo(bodyBuffer);
            Google.FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuffer);

            // Keep stream-reader ownership semantics: batches outlive the source MemoryStream buffer.
            return new ReadResult(messageLength, CreateArrowObjectFromMessage(message, bodybb, bodyBufferOwner));
        }

        private int ReadMessageLengthFromExposedMemoryStream(bool throwOnFullRead, bool returnOnEmptyStream)
        {
            if (_stream.Position == _stream.Length && returnOnEmptyStream)
            {
                return 0;
            }

            if (!TryReadInt32FromExposedMemoryStream(throwOnFullRead, out int messageLength))
            {
                return 0;
            }

            if (messageLength == MessageSerializer.IpcContinuationToken &&
                !TryReadInt32FromExposedMemoryStream(throwOnFullRead, out messageLength))
            {
                return 0;
            }

            return messageLength;
        }

        private bool TryReadInt32FromExposedMemoryStream(bool throwOnFullRead, out int value)
        {
            value = 0;

            if (!TryReadExposedMemory(sizeof(int), throwOnFullRead, out Memory<byte> buffer))
            {
                return false;
            }

            value = BitUtility.ReadInt32(buffer);
            return true;
        }

        private bool TryReadExposedMemory(int length, bool throwOnFullRead, out Memory<byte> buffer)
        {
            buffer = default;

            long remainingLength = _stream.Length - _stream.Position;
            if (remainingLength < length)
            {
                if (throwOnFullRead)
                {
                    throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                }

                _stream.Position = _stream.Length;
                return false;
            }

            buffer = ReadExposedMemory(length);
            return true;
        }

        private Memory<byte> ReadExposedMemory(int length)
        {
            if (length == 0)
            {
                return Memory<byte>.Empty;
            }

            if (!_stream.TryGetBuffer(out ArraySegment<byte> streamBuffer))
            {
                throw new InvalidOperationException("Expected MemoryStream to expose its backing buffer.");
            }

            int offset = checked(streamBuffer.Offset + (int)_stream.Position);
            Memory<byte> buffer = streamBuffer.Array.AsMemory(offset, length);
            _stream.Position += length;
            return buffer;
        }
    }
}
