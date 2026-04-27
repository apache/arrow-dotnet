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
    internal class ArrowStreamReaderImplementation : ArrowReaderImplementation
    {
        public Stream BaseStream { get; }
        private readonly bool _leaveOpen;

        public ArrowStreamReaderImplementation(Stream stream, MemoryAllocator allocator, ICompressionCodecFactory compressionCodecFactory, bool leaveOpen)
            : this(stream, allocator, compressionCodecFactory, leaveOpen, null)
        {
        }

        public ArrowStreamReaderImplementation(Stream stream, MemoryAllocator allocator, ICompressionCodecFactory compressionCodecFactory, bool leaveOpen, ExtensionTypeRegistry extensionRegistry)
            : base(allocator, compressionCodecFactory, extensionRegistry)
        {
            BaseStream = stream;
            _leaveOpen = leaveOpen;
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_leaveOpen)
            {
                BaseStream.Dispose();
            }
        }

        public override ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (CanUseExposedMemoryStreamFastPath())
            {
                try
                {
                    return new ValueTask<RecordBatch>(ReadNextRecordBatch());
                }
                catch (Exception ex)
                {
                    return new ValueTask<RecordBatch>(Task.FromException<RecordBatch>(ex));
                }
            }

            return ReadRecordBatchAsync(cancellationToken);
        }

        public override RecordBatch ReadNextRecordBatch()
        {
            return ReadRecordBatch();
        }

        protected async ValueTask<RecordBatch> ReadRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            await ReadSchemaAsync(cancellationToken).ConfigureAwait(false);

            ReadResult result = default;
            do
            {
                result = await ReadMessageAsync(cancellationToken).ConfigureAwait(false);
            } while (result.Batch == null && result.MessageLength > 0);

            return result.Batch;
        }

        protected async ValueTask<ReadResult> ReadMessageAsync(CancellationToken cancellationToken)
        {
            int messageLength = await ReadMessageLengthAsync(throwOnFullRead: false, returnOnEmptyStream: false, cancellationToken)
                .ConfigureAwait(false);

            if (messageLength == 0)
            {
                // reached end
                return default;
            }

            RecordBatch result = null;
            using (ArrayPool<byte>.Shared.RentReturn(messageLength, out Memory<byte> messageBuff))
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(messageBuff, cancellationToken)
                    .ConfigureAwait(false);
                EnsureFullRead(messageBuff, bytesRead);

                Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                int bodyLength = checked((int)message.BodyLength);

                IMemoryOwner<byte> bodyBuffOwner = AllocateMessageBodyBuffer(bodyLength);
                Memory<byte> bodyBuff = bodyBuffOwner.Memory.Slice(0, bodyLength);
                bytesRead = await BaseStream.ReadFullBufferAsync(bodyBuff, cancellationToken)
                    .ConfigureAwait(false);
                EnsureFullRead(bodyBuff, bytesRead);

                Google.FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                result = CreateArrowObjectFromMessage(message, bodybb, bodyBuffOwner);
            }

            return new ReadResult(messageLength, result);
        }

        protected RecordBatch ReadRecordBatch()
        {
            ReadSchema();

            ReadResult result = default;
            do
            {
                result = ReadMessage();
            } while (result.Batch == null && result.MessageLength > 0);

            return result.Batch;
        }

        protected ReadResult ReadMessage()
        {
            if (TryReadMessageFromExposedMemoryStream(out ReadResult directResult))
            {
                return directResult;
            }

            int messageLength = ReadMessageLength(throwOnFullRead: false, returnOnEmptyStream: false);
            if (messageLength == 0)
            {
                // reached end
                return default;
            }

            RecordBatch result = null;
            using (ArrayPool<byte>.Shared.RentReturn(messageLength, out Memory<byte> messageBuff))
            {
                int bytesRead = BaseStream.ReadFullBuffer(messageBuff);
                EnsureFullRead(messageBuff, bytesRead);

                Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuff));

                if (message.BodyLength > int.MaxValue)
                {
                    throw new OverflowException(
                        $"Arrow IPC message body length ({message.BodyLength}) is larger than " +
                        $"the maximum supported message size ({int.MaxValue})");
                }
                int bodyLength = (int)message.BodyLength;

                IMemoryOwner<byte> bodyBuffOwner = AllocateMessageBodyBuffer(bodyLength);
                Memory<byte> bodyBuff = bodyBuffOwner.Memory.Slice(0, bodyLength);
                bytesRead = BaseStream.ReadFullBuffer(bodyBuff);
                EnsureFullRead(bodyBuff, bytesRead);

                Google.FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuff);
                result = CreateArrowObjectFromMessage(message, bodybb, bodyBuffOwner);
            }

            return new ReadResult(messageLength, result);
        }

        private IMemoryOwner<byte> AllocateMessageBodyBuffer(int bodyLength)
        {
            return _allocator.Allocate(bodyLength);
        }

        private bool TryReadMessageFromExposedMemoryStream(out ReadResult result)
        {
            result = default;

            if (!TryReadMessageLengthFromExposedMemoryStream(throwOnFullRead: false, returnOnEmptyStream: false, out int messageLength))
            {
                return false;
            }

            if (messageLength == 0)
            {
                return true;
            }

            TryGetExposedMemoryStream(out MemoryStream stream, out ArraySegment<byte> streamBuffer);

            Memory<byte> messageBuffer = ReadExposedMemory(stream, streamBuffer, messageLength);
            Flatbuf.Message message = Flatbuf.Message.GetRootAsMessage(CreateByteBuffer(messageBuffer));

            if (message.BodyLength > int.MaxValue)
            {
                throw new OverflowException(
                    $"Arrow IPC message body length ({message.BodyLength}) is larger than " +
                    $"the maximum supported message size ({int.MaxValue})");
            }

            int bodyLength = (int)message.BodyLength;
            Memory<byte> sourceBodyBuffer = ReadExposedMemory(stream, streamBuffer, bodyLength);
            IMemoryOwner<byte> bodyBufferOwner = AllocateMessageBodyBuffer(bodyLength);
            Memory<byte> bodyBuffer = bodyBufferOwner.Memory.Slice(0, bodyLength);
            sourceBodyBuffer.CopyTo(bodyBuffer);
            Google.FlatBuffers.ByteBuffer bodybb = CreateByteBuffer(bodyBuffer);

            // Stream readers have historically returned batches backed by reader-owned body
            // buffers. Keep that ownership boundary even when the stream exposes its byte[].
            result = new ReadResult(messageLength, CreateArrowObjectFromMessage(message, bodybb, bodyBufferOwner));
            return true;
        }

        private bool TryReadSchemaFromExposedMemoryStream()
        {
            if (!TryReadMessageLengthFromExposedMemoryStream(throwOnFullRead: true, returnOnEmptyStream: true, out int schemaMessageLength))
            {
                return false;
            }

            if (schemaMessageLength == 0)
            {
                return true;
            }

            TryGetExposedMemoryStream(out MemoryStream stream, out ArraySegment<byte> streamBuffer);
            Memory<byte> schemaBuffer = ReadExposedMemory(stream, streamBuffer, schemaMessageLength);
            _schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(CreateByteBuffer(schemaBuffer)), ref _dictionaryMemo, _extensionRegistry);
            return true;
        }

        private bool TryReadMessageLengthFromExposedMemoryStream(bool throwOnFullRead, bool returnOnEmptyStream, out int messageLength)
        {
            messageLength = 0;

            if (!TryGetExposedMemoryStream(out MemoryStream stream, out ArraySegment<byte> streamBuffer))
            {
                return false;
            }

            if (stream.Position == stream.Length && returnOnEmptyStream)
            {
                return true;
            }

            if (!TryReadInt32FromExposedMemoryStream(stream, streamBuffer, throwOnFullRead, out messageLength))
            {
                return true;
            }

            if (messageLength == MessageSerializer.IpcContinuationToken &&
                !TryReadInt32FromExposedMemoryStream(stream, streamBuffer, throwOnFullRead, out messageLength))
            {
                return true;
            }

            return true;
        }

        private static bool TryReadInt32FromExposedMemoryStream(MemoryStream stream, ArraySegment<byte> streamBuffer, bool throwOnFullRead, out int value)
        {
            value = 0;

            if (!TryReadExposedMemory(stream, streamBuffer, sizeof(int), throwOnFullRead, out Memory<byte> buffer))
            {
                return false;
            }

            value = BitUtility.ReadInt32(buffer);
            return true;
        }

        private static bool TryReadExposedMemory(MemoryStream stream, ArraySegment<byte> streamBuffer, int length, bool throwOnFullRead, out Memory<byte> buffer)
        {
            buffer = default;

            long remainingLength = stream.Length - stream.Position;
            if (remainingLength < length)
            {
                if (throwOnFullRead)
                {
                    throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
                }

                stream.Position = stream.Length;
                return false;
            }

            buffer = ReadExposedMemory(stream, streamBuffer, length);
            return true;
        }

        private static Memory<byte> ReadExposedMemory(MemoryStream stream, ArraySegment<byte> streamBuffer, int length)
        {
            if (length == 0)
            {
                return Memory<byte>.Empty;
            }

            int offset = checked(streamBuffer.Offset + (int)stream.Position);
            Memory<byte> buffer = streamBuffer.Array.AsMemory(offset, length);
            stream.Position += length;
            return buffer;
        }

        private bool TryGetExposedMemoryStream(out MemoryStream stream, out ArraySegment<byte> streamBuffer)
        {
            if (BaseStream is MemoryStream memoryStream && memoryStream.TryGetBuffer(out streamBuffer))
            {
                stream = memoryStream;
                return true;
            }

            stream = null;
            streamBuffer = default;
            return false;
        }

        protected bool CanUseExposedMemoryStreamFastPath()
        {
            return TryGetExposedMemoryStream(out _, out _);
        }

        public override ValueTask<Schema> ReadSchemaAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (HasReadSchema)
            {
                return new ValueTask<Schema>(_schema);
            }

            if (CanUseExposedMemoryStreamFastPath())
            {
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

            return ReadSchemaAsyncCore(cancellationToken);
        }

        private async ValueTask<Schema> ReadSchemaAsyncCore(CancellationToken cancellationToken)
        {
            // Figure out length of schema
            int schemaMessageLength = await ReadMessageLengthAsync(throwOnFullRead: true, returnOnEmptyStream: true, cancellationToken)
                .ConfigureAwait(false);
            if (schemaMessageLength == 0)
            {
                return null;
            }

            using (ArrayPool<byte>.Shared.RentReturn(schemaMessageLength, out Memory<byte> buff))
            {
                // Read in schema
                int bytesRead = await BaseStream.ReadFullBufferAsync(buff, cancellationToken).ConfigureAwait(false);
                EnsureFullRead(buff, bytesRead);

                Google.FlatBuffers.ByteBuffer schemabb = CreateByteBuffer(buff);
                _schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb), ref _dictionaryMemo, _extensionRegistry);
                return _schema;
            }
        }

        public override void ReadSchema()
        {
            if (HasReadSchema)
            {
                return;
            }

            if (TryReadSchemaFromExposedMemoryStream())
            {
                return;
            }

            // Figure out length of schema
            int schemaMessageLength = ReadMessageLength(throwOnFullRead: true, returnOnEmptyStream: true);
            if (schemaMessageLength == 0)
            {
                return;
            }

            using (ArrayPool<byte>.Shared.RentReturn(schemaMessageLength, out Memory<byte> buff))
            {
                int bytesRead = BaseStream.ReadFullBuffer(buff);
                EnsureFullRead(buff, bytesRead);

                Google.FlatBuffers.ByteBuffer schemabb = CreateByteBuffer(buff);
                _schema = MessageSerializer.GetSchema(ReadMessage<Flatbuf.Schema>(schemabb), ref _dictionaryMemo, _extensionRegistry);
            }
        }

        private async ValueTask<int> ReadMessageLengthAsync(bool throwOnFullRead, bool returnOnEmptyStream, CancellationToken cancellationToken = default)
        {
            int messageLength = 0;
            using (ArrayPool<byte>.Shared.RentReturn(4, out Memory<byte> lengthBuffer))
            {
                int bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer, cancellationToken)
                    .ConfigureAwait(false);
                if (bytesRead == 0 && returnOnEmptyStream)
                {
                    return 0;
                }
                if (throwOnFullRead)
                {
                    EnsureFullRead(lengthBuffer, bytesRead);
                }
                else if (bytesRead != 4)
                {
                    return 0;
                }

                messageLength = BitUtility.ReadInt32(lengthBuffer);

                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (messageLength == MessageSerializer.IpcContinuationToken)
                {
                    bytesRead = await BaseStream.ReadFullBufferAsync(lengthBuffer, cancellationToken)
                        .ConfigureAwait(false);
                    if (throwOnFullRead)
                    {
                        EnsureFullRead(lengthBuffer, bytesRead);
                    }
                    else if (bytesRead != 4)
                    {
                        return 0;
                    }

                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            }

            return messageLength;
        }

        private int ReadMessageLength(bool throwOnFullRead, bool returnOnEmptyStream)
        {
            int messageLength = 0;
            using (ArrayPool<byte>.Shared.RentReturn(4, out Memory<byte> lengthBuffer))
            {
                int bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                if (bytesRead == 0 && returnOnEmptyStream)
                {
                    return 0;
                }
                if (throwOnFullRead)
                {
                    EnsureFullRead(lengthBuffer, bytesRead);
                }
                else if (bytesRead != 4)
                {
                    return 0;
                }

                messageLength = BitUtility.ReadInt32(lengthBuffer);

                // ARROW-6313, if the first 4 bytes are continuation message, read the next 4 for the length
                if (messageLength == MessageSerializer.IpcContinuationToken)
                {
                    bytesRead = BaseStream.ReadFullBuffer(lengthBuffer);
                    if (throwOnFullRead)
                    {
                        EnsureFullRead(lengthBuffer, bytesRead);
                    }
                    else if (bytesRead != 4)
                    {
                        return 0;
                    }

                    messageLength = BitUtility.ReadInt32(lengthBuffer);
                }
            }

            return messageLength;
        }

        /// <summary>
        /// Ensures the number of bytes read matches the buffer length
        /// and throws an exception it if doesn't. This ensures we have read
        /// a full buffer from the stream.
        /// </summary>
        internal static void EnsureFullRead(Memory<byte> buffer, int bytesRead)
        {
            if (bytesRead != buffer.Length)
            {
                throw new InvalidOperationException("Unexpectedly reached the end of the stream before a full buffer was read.");
            }
        }

        internal struct ReadResult
        {
            public readonly int MessageLength;
            public readonly RecordBatch Batch;

            public ReadResult(int messageLength, RecordBatch batch)
            {
                MessageLength = messageLength;
                Batch = batch;
            }
        }

    }
}
