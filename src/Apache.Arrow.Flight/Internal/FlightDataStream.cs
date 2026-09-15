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
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Flatbuf;
using Apache.Arrow.Flight.Protocol;
using Apache.Arrow.Ipc;
using Google.FlatBuffers;
using Google.Protobuf;
using Grpc.Core;

namespace Apache.Arrow.Flight.Internal
{
    /// <summary>
    /// Handles writing record batches as flight data
    /// </summary>
    internal class FlightDataStream : ArrowStreamWriter
    {
        private readonly FlightDescriptor _flightDescriptor;
        private readonly IAsyncStreamWriter<Protocol.FlightData> _clientStreamWriter;
        private Protocol.FlightData _currentFlightData;
        private bool _hasPendingMessage;
        private ByteString _recordBatchAppMetadata;

        public FlightDataStream(IAsyncStreamWriter<Protocol.FlightData> clientStreamWriter, FlightDescriptor flightDescriptor, Schema schema)
            : base(new MemoryStream(), schema)
        {
            _clientStreamWriter = clientStreamWriter;
            _flightDescriptor = flightDescriptor;
        }

        public async Task SendSchema()
        {
            var offset = SerializeSchema(Schema);
            await WriteMessageAsync(MessageHeader.Schema, offset, 0, default, CancellationToken.None).ConfigureAwait(false);
            await FlushCurrentMessageAsync().ConfigureAwait(false);
            HasWrittenSchema = true;
        }

        private void ResetStream()
        {
            this.BaseStream.Position = 0;
            this.BaseStream.SetLength(0);
        }

        public async Task Write(RecordBatch recordBatch, ByteString applicationMetadata)
        {
            if (!HasWrittenSchema)
            {
                await SendSchema().ConfigureAwait(false);
            }
            ResetStream();

            // Attached to the record-batch message, not to any preceding dictionary messages.
            _recordBatchAppMetadata = applicationMetadata;

            // Writes any dictionary-batch messages followed by the record-batch message. Each is
            // flushed as its own FlightData frame (see WriteMessageAsync) so that dictionary batches
            // are delivered before the record batch that references them.
            await WriteRecordBatchInternalAsync(recordBatch, customMetadata: null).ConfigureAwait(false);

            // Flush the final (record-batch) message.
            await FlushCurrentMessageAsync().ConfigureAwait(false);
            _recordBatchAppMetadata = null;
        }

        private protected override async ValueTask<long> WriteMessageAsync<T>(MessageHeader headerType, Offset<T> headerOffset, int bodyLength, VectorOffset customMetadataOffset, CancellationToken cancellationToken)
        {
            // A new message is beginning; the previous message's body is now fully buffered, so flush
            // it as its own FlightData frame before starting the next one.
            if (_hasPendingMessage)
            {
                await FlushCurrentMessageAsync().ConfigureAwait(false);
            }

            Offset<Flatbuf.Message> messageOffset = Flatbuf.Message.CreateMessage(
                Builder, CurrentMetadataVersion, headerType, headerOffset.Value,
                bodyLength, customMetadataOffset);

            Builder.Finish(messageOffset.Value);

            ReadOnlyMemory<byte> messageData = Builder.DataBuffer.ToReadOnlyMemory(Builder.DataBuffer.Position, Builder.Offset);

            _currentFlightData = new Protocol.FlightData
            {
                DataHeader = ByteString.CopyFrom(messageData.Span)
            };

            if (headerType == MessageHeader.Schema && _flightDescriptor != null)
            {
                _currentFlightData.FlightDescriptor = _flightDescriptor.ToProtocol();
            }

            if (headerType == MessageHeader.RecordBatch && _recordBatchAppMetadata != null)
            {
                _currentFlightData.AppMetadata = _recordBatchAppMetadata;
            }

            _hasPendingMessage = true;
            return 0;
        }

        private async Task FlushCurrentMessageAsync()
        {
            this.BaseStream.Position = 0;
            _currentFlightData.DataBody = await ByteString.FromStreamAsync(this.BaseStream).ConfigureAwait(false);
            await _clientStreamWriter.WriteAsync(_currentFlightData).ConfigureAwait(false);
            ResetStream();
            _hasPendingMessage = false;
        }
    }
}
