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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.Flight.Client;
using Apache.Arrow.Flight.TestWeb;
using Apache.Arrow.Memory;
using Grpc.Core.Utils;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Apache.Arrow.Flight.Tests
{
    /// <summary>
    /// Tests that an <see cref="ArrowContext"/> given to a <see cref="FlightClient"/> or to a
    /// <see cref="Server.FlightServer"/> reaches the record-batch readers and writers they create.
    /// The server is created by dependency injection, so the server tests also cover that path.
    /// </summary>
    public class FlightContextTests
    {
        private static readonly Guid s_guid = Guid.Parse("01234567-89ab-cdef-0123-456789abcdef");

        [Fact]
        public async Task ServerContextRegistryResolvesExtensionTypesOnPut()
        {
            FlightStore store = new FlightStore();
            using TestWebFactory factory = new TestWebFactory(store, GuidContext());
            FlightClient client = new FlightClient(factory.GetChannel());
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("guids");

            RecordBatch batch = GuidBatch();
            FlightRecordBatchDuplexStreamingCall put = await client.StartPut(descriptor, batch.Schema);
            await put.RequestStream.WriteAsync(batch);
            await put.RequestStream.CompleteAsync();
            await put.ResponseStream.ToListAsync();

            RecordBatch received = store.Flights[descriptor].GetRecordBatches().Single().RecordBatch;
            Assert.Equal(s_guid, Assert.IsType<GuidArray>(received.Column(0)).GetGuid(0));
        }

        [Fact]
        public async Task ServerWithoutContextUsesTheDefaultRegistry()
        {
            Assert.False(ExtensionTypeRegistry.Default.TryGetDefinition(GuidType.Default.Name, out _),
                "This test assumes that nothing has registered the GUID extension type globally.");

            FlightStore store = new FlightStore();
            using TestWebFactory factory = new TestWebFactory(store);
            FlightClient client = new FlightClient(factory.GetChannel());
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("guids");

            RecordBatch batch = GuidBatch();
            FlightRecordBatchDuplexStreamingCall put = await client.StartPut(descriptor, batch.Schema);
            await put.RequestStream.WriteAsync(batch);
            await put.RequestStream.CompleteAsync();
            await put.ResponseStream.ToListAsync();

            RecordBatch received = store.Flights[descriptor].GetRecordBatches().Single().RecordBatch;
            Assert.IsType<FixedSizeBinaryArray>(received.Column(0));
        }

        [Fact]
        public async Task ClientContextRegistryResolvesExtensionTypesOnGet()
        {
            FlightStore store = new FlightStore();
            using TestWebFactory factory = new TestWebFactory(store);
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("guids");
            RecordBatch batch = GuidBatch();
            FlightHolder holder = new FlightHolder(descriptor, batch.Schema, factory.GetAddress());
            holder.AddBatch(new RecordBatchWithMetadata(batch));
            store.Flights[descriptor] = holder;

            FlightClient withContext = new FlightClient(factory.GetChannel(), GuidContext());
            FlightClient withoutContext = new FlightClient(factory.GetChannel());
            FlightTicket ticket = new FlightTicket(descriptor.Paths.Single());

            RecordBatch resolved = (await withContext.GetStream(ticket).ResponseStream.ToListAsync()).Single();
            RecordBatch unresolved = (await withoutContext.GetStream(ticket).ResponseStream.ToListAsync()).Single();

            Assert.Equal(s_guid, Assert.IsType<GuidArray>(resolved.Column(0)).GetGuid(0));
            Assert.IsType<FixedSizeBinaryArray>(unresolved.Column(0));
        }

        [Fact]
        public async Task ClientFactoryClientGetsTheRegisteredContext()
        {
            FlightStore store = new FlightStore();
            using TestWebFactory factory = new TestWebFactory(store);
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("guids");
            RecordBatch batch = GuidBatch();
            FlightHolder holder = new FlightHolder(descriptor, batch.Schema, factory.GetAddress());
            holder.AddBatch(new RecordBatchWithMetadata(batch));
            store.Flights[descriptor] = holder;

            // Grpc.Net.ClientFactory creates the client by reflection; the context comes from DI.
            IServiceCollection services = new ServiceCollection();
            services.AddSingleton(GuidContext());
            services.AddGrpcClient<FlightClient>(grpc => grpc.Address = new Uri(factory.GetAddress()));
            using ServiceProvider provider = services.BuildServiceProvider();
            FlightClient client = provider.GetRequiredService<FlightClient>();

            RecordBatch received = (await client.GetStream(new FlightTicket("guids")).ResponseStream.ToListAsync()).Single();

            Assert.Equal(s_guid, Assert.IsType<GuidArray>(received.Column(0)).GetGuid(0));
        }

        [Fact]
        public async Task ClientContextAllocatorIsUsedForWriterTemporaries()
        {
            FlightStore store = new FlightStore();
            using TestWebFactory factory = new TestWebFactory(store);
            LiveCountingAllocator allocator = new LiveCountingAllocator();
            FlightClient client = new FlightClient(factory.GetChannel(), new ArrowContext(allocator));
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("sliced");

            // A sliced string column makes the writer re-base its offsets into a new buffer.
            RecordBatch batch = SlicedStringBatch();
            FlightRecordBatchDuplexStreamingCall put = await client.StartPut(descriptor, batch.Schema);
            await put.RequestStream.WriteAsync(batch);
            await put.RequestStream.CompleteAsync();
            await put.ResponseStream.ToListAsync();

            Assert.True(allocator.Statistics.Allocations > 0, "The writer should have allocated from the context.");
            Assert.Equal(0, allocator.LiveAllocations);
            RecordBatch received = store.Flights[descriptor].GetRecordBatches().Single().RecordBatch;
            Assert.Equal("ccc", ((StringArray)received.Column(0)).GetString(1));
        }

        [Fact]
        public async Task ServerContextAllocatorIsUsedForWriterTemporaries()
        {
            FlightStore store = new FlightStore();
            LiveCountingAllocator allocator = new LiveCountingAllocator();
            using TestWebFactory factory = new TestWebFactory(store, new ArrowContext(allocator));
            FlightDescriptor descriptor = FlightDescriptor.CreatePathDescriptor("sliced");
            RecordBatch batch = SlicedStringBatch();
            FlightHolder holder = new FlightHolder(descriptor, batch.Schema, factory.GetAddress());
            holder.AddBatch(new RecordBatchWithMetadata(batch));
            store.Flights[descriptor] = holder;

            FlightClient client = new FlightClient(factory.GetChannel());
            RecordBatch received = (await client.GetStream(new FlightTicket("sliced")).ResponseStream.ToListAsync()).Single();

            Assert.True(allocator.Statistics.Allocations > 0, "The writer should have allocated from the context.");
            Assert.Equal(0, allocator.LiveAllocations);
            Assert.Equal("ccc", ((StringArray)received.Column(0)).GetString(1));
        }

        private static ArrowContext GuidContext()
        {
            ExtensionTypeRegistry registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);
            return new ArrowContext(extensionRegistry: registry);
        }

        private static RecordBatch GuidBatch()
        {
            GuidArray guids = new GuidArray.Builder().Append(s_guid).Build();
            return new RecordBatch.Builder().Append("id", false, guids).Build();
        }

        private static RecordBatch SlicedStringBatch()
        {
            StringArray full = new StringArray.Builder().Append("a").Append("bb").Append("ccc").Build();
            return new RecordBatch.Builder().Append("s", false, (StringArray)full.Slice(1, 2)).Build();
        }

        /// <summary>
        /// A caller-owned allocator over managed arrays that counts allocations not yet released.
        /// </summary>
        private sealed class LiveCountingAllocator : MemoryAllocator
        {
            private int _live;

            public int LiveAllocations => Volatile.Read(ref _live);

            public override AllocationLifetime Lifetime => AllocationLifetime.CallerOwned;

            protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                Interlocked.Increment(ref _live);
                bytesAllocated = length;
                return new Owner(this, new byte[length]);
            }

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly LiveCountingAllocator _allocator;
                private byte[] _buffer;

                public Owner(LiveCountingAllocator allocator, byte[] buffer)
                {
                    _allocator = allocator;
                    _buffer = buffer;
                }

                public Memory<byte> Memory => _buffer ?? throw new ObjectDisposedException(nameof(Owner));

                public void Dispose()
                {
                    if (Interlocked.Exchange(ref _buffer, null) != null)
                    {
                        Interlocked.Decrement(ref _allocator._live);
                    }
                }
            }
        }
    }
}
