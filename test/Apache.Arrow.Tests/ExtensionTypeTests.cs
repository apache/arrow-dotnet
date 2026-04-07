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
using Apache.Arrow.Arrays;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class ExtensionTypeTests
    {
        private static RecordBatch BuildGuidRecordBatch(Guid?[] values)
        {
            var builder = new GuidArray.Builder();

            foreach (var value in values)
            {
                if (value.HasValue)
                {
                    builder.Append(value.Value);
                }
                else
                {
                    builder.AppendNull();
                }
            }
            var guidArray = builder.Build();

            var field = new Field("guids", GuidType.Default, true);
            var schema = new Schema(new[] { field }, null);

            return new RecordBatch(schema, new[] { guidArray }, values.Length);
        }

        [Fact]
        public void ExtensionTypeProperties()
        {
            var guidType = new GuidType();
            Assert.Equal(ArrowTypeId.Extension, guidType.TypeId);
            Assert.Equal("arrow.uuid", guidType.Name);
            Assert.Equal("", guidType.ExtensionMetadata);
            Assert.True(guidType.IsFixedWidth);
            Assert.IsType<FixedSizeBinaryType>(guidType.StorageType);
            Assert.Equal(16, ((FixedSizeBinaryType)guidType.StorageType).ByteWidth);
        }

        [Fact]
        public void GuidArrayReadValues()
        {
            var guids = new Guid?[]
            {
                Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"),
                null,
                Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210"),
            };

            var batch = BuildGuidRecordBatch(guids);
            var array = (GuidArray)batch.Column(0);

            Assert.Equal(3, array.Length);
            Assert.Equal(guids[0], array.GetGuid(0));
            Assert.Null(array.GetGuid(1));
            Assert.True(array.IsNull(1));
            Assert.Equal(guids[2], array.GetGuid(2));
        }

        [Fact]
        public void GuidArraySlice()
        {
            var guids = new Guid?[]
            {
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
                Guid.NewGuid(),
            };

            var batch = BuildGuidRecordBatch(guids);
            var array = (GuidArray)batch.Column(0);

            var sliced = ArrowArrayFactory.Slice(array, 1, 2);
            Assert.IsType<GuidArray>(sliced);
            var slicedGuid = (GuidArray)sliced;
            Assert.Equal(2, slicedGuid.Length);
            Assert.Equal(guids[1], slicedGuid.GetGuid(0));
            Assert.Equal(guids[2], slicedGuid.GetGuid(1));
        }

        [Fact]
        public void IpcStreamRoundTrip()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);
            var context = new ArrowContext(extensionRegistry: registry);

            var guids = new Guid?[]
            {
                Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"),
                null,
                Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210"),
            };

            var batch = BuildGuidRecordBatch(guids);

            // Write
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
            stream.Position = 0;

            // Read with extension registry
            var reader = new ArrowStreamReader(context, stream);
            var readBatch = reader.ReadNextRecordBatch();

            Assert.NotNull(readBatch);
            Assert.Equal(3, readBatch.Length);

            var readArray = readBatch.Column(0);
            Assert.IsType<GuidArray>(readArray);

            var guidArray = (GuidArray)readArray;
            Assert.Equal(guids[0], guidArray.GetGuid(0));
            Assert.Null(guidArray.GetGuid(1));
            Assert.Equal(guids[2], guidArray.GetGuid(2));
        }

        [Fact]
        public void IpcStreamRoundTripWithoutRegistration()
        {
            // Without registering the extension type, the field should come back
            // as a FixedSizeBinaryArray (backwards compat)
            var guids = new Guid?[]
            {
                Guid.Parse("01234567-89ab-cdef-0123-456789abcdef"),
                null,
                Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210"),
            };

            var batch = BuildGuidRecordBatch(guids);

            // Write
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
            stream.Position = 0;

            // Read without extension registry
            var context = new ArrowContext(extensionRegistry: new ExtensionTypeRegistry());
            var reader = new ArrowStreamReader(context, stream);
            var readBatch = reader.ReadNextRecordBatch();

            Assert.NotNull(readBatch);
            var readArray = readBatch.Column(0);
            Assert.IsType<FixedSizeBinaryArray>(readArray);
            Assert.IsNotType<GuidArray>(readArray);
        }

        [Fact]
        public void ExtensionMetadataPreservedInRoundTrip()
        {
            var guids = new Guid?[] { Guid.NewGuid() };
            var batch = BuildGuidRecordBatch(guids);

            // Write
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
            stream.Position = 0;

            // Read without extension registry — metadata should still be on the field
            var context = new ArrowContext(extensionRegistry: new ExtensionTypeRegistry());
            var reader = new ArrowStreamReader(context, stream);
            var readBatch = reader.ReadNextRecordBatch();

            Assert.NotNull(readBatch);
            var field = readBatch.Schema.GetFieldByIndex(0);
            Assert.NotNull(field.Metadata);
            Assert.True(field.Metadata.ContainsKey("ARROW:extension:name"));
            Assert.Equal("arrow.uuid", field.Metadata["ARROW:extension:name"]);
        }

        [Fact]
        public void ContextIsolation()
        {
            // Two contexts with different registries should resolve differently
            var registry1 = new ExtensionTypeRegistry();
            registry1.Register(GuidExtensionDefinition.Instance);
            var context1 = new ArrowContext(extensionRegistry: registry1);

            var registry2 = new ExtensionTypeRegistry();
            // registry2 has no Guid definition registered
            var context2 = new ArrowContext(extensionRegistry: registry2);

            var guids = new Guid?[] { Guid.NewGuid() };
            var batch = BuildGuidRecordBatch(guids);

            var stream1 = new MemoryStream();
            var writer = new ArrowStreamWriter(stream1, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();

            // Read with context1 — should resolve as GuidArray
            stream1.Position = 0;
            var reader1 = new ArrowStreamReader(context1, stream1);
            var readBatch1 = reader1.ReadNextRecordBatch();
            Assert.IsType<GuidArray>(readBatch1.Column(0));

            // Read with context2 — should resolve as FixedSizeBinaryArray
            stream1.Position = 0;
            var reader2 = new ArrowStreamReader(context2, stream1);
            var readBatch2 = reader2.ReadNextRecordBatch();
            Assert.IsType<FixedSizeBinaryArray>(readBatch2.Column(0));
        }

        [Fact]
        public void ExtensionDefinitionRejectsWrongStorageType()
        {
            var def = GuidExtensionDefinition.Instance;

            // Should fail for wrong byte width
            Assert.False(def.TryCreateType(new FixedSizeBinaryType(8), "", out _));

            // Should fail for non-FixedSizeBinary type
            Assert.False(def.TryCreateType(Int32Type.Default, "", out _));

            // Should succeed for correct type
            Assert.True(def.TryCreateType(new FixedSizeBinaryType(16), "", out var extType));
            Assert.IsType<GuidType>(extType);
        }

        [Fact]
        public void ExtensionTypeRegistryClone()
        {
            var clone = ExtensionTypeRegistry.Default.Clone();
            clone.Register(GuidExtensionDefinition.Instance);

            Assert.True(clone.TryGetDefinition("arrow.uuid", out _));

            // Mutating the clone should not have affected the default
            Assert.False(ExtensionTypeRegistry.Default.TryGetDefinition("arrow.uuid", out _));
        }

        [Fact]
        public void ExtensionTypeRegistryScoped()
        {
            using (ExtensionTypeRegistry.Default.RegisterTemporary(GuidExtensionDefinition.Instance))
            {
                Assert.True(ExtensionTypeRegistry.Default.TryGetDefinition("arrow.uuid", out _));
            }
            Assert.False(ExtensionTypeRegistry.Default.TryGetDefinition("arrow.uuid", out _));
        }

        [Fact]
        public void ArrowArrayFactoryBuildExtension()
        {
            var guidType = new GuidType();
            var guid = Guid.NewGuid();

            var buffers = new[]
            {
                ArrowBuffer.Empty,
                new ArrowBuffer.Builder<byte>().Append(GuidArray.GuidToBytes(guid)).Build()
            };
            var data = new ArrayData(guidType, 1, 0, 0, buffers);

            var array = ArrowArrayFactory.BuildArray(data);
            Assert.IsType<GuidArray>(array);
            var guidArray = (GuidArray)array;
            Assert.Equal(guid, guidArray.GetGuid(0));
        }

        [Fact]
        public unsafe void CDataSchemaRoundTrip()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);

            var guidType = new GuidType();
            var field = new Field("uuid_field", guidType, true);
            var schema = new Schema(new[] { field }, null);

            // Export
            var cSchema = Apache.Arrow.C.CArrowSchema.Create();
            try
            {
                Apache.Arrow.C.CArrowSchemaExporter.ExportSchema(schema, cSchema);

                // Import with registry
                var importedSchema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(cSchema, registry);

                Assert.Single(importedSchema.FieldsList);
                Assert.Equal("uuid_field", importedSchema.FieldsList[0].Name);
                Assert.IsType<GuidType>(importedSchema.FieldsList[0].DataType);
            }
            finally
            {
                Apache.Arrow.C.CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CDataSchemaRoundTripWithoutRegistry()
        {
            var guidType = new GuidType();
            var field = new Field("uuid_field", guidType, true,
                new Dictionary<string, string>
                {
                    ["ARROW:extension:name"] = "arrow.uuid",
                    ["ARROW:extension:metadata"] = ""
                });
            var schema = new Schema(new[] { field }, null);

            // Export
            var cSchema = Apache.Arrow.C.CArrowSchema.Create();
            try
            {
                Apache.Arrow.C.CArrowSchemaExporter.ExportSchema(schema, cSchema);

                // Import without registry — should fall back to storage type
                var importedSchema = Apache.Arrow.C.CArrowSchemaImporter.ImportSchema(cSchema);

                Assert.Single(importedSchema.FieldsList);
                Assert.IsType<FixedSizeBinaryType>(importedSchema.FieldsList[0].DataType);
                Assert.Equal(16, ((FixedSizeBinaryType)importedSchema.FieldsList[0].DataType).ByteWidth);
            }
            finally
            {
                Apache.Arrow.C.CArrowSchema.Free(cSchema);
            }
        }
    }
}
