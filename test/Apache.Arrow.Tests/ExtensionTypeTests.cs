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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Arrays;
using Apache.Arrow.C;
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

        [Fact]
        public async Task CDataArrayStreamWithRegistry()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);

            var values = new Guid?[] { Guid.NewGuid(), null, Guid.NewGuid() };
            RecordBatch batch = BuildGuidRecordBatch(values);

            IArrowArrayStream imported;
            unsafe
            {
                CArrowArrayStream* cStream = CArrowArrayStream.Create();
                CArrowArrayStreamExporter.ExportArrayStream(new SingleBatchStream(batch), cStream);
                imported = CArrowArrayStreamImporter.ImportArrayStream(cStream, registry);
                CArrowArrayStream.Free(cStream);
            }

            using (imported)
            {
                Assert.IsType<GuidType>(imported.Schema.FieldsList[0].DataType);

                using RecordBatch importedBatch = await imported.ReadNextRecordBatchAsync();
                GuidArray guids = Assert.IsType<GuidArray>(importedBatch.Column(0));
                Assert.Equal(values, guids);

                Assert.Null(await imported.ReadNextRecordBatchAsync());
            }
        }

        [Fact]
        public async Task CDataArrayStreamWithoutRegistry()
        {
            RecordBatch batch = BuildGuidRecordBatch(new Guid?[] { Guid.NewGuid() });

            IArrowArrayStream imported;
            unsafe
            {
                CArrowArrayStream* cStream = CArrowArrayStream.Create();
                CArrowArrayStreamExporter.ExportArrayStream(new SingleBatchStream(batch), cStream);
                imported = CArrowArrayStreamImporter.ImportArrayStream(cStream);
                CArrowArrayStream.Free(cStream);
            }

            using (imported)
            {
                Field field = imported.Schema.FieldsList[0];
                Assert.IsType<FixedSizeBinaryType>(field.DataType);
                Assert.Equal("arrow.uuid", field.Metadata["ARROW:extension:name"]);

                using RecordBatch importedBatch = await imported.ReadNextRecordBatchAsync();
                Assert.IsType<FixedSizeBinaryArray>(importedBatch.Column(0));
            }
        }

        [Fact]
        public unsafe void CDataSchemaResolvesNestedExtensionTypes()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);

            Field Item() => new Field("item", GuidType.Default, true);
            var schema = new Schema(new[]
            {
                new Field("list", new ListType(Item()), true),
                new Field("large_list", new LargeListType(Item()), true),
                new Field("list_view", new ListViewType(Item()), true),
                new Field("large_list_view", new LargeListViewType(Item()), true),
                new Field("fixed_size_list", new FixedSizeListType(Item(), 2), true),
                new Field("list_of_list", new ListType(new Field("item", new ListType(Item()), true)), true),
                new Field("struct", new StructType(new[] { Item() }), true),
                new Field("map", new MapType(new Field("key", StringType.Default, false), Item()), true),
                new Field("union", new UnionType(new[] { Item() }, new[] { 0 }, UnionMode.Sparse), true),
                new Field("ree", new RunEndEncodedType(Int32Type.Default, GuidType.Default), true),
                new Field("dictionary", new DictionaryType(Int32Type.Default, new ListType(Item()), false), true),
            }, null);

            var cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportSchema(schema, cSchema);
                Schema imported = CArrowSchemaImporter.ImportSchema(cSchema, registry);

                Assert.Equal(schema.FieldsList.Count, imported.FieldsList.Count);
                foreach (Field field in imported.FieldsList)
                {
                    Assert.True(ContainsGuidType(field.DataType), $"Field '{field.Name}' did not resolve arrow.uuid: {field.DataType}");
                }
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }

            static bool ContainsGuidType(IArrowType type) => type switch
            {
                GuidType => true,
                DictionaryType dictionary => ContainsGuidType(dictionary.ValueType),
                NestedType nested => nested.Fields.Any(f => ContainsGuidType(f.DataType)),
                _ => false,
            };
        }

        [Fact]
        public async Task CDataArrayStreamResolvesListOfExtensionType()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);

            var values = new Guid?[] { Guid.NewGuid(), null, Guid.NewGuid() };
            GuidArray guids = (GuidArray)BuildGuidRecordBatch(values).Column(0);
            var listType = new ListType(new Field("item", GuidType.Default, true));
            var list = new ListArray(listType, 2,
                new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(3).Build(),
                guids, ArrowBuffer.Empty);
            var schema = new Schema(new[] { new Field("guid_lists", listType, true) }, null);
            RecordBatch batch = new RecordBatch(schema, new[] { list }, list.Length);

            IArrowArrayStream imported;
            unsafe
            {
                CArrowArrayStream* cStream = CArrowArrayStream.Create();
                CArrowArrayStreamExporter.ExportArrayStream(new SingleBatchStream(batch), cStream);
                imported = CArrowArrayStreamImporter.ImportArrayStream(cStream, registry);
                CArrowArrayStream.Free(cStream);
            }

            using (imported)
            {
                ListType importedType = Assert.IsType<ListType>(imported.Schema.FieldsList[0].DataType);
                Assert.IsType<GuidType>(importedType.ValueDataType);

                using RecordBatch importedBatch = await imported.ReadNextRecordBatchAsync();
                ListArray importedList = Assert.IsType<ListArray>(importedBatch.Column(0));
                Assert.IsType<GuidArray>(importedList.Values);
                Assert.Equal(new Guid?[] { values[0] }, Assert.IsType<GuidArray>(importedList.GetSlicedValues(0)));
                Assert.Equal(new Guid?[] { values[1], values[2] }, Assert.IsType<GuidArray>(importedList.GetSlicedValues(1)));
            }
        }

        private static readonly DictionaryType s_guidDictionaryType = new DictionaryType(Int32Type.Default, GuidType.Default, false);

        private static ExtensionTypeRegistry GuidRegistry()
        {
            var registry = new ExtensionTypeRegistry();
            registry.Register(GuidExtensionDefinition.Instance);
            return registry;
        }

        private static unsafe Dictionary<string, string> ReadMetadata(byte* metadata)
        {
            var result = new Dictionary<string, string>();
            if (metadata == null)
            {
                return result;
            }

            int count = *(int*)metadata;
            byte* ptr = metadata + 4;
            for (int i = 0; i < count; i++)
            {
                int keyLength = *(int*)ptr;
                string key = System.Text.Encoding.UTF8.GetString(ptr + 4, keyLength);
                ptr += 4 + keyLength;
                int valueLength = *(int*)ptr;
                string value = System.Text.Encoding.UTF8.GetString(ptr + 4, valueLength);
                ptr += 4 + valueLength;
                result[key] = value;
            }
            return result;
        }

        [Fact]
        public unsafe void CDataExportTypeIncludesExtensionMetadata()
        {
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportType(GuidType.Default, cSchema);

                Dictionary<string, string> metadata = ReadMetadata(cSchema->metadata);
                Assert.Equal("arrow.uuid", metadata["ARROW:extension:name"]);
                Assert.Equal("", metadata["ARROW:extension:metadata"]);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CDataExportDictionaryOfExtensionPutsMetadataOnDictionary()
        {
            var field = new Field("f", s_guidDictionaryType, true,
                new Dictionary<string, string> { ["other"] = "value" });

            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportField(field, cSchema);

                Assert.Equal(new Dictionary<string, string> { ["other"] = "value" }, ReadMetadata(cSchema->metadata));
                Dictionary<string, string> dictionaryMetadata = ReadMetadata(cSchema->dictionary->metadata);
                Assert.Equal("arrow.uuid", dictionaryMetadata["ARROW:extension:name"]);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CDataExportMovesValueExtensionMetadataFromDictionaryField()
        {
            // Earlier versions of the IPC reader left a dictionary value type's extension metadata
            // on the field.
            var field = new Field("f", s_guidDictionaryType, true,
                new Dictionary<string, string>
                {
                    ["ARROW:extension:name"] = "arrow.uuid",
                    ["ARROW:extension:metadata"] = "",
                    ["other"] = "value",
                });

            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportField(field, cSchema);

                Assert.Equal(new Dictionary<string, string> { ["other"] = "value" }, ReadMetadata(cSchema->metadata));
                Assert.Equal(
                    new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid", ["ARROW:extension:metadata"] = "" },
                    ReadMetadata(cSchema->dictionary->metadata));
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CDataExportKeepsUnresolvedDictionaryFieldExtensionMetadata()
        {
            // Without a resolved value type, extension metadata on a dictionary field means
            // the extension's storage is the whole dictionary, so it must stay on the field.
            var metadata = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = "test.dict_storage",
                ["ARROW:extension:metadata"] = "",
            };
            var field = new Field("f", new DictionaryType(Int32Type.Default, StringType.Default, false), true, metadata);

            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportField(field, cSchema);

                Assert.Equal(metadata, ReadMetadata(cSchema->metadata));
                Assert.Empty(ReadMetadata(cSchema->dictionary->metadata));
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [Fact]
        public unsafe void CDataDictionaryStorageExtensionPassThroughWithoutRegistry()
        {
            // Arrow C++ exports an extension type whose storage is a dictionary with its
            // metadata on the field. Importing it without the definition and exporting it
            // again must not move that metadata to the dictionary values.
            var metadata = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = "test.dict_storage",
                ["ARROW:extension:metadata"] = "",
            };
            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchema* cMetadataSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportField(new Field("f", new DictionaryType(Int32Type.Default, StringType.Default, false), true), cSchema);
            CArrowSchemaExporter.ExportField(new Field("m", Int32Type.Default, true, metadata), cMetadataSchema);
            byte* swapped = cSchema->metadata;
            cSchema->metadata = cMetadataSchema->metadata;
            cMetadataSchema->metadata = swapped;
            CArrowSchema.Free(cMetadataSchema);

            Field imported = CArrowSchemaImporter.ImportField(cSchema, GuidRegistry());
            CArrowSchema.Free(cSchema);
            Assert.Equal(metadata, imported.Metadata);

            CArrowSchema* cReexported = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportField(imported, cReexported);

                Assert.Equal(metadata, ReadMetadata(cReexported->metadata));
                Assert.Empty(ReadMetadata(cReexported->dictionary->metadata));
            }
            finally
            {
                CArrowSchema.Free(cReexported);
            }
        }

        [Fact]
        public unsafe void CDataDictionaryOfExtensionRoundTrip()
        {
            var field = new Field("f", s_guidDictionaryType, true);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportField(field, cSchema);
            Field imported = CArrowSchemaImporter.ImportField(cSchema, GuidRegistry());
            CArrowSchema.Free(cSchema);

            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(imported.DataType);
            Assert.IsType<Int32Type>(dictionaryType.IndexType);
            Assert.IsType<GuidType>(dictionaryType.ValueType);
            Assert.Null(imported.Metadata);
        }

        [Fact]
        public unsafe void CDataDictionaryOfExtensionRoundTripWithoutRegistry()
        {
            var field = new Field("f", s_guidDictionaryType, true);

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportField(field, cSchema);
            Field imported = CArrowSchemaImporter.ImportField(cSchema);
            CArrowSchema.Free(cSchema);

            // Like Arrow C++, the unresolved value type's extension metadata is dropped rather
            // than moved to the field, where it would describe the whole dictionary.
            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(imported.DataType);
            Assert.IsType<FixedSizeBinaryType>(dictionaryType.ValueType);
            Assert.Null(imported.Metadata);
        }

        [Fact]
        public unsafe void CDataImportDoesNotApplyDictionaryFieldExtensionMetadataToValues()
        {
            // On a dictionary field, extension metadata describes the whole dictionary, so as in
            // Arrow C++ a field-level arrow.uuid doesn't resolve. (Older versions of this library
            // exported a value type's extension metadata this way.) Build such a schema by
            // swapping in the metadata of another export.
            var storageField = new Field("f", new DictionaryType(Int32Type.Default, new FixedSizeBinaryType(16), false), true);
            var metadataField = new Field("m", Int32Type.Default, true,
                new Dictionary<string, string>
                {
                    ["ARROW:extension:name"] = "arrow.uuid",
                    ["ARROW:extension:metadata"] = "",
                });

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowSchema* cMetadataSchema = CArrowSchema.Create();
            CArrowSchemaExporter.ExportField(storageField, cSchema);
            CArrowSchemaExporter.ExportField(metadataField, cMetadataSchema);
            byte* metadata = cSchema->metadata;
            cSchema->metadata = cMetadataSchema->metadata;
            cMetadataSchema->metadata = metadata;
            CArrowSchema.Free(cMetadataSchema);

            Field imported = CArrowSchemaImporter.ImportField(cSchema, GuidRegistry());
            CArrowSchema.Free(cSchema);

            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(imported.DataType);
            Assert.IsType<FixedSizeBinaryType>(dictionaryType.ValueType);
            Assert.Equal("arrow.uuid", imported.Metadata["ARROW:extension:name"]);
        }

        [Fact]
        public unsafe void CDataDictionaryOfExtensionArrayRoundTrip()
        {
            var values = new Guid?[] { Guid.NewGuid(), Guid.NewGuid() };
            GuidArray dictionary = (GuidArray)BuildGuidRecordBatch(values).Column(0);
            Int32Array indices = new Int32Array.Builder().Append(1).AppendNull().Append(0).Append(1).Build();
            var array = new DictionaryArray(s_guidDictionaryType, indices, dictionary);
            var schema = new Schema(new[] { new Field("f", s_guidDictionaryType, true) }, null);
            var batch = new RecordBatch(schema, new[] { array }, array.Length);

            CArrowArray* cArray = CArrowArray.Create();
            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowArrayExporter.ExportRecordBatch(batch, cArray);
            CArrowSchemaExporter.ExportSchema(schema, cSchema);

            Schema importedSchema = CArrowSchemaImporter.ImportSchema(cSchema, GuidRegistry());
            using RecordBatch importedBatch = CArrowArrayImporter.ImportRecordBatch(cArray, importedSchema);
            CArrowSchema.Free(cSchema);
            CArrowArray.Free(cArray);

            DictionaryArray importedArray = Assert.IsType<DictionaryArray>(importedBatch.Column(0));
            Assert.Equal(values, Assert.IsType<GuidArray>(importedArray.Dictionary));
            Assert.Equal(new int?[] { 1, null, 0, 1 }, (Int32Array)importedArray.Indices);
        }

        [Fact]
        public void IpcDictionaryOfExtensionWritesValueMetadataToDictionaryEncoding()
        {
            // Earlier versions of the IPC reader left a dictionary value type's extension metadata
            // on the field.
            var field = new Field("f", s_guidDictionaryType, true,
                new Dictionary<string, string>
                {
                    ["ARROW:extension:name"] = "arrow.uuid",
                    ["ARROW:extension:metadata"] = "",
                    ["other"] = "value",
                });

            Flatbuf.Field written = WriteSchemaField(field);

            Assert.Equal(Flatbuf.Type.FixedSizeBinary, written.TypeType);
            Assert.Equal(new Dictionary<string, string> { ["other"] = "value" }, ReadFieldMetadata(written));
            Assert.Equal(
                new Dictionary<string, string> { ["ARROW:extension:name"] = "arrow.uuid", ["ARROW:extension:metadata"] = "" },
                ReadDictionaryMetadata(written));
        }

        [Fact]
        public void IpcDictionaryOfExtensionRoundTrip()
        {
            var values = new Guid?[] { Guid.NewGuid(), Guid.NewGuid() };
            RecordBatch batch = BuildGuidDictionaryRecordBatch(values);

            RecordBatch readBatch = IpcRoundTrip(batch, GuidRegistry());

            Field field = readBatch.Schema.GetFieldByIndex(0);
            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(field.DataType);
            Assert.IsType<Int32Type>(dictionaryType.IndexType);
            Assert.IsType<GuidType>(dictionaryType.ValueType);
            Assert.Null(field.Metadata);

            DictionaryArray readArray = Assert.IsType<DictionaryArray>(readBatch.Column(0));
            Assert.Equal(values, Assert.IsType<GuidArray>(readArray.Dictionary));
            Assert.Equal(new int?[] { 1, null, 0, 1 }, (Int32Array)readArray.Indices);
        }

        [Fact]
        public void IpcDictionaryOfExtensionRoundTripWithoutRegistry()
        {
            RecordBatch batch = BuildGuidDictionaryRecordBatch(new Guid?[] { Guid.NewGuid(), Guid.NewGuid() });

            RecordBatch readBatch = IpcRoundTrip(batch, new ExtensionTypeRegistry());

            // As in Arrow C++, an unresolved value type's metadata is dropped rather than moved
            // to the field, where it would describe the whole dictionary.
            Field field = readBatch.Schema.GetFieldByIndex(0);
            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(field.DataType);
            Assert.IsType<FixedSizeBinaryType>(dictionaryType.ValueType);
            Assert.Null(field.Metadata);
            Assert.IsType<FixedSizeBinaryArray>(((DictionaryArray)readBatch.Column(0)).Dictionary);
        }

        [Fact]
        public void IpcNestedDictionaryOfExtensionRoundTrip()
        {
            var values = new Guid?[] { Guid.NewGuid(), Guid.NewGuid() };
            DictionaryArray dictionaryArray = (DictionaryArray)BuildGuidDictionaryRecordBatch(values).Column(0);
            var listType = new ListType(new Field("item", s_guidDictionaryType, true));
            ArrowBuffer offsets = new ArrowBuffer.Builder<int>().Append(0).Append(1).Append(4).Build();
            var listArray = new ListArray(listType, 2, offsets, dictionaryArray, ArrowBuffer.Empty);
            var schema = new Schema(new[] { new Field("f", listType, true) }, null);
            var batch = new RecordBatch(schema, new[] { listArray }, listArray.Length);

            RecordBatch readBatch = IpcRoundTrip(batch, GuidRegistry());

            ListType readListType = Assert.IsType<ListType>(readBatch.Schema.GetFieldByIndex(0).DataType);
            DictionaryType dictionaryType = Assert.IsType<DictionaryType>(readListType.ValueDataType);
            Assert.IsType<GuidType>(dictionaryType.ValueType);
            Assert.Null(readListType.ValueField.Metadata);

            ListArray readList = Assert.IsType<ListArray>(readBatch.Column(0));
            DictionaryArray readValues = Assert.IsType<DictionaryArray>(readList.Values);
            Assert.Equal(values, Assert.IsType<GuidArray>(readValues.Dictionary));
            Assert.Equal(new int?[] { 1, null, 0, 1 }, (Int32Array)readValues.Indices);
        }

        [Fact]
        public void IpcDictionaryFieldExtensionMetadataDescribesWholeDictionary()
        {
            // Writers before format 1.6, including earlier versions of this library, put a
            // dictionary value type's extension metadata on the field. Like Arrow C++, it is
            // resolved against the whole dictionary type, not the values.
            var metadata = new Dictionary<string, string>
            {
                ["ARROW:extension:name"] = "arrow.uuid",
                ["ARROW:extension:metadata"] = "",
            };
            var dictionaryType = new DictionaryType(Int32Type.Default, new FixedSizeBinaryType(16), false);
            var field = new Field("f", dictionaryType, true, metadata);
            var storage = (FixedSizeBinaryArray)((GuidArray)BuildGuidRecordBatch(new Guid?[] { Guid.NewGuid() }).Column(0)).Storage;
            Int32Array indices = new Int32Array.Builder().Append(0).AppendNull().Build();
            var array = new DictionaryArray(dictionaryType, indices, storage);
            var batch = new RecordBatch(new Schema(new[] { field }, null), new[] { array }, array.Length);

            Flatbuf.Field written = WriteSchemaField(field);
            Assert.Equal(metadata, ReadFieldMetadata(written));
            Assert.Empty(ReadDictionaryMetadata(written));

            RecordBatch readBatch = IpcRoundTrip(batch, GuidRegistry());

            Field readField = readBatch.Schema.GetFieldByIndex(0);
            DictionaryType readType = Assert.IsType<DictionaryType>(readField.DataType);
            Assert.IsType<FixedSizeBinaryType>(readType.ValueType);
            Assert.Equal(metadata, readField.Metadata);
        }

        [Fact]
        public void IpcDictionaryStorageExtensionRoundTrip()
        {
            var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
            var extensionType = new DictionaryStorageType(dictionaryType);
            var field = new Field("f", extensionType, true);
            StringArray dictionary = new StringArray.Builder().Append("a").Append("b").Build();
            Int32Array indices = new Int32Array.Builder().Append(1).AppendNull().Append(0).Build();
            var array = extensionType.CreateArray(new DictionaryArray(dictionaryType, indices, dictionary));
            var batch = new RecordBatch(new Schema(new[] { field }, null), new[] { array }, array.Length);

            // An extension type whose storage is a dictionary keeps its metadata on the field
            Flatbuf.Field written = WriteSchemaField(field);
            Assert.Equal(
                new Dictionary<string, string> { ["ARROW:extension:name"] = DictionaryStorageType.ExtensionName, ["ARROW:extension:metadata"] = "" },
                ReadFieldMetadata(written));
            Assert.Empty(ReadDictionaryMetadata(written));

            var registry = new ExtensionTypeRegistry();
            registry.Register(DictionaryStorageDefinition.Instance);
            RecordBatch readBatch = IpcRoundTrip(batch, registry);

            DictionaryStorageType readType = Assert.IsType<DictionaryStorageType>(readBatch.Schema.GetFieldByIndex(0).DataType);
            Assert.IsType<StringType>(Assert.IsType<DictionaryType>(readType.StorageType).ValueType);
            ExtensionArray readArray = Assert.IsAssignableFrom<ExtensionArray>(readBatch.Column(0));
            DictionaryArray readStorage = Assert.IsType<DictionaryArray>(readArray.Storage);
            StringArray readDictionary = Assert.IsType<StringArray>(readStorage.Dictionary);
            Assert.Equal(new[] { "a", "b" }, new[] { readDictionary.GetString(0), readDictionary.GetString(1) });
            Assert.Equal(new int?[] { 1, null, 0 }, (Int32Array)readStorage.Indices);
        }

        private static RecordBatch BuildGuidDictionaryRecordBatch(Guid?[] values)
        {
            GuidArray dictionary = (GuidArray)BuildGuidRecordBatch(values).Column(0);
            Int32Array indices = new Int32Array.Builder().Append(1).AppendNull().Append(0).Append(1).Build();
            var array = new DictionaryArray(s_guidDictionaryType, indices, dictionary);
            var schema = new Schema(new[] { new Field("f", s_guidDictionaryType, true) }, null);
            return new RecordBatch(schema, new[] { array }, array.Length);
        }

        private static RecordBatch IpcRoundTrip(RecordBatch batch, ExtensionTypeRegistry registry)
        {
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, batch.Schema);
            writer.WriteRecordBatch(batch);
            writer.WriteEnd();
            stream.Position = 0;

            var reader = new ArrowStreamReader(new ArrowContext(extensionRegistry: registry), stream);
            RecordBatch readBatch = reader.ReadNextRecordBatch();
            Assert.NotNull(readBatch);
            return readBatch;
        }

        private static Flatbuf.Field WriteSchemaField(Field field)
        {
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, new Schema(new[] { field }, null));
            writer.WriteStart();

            // Skip the continuation marker and the metadata length
            var message = Flatbuf.Message.GetRootAsMessage(new Google.FlatBuffers.ByteBuffer(stream.ToArray(), 8));
            return message.Header<Flatbuf.Schema>().Value.Fields(0).Value;
        }

        private static Dictionary<string, string> ReadFieldMetadata(Flatbuf.Field field)
        {
            var result = new Dictionary<string, string>();
            for (int i = 0; i < field.CustomMetadataLength; i++)
            {
                Flatbuf.KeyValue keyValue = field.CustomMetadata(i).Value;
                result[keyValue.Key] = keyValue.Value;
            }
            return result;
        }

        private static Dictionary<string, string> ReadDictionaryMetadata(Flatbuf.Field field)
        {
            Flatbuf.DictionaryEncoding encoding = field.Dictionary.Value;
            var result = new Dictionary<string, string>();
            for (int i = 0; i < encoding.CustomMetadataLength; i++)
            {
                Flatbuf.KeyValue keyValue = encoding.CustomMetadata(i).Value;
                result[keyValue.Key] = keyValue.Value;
            }
            return result;
        }

        private sealed class DictionaryStorageType : ExtensionType
        {
            public const string ExtensionName = "test.dict_storage";

            public DictionaryStorageType(IArrowType storageType) : base(storageType) { }

            public override string Name => ExtensionName;
            public override string ExtensionMetadata => "";
            public override ExtensionArray CreateArray(IArrowArray storageArray) => new ExtensionArray(this, storageArray);
        }

        private sealed class DictionaryStorageDefinition : ExtensionDefinition
        {
            public static readonly DictionaryStorageDefinition Instance = new DictionaryStorageDefinition();

            public override string ExtensionName => DictionaryStorageType.ExtensionName;

            public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
            {
                type = storageType is DictionaryType ? new DictionaryStorageType(storageType) : null;
                return type != null;
            }
        }

        private sealed class SingleBatchStream : IArrowArrayStream
        {
            private RecordBatch _batch;

            public SingleBatchStream(RecordBatch batch)
            {
                Schema = batch.Schema;
                _batch = batch;
            }

            public Schema Schema { get; }

            public ValueTask<RecordBatch> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                RecordBatch result = _batch;
                _batch = null;
                return new ValueTask<RecordBatch>(result);
            }

            public void Dispose()
            {
                _batch?.Dispose();
                _batch = null;
            }
        }
    }
}
