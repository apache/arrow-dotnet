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
using Apache.Arrow.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// The Arrow format allows a struct with no fields; other implementations (C++, Rust, Java) create and
    /// exchange them.
    /// </summary>
    public class EmptyStructTests
    {
        private static readonly StructType EmptyStruct = new StructType(new List<Field>());

        /// <summary>Three rows, the second null.</summary>
        private static StructArray EmptyStructArray()
        {
            ArrowBuffer validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build();
            return new StructArray(EmptyStruct, 3, new List<IArrowArray>(), validity, nullCount: 1);
        }

        private static RecordBatch Batch()
        {
            var listType = new ListType(new Field("item", EmptyStruct, true));
            var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Append(2).Append(3).Build();
            var list = new ListArray(listType, 3, offsets, EmptyStructArray(), ArrowBuffer.Empty, 0);

            var schema = new Schema.Builder()
                .Field(f => f.Name("empty").DataType(EmptyStruct).Nullable(true))
                .Field(f => f.Name("list_of_empty").DataType(listType).Nullable(true))
                .Field(f => f.Name("ints").DataType(Int32Type.Default).Nullable(false))
                .Build();
            var ints = new Int32Array.Builder().Append(1).Append(2).Append(3).Build();
            return new RecordBatch(schema, new IArrowArray[] { EmptyStructArray(), list, ints }, 3);
        }

        [Fact]
        public void AStructTypeMayHaveNoFields()
        {
            Assert.Empty(EmptyStruct.Fields);
            Assert.Equal(-1, EmptyStruct.GetFieldIndex("a"));
            Assert.Null(EmptyStruct.GetFieldByName("a"));
        }

        [Fact]
        public void NullFieldsAreStillRefused()
        {
            Assert.Throws<ArgumentNullException>(() => new StructType(null));
        }

        [Fact]
        public void AStructArrayMayHaveNoChildren()
        {
            StructArray array = EmptyStructArray();
            Assert.Equal(3, array.Length);
            Assert.Empty(array.Fields);
            Assert.True(array.IsValid(0));
            Assert.True(array.IsNull(1));

            var slice = (StructArray)array.Slice(1, 2);
            Assert.Equal(2, slice.Length);
            Assert.True(slice.IsNull(0));
            Assert.True(slice.IsValid(1));
        }

        [Fact]
        public void RoundTripsThroughTheStreamFormat()
        {
            RecordBatch original = Batch();
            using var stream = new MemoryStream();
            using (var writer = new ArrowStreamWriter(stream, original.Schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            stream.Position = 0;
            using var reader = new ArrowStreamReader(stream);
            RecordBatch read = reader.ReadNextRecordBatch();
            ArrowReaderVerifier.CompareBatches(original, read);
            Assert.Empty(((StructType)read.Schema.GetFieldByName("empty").DataType).Fields);
        }

        [Fact]
        public void RoundTripsThroughTheFileFormat()
        {
            RecordBatch original = Batch();
            using var stream = new MemoryStream();
            using (var writer = new ArrowFileWriter(stream, original.Schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            stream.Position = 0;
            using var reader = new ArrowFileReader(stream);
            ArrowReaderVerifier.CompareBatches(original, reader.ReadNextRecordBatch());
        }

        [Fact]
        public unsafe void RoundTripsThroughTheCDataInterface()
        {
            RecordBatch original = Batch();

            CArrowSchema* cSchema = CArrowSchema.Create();
            CArrowArray* cArray = CArrowArray.Create();
            try
            {
                CArrowSchemaExporter.ExportSchema(original.Schema, cSchema);
                CArrowArrayExporter.ExportRecordBatch(original, cArray);

                Schema schema = CArrowSchemaImporter.ImportSchema(cSchema);
                using RecordBatch imported = CArrowArrayImporter.ImportRecordBatch(cArray, schema);
                // Not strict: an imported buffer is only as long as the data, not padded as a built one is.
                ArrowReaderVerifier.CompareBatches(original, imported, strictCompare: false);
                Assert.Empty(((StructType)schema.GetFieldByName("empty").DataType).Fields);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
                CArrowArray.Free(cArray);
            }
        }

        [Fact]
        public void Concatenates()
        {
            var result = (StructArray)ArrowArrayConcatenator.Concatenate(
                new List<IArrowArray> { EmptyStructArray(), EmptyStructArray() });
            Assert.Equal(6, result.Length);
            Assert.Equal(2, result.NullCount);
            Assert.True(result.IsNull(4));
            Assert.Empty(result.Fields);
        }

        [Fact]
        public void AUnionTypeMayHaveNoFields()
        {
            var union = new UnionType(new List<Field>(), new int[0], UnionMode.Sparse);
            Assert.Empty(union.Fields);
        }

        /// <summary>The exporter writes "+us:" or "+ud:"; the importer required at least one type id.</summary>
        [Theory]
        [InlineData(UnionMode.Sparse)]
        [InlineData(UnionMode.Dense)]
        public unsafe void AnEmptyUnionTypeRoundTripsThroughTheCDataInterface(UnionMode mode)
        {
            var union = new UnionType(new List<Field>(), new int[0], mode);
            CArrowSchema* cSchema = CArrowSchema.Create();
            try
            {
                CArrowSchemaExporter.ExportType(union, cSchema);
                var imported = (UnionType)CArrowSchemaImporter.ImportType(cSchema);
                Assert.Equal(mode, imported.Mode);
                Assert.Empty(imported.Fields);
                Assert.Empty(imported.TypeIds);
            }
            finally
            {
                CArrowSchema.Free(cSchema);
            }
        }

        [Theory]
        [InlineData(UnionMode.Sparse)]
        [InlineData(UnionMode.Dense)]
        public void AnEmptyUnionColumnRoundTripsThroughTheStreamFormat(UnionMode mode)
        {
            var union = new UnionType(new List<Field>(), new int[0], mode);
            var schema = new Schema.Builder().Field(f => f.Name("u").DataType(union).Nullable(false)).Build();
            IArrowArray column = mode == UnionMode.Sparse
                ? new SparseUnionArray(union, 0, new List<IArrowArray>(), ArrowBuffer.Empty)
                : new DenseUnionArray(union, 0, new List<IArrowArray>(), ArrowBuffer.Empty, ArrowBuffer.Empty);
            var original = new RecordBatch(schema, new[] { column }, 0);

            using var stream = new MemoryStream();
            using (var writer = new ArrowStreamWriter(stream, schema, leaveOpen: true))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            stream.Position = 0;
            using var reader = new ArrowStreamReader(stream);
            RecordBatch read = reader.ReadNextRecordBatch();
            var readUnion = (UnionType)read.Schema.GetFieldByName("u").DataType;
            Assert.Equal(mode, readUnion.Mode);
            Assert.Empty(readUnion.Fields);
            Assert.Equal(0, read.Length);
        }

        /// <summary>
        /// The schema format makes a union's typeIds optional: without it, the ids in the type vector are the
        /// children's offsets. Reading a union without it threw ArgumentNullException, with children or without.
        /// </summary>
        [Theory]
        [InlineData(0)]
        [InlineData(2)]
        public void AUnionWithoutTypeIds_TakesTheChildOffsets(int childCount)
        {
            var builder = new Google.FlatBuffers.FlatBufferBuilder(256);

            var children = new Google.FlatBuffers.Offset<Flatbuf.Field>[childCount];
            for (int i = 0; i < childCount; i++)
            {
                var childName = builder.CreateString("c" + i);
                var intType = Flatbuf.Int.CreateInt(builder, 32, true);
                children[i] = Flatbuf.Field.CreateField(builder, childName, true, Flatbuf.Type.Int, intType.Value);
            }
            var childrenVector = Flatbuf.Field.CreateChildrenVector(builder, children);

            var name = builder.CreateString("u");
            var union = Flatbuf.Union.CreateUnion(builder, Flatbuf.UnionMode.Dense); // no typeIds vector
            var field = Flatbuf.Field.CreateField(
                builder, name, false, Flatbuf.Type.Union, union.Value, childrenOffset: childrenVector);
            var fields = Flatbuf.Schema.CreateFieldsVector(builder, new[] { field });
            builder.Finish(Flatbuf.Schema.CreateSchema(builder, fieldsOffset: fields).Value);

            var flatbufSchema = Flatbuf.Schema.GetRootAsSchema(builder.DataBuffer);
            Assert.Equal(0, flatbufSchema.Fields(0).Value.Type<Flatbuf.Union>().Value.TypeIdsLength);

            DictionaryMemo memo = null;
            Schema schema = MessageSerializer.GetSchema(flatbufSchema, ref memo);
            var type = (UnionType)schema.FieldsList[0].DataType;
            Assert.Equal(UnionMode.Dense, type.Mode);
            Assert.Equal(childCount, type.Fields.Count);
            Assert.Equal(System.Linq.Enumerable.Range(0, childCount), type.TypeIds);
        }
    }
}
