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
using Apache.Arrow.Ipc;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    public class VariantArrayTests
    {
        [Fact]
        public void BuildEmptyArray()
        {
            var builder = new VariantArray.Builder();
            var array = builder.Build();

            Assert.Empty(array);
        }

        [Fact]
        public void BuildSinglePrimitiveValue()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(42));
            var array = builder.Build();

            Assert.Equal(1, array.Length);
            Assert.Equal(0, array.NullCount);

            var value = array.GetVariantValue(0);
            Assert.Equal(42, value.AsInt32());
        }

        [Fact]
        public void BuildMultiplePrimitiveTypes()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(1));
            builder.Append(VariantValue.FromString("hello"));
            builder.Append(VariantValue.True);
            builder.Append(VariantValue.FromDouble(3.14));
            builder.Append(VariantValue.FromInt64(long.MaxValue));
            var array = builder.Build();

            Assert.Equal(5, array.Length);
            Assert.Equal(0, array.NullCount);

            Assert.Equal(1, array.GetVariantValue(0).AsInt32());
            Assert.Equal("hello", array.GetVariantValue(1).AsString());
            Assert.True(array.GetVariantValue(2).AsBoolean());
            Assert.Equal(3.14, array.GetVariantValue(3).AsDouble());
            Assert.Equal(long.MaxValue, array.GetVariantValue(4).AsInt64());
        }

        [Fact]
        public void StructNullVsVariantNull()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.Null);   // variant-encoded null (valid slot)
            builder.AppendNull();                  // struct-level null (invalid slot)
            var array = builder.Build();

            Assert.Equal(2, array.Length);
            Assert.Equal(1, array.NullCount);

            // Row 0: valid slot containing variant null
            Assert.False(array.IsNull(0));
            var v0 = array.GetVariantValue(0);
            Assert.True(v0.IsNull);

            // Row 1: struct-level null
            Assert.True(array.IsNull(1));
            Assert.True(array.GetVariantValue(1).IsNull);
        }

        [Fact]
        public void StructNullDoesNotCreateChildNulls()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(1));
            builder.AppendNull();
            builder.Append(VariantValue.FromString("test"));
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);

            // Child arrays should have zero nulls (placeholder bytes for struct null)
            var structArray = array.StorageArray;
            Assert.Equal(0, structArray.Fields[0].NullCount);
            Assert.Equal(0, structArray.Fields[1].NullCount);
        }

        [Fact]
        public void NullableAppend()
        {
            var builder = new VariantArray.Builder();
            builder.Append((VariantValue?)VariantValue.FromInt32(1));
            builder.Append((VariantValue?)null);
            builder.Append((VariantValue?)VariantValue.FromString("test"));
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);

            Assert.Equal(1, array.GetVariantValue(0).AsInt32());
            Assert.True(array.GetVariantValue(1).IsNull);
            Assert.Equal("test", array.GetVariantValue(2).AsString());
        }

        [Fact]
        public void AppendRangeNonNullable()
        {
            var values = new[]
            {
                VariantValue.FromInt32(10),
                VariantValue.FromString("abc"),
                VariantValue.False,
            };

            var builder = new VariantArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(10, array.GetVariantValue(0).AsInt32());
            Assert.Equal("abc", array.GetVariantValue(1).AsString());
            Assert.False(array.GetVariantValue(2).AsBoolean());
        }

        [Fact]
        public void AppendRangeNullable()
        {
            var values = new VariantValue?[]
            {
                VariantValue.FromInt32(10),
                null,
                VariantValue.FromString("abc"),
            };

            var builder = new VariantArray.Builder();
            builder.AppendRange(values);
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);
            Assert.Equal(10, array.GetVariantValue(0).AsInt32());
            Assert.True(array.GetVariantValue(1).IsNull);
            Assert.Equal("abc", array.GetVariantValue(2).AsString());
        }

        [Fact]
        public void AppendRangeThrowsOnNull()
        {
            var builder = new VariantArray.Builder();
            Assert.Throws<ArgumentNullException>(() =>
                builder.AppendRange((IEnumerable<VariantValue>)null));
            Assert.Throws<ArgumentNullException>(() =>
                builder.AppendRange((IEnumerable<VariantValue?>)null));
        }

        [Fact]
        public void AppendRawBytes()
        {
            // Encode a value manually, then append raw bytes
            var encoder = new VariantBuilder();
            var (metadata, value) = encoder.Encode(VariantValue.FromInt32(99));

            var builder = new VariantArray.Builder();
            builder.Append((ReadOnlySpan<byte>)metadata, (ReadOnlySpan<byte>)value);
            var array = builder.Build();

            Assert.Equal(1, array.Length);
            Assert.Equal(99, array.GetVariantValue(0).AsInt32());
        }

        [Fact]
        public void ComplexVariantObject()
        {
            var obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["name"] = VariantValue.FromString("Alice"),
                ["age"] = VariantValue.FromInt32(30),
                ["active"] = VariantValue.True,
            });

            var builder = new VariantArray.Builder();
            builder.Append(obj);
            var array = builder.Build();

            Assert.Equal(1, array.Length);
            var result = array.GetVariantValue(0);
            Assert.True(result.IsObject);
            var fields = result.AsObject();
            Assert.Equal("Alice", fields["name"].AsString());
            Assert.Equal(30, fields["age"].AsInt32());
            Assert.True(fields["active"].AsBoolean());
        }

        [Fact]
        public void ComplexVariantArray()
        {
            var variantArray = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(3)
            );

            var builder = new VariantArray.Builder();
            builder.Append(variantArray);
            var array = builder.Build();

            Assert.Equal(1, array.Length);
            var result = array.GetVariantValue(0);
            Assert.True(result.IsArray);
            var elements = result.AsArray();
            Assert.Equal(3, elements.Count);
            Assert.Equal(1, elements[0].AsInt32());
            Assert.Equal(2, elements[1].AsInt32());
            Assert.Equal(3, elements[2].AsInt32());
        }

        [Fact]
        public void NestedObjectWithArray()
        {
            var obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["tags"] = VariantValue.FromArray(
                    VariantValue.FromString("a"),
                    VariantValue.FromString("b")
                ),
                ["count"] = VariantValue.FromInt32(2),
            });

            var builder = new VariantArray.Builder();
            builder.Append(obj);
            var array = builder.Build();

            var result = array.GetVariantValue(0);
            Assert.True(result.IsObject);
            var fields = result.AsObject();
            Assert.True(fields["tags"].IsArray);
            Assert.Equal(2, fields["tags"].AsArray().Count);
        }

        [Fact]
        public void GetVariantReaderRoundTrip()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromString("test-reader"));
            builder.Append(VariantValue.FromInt32(42));
            var array = builder.Build();

            var reader0 = array.GetVariantReader(0);
            Assert.Equal("test-reader", reader0.ToVariantValue().AsString());

            var reader1 = array.GetVariantReader(1);
            Assert.Equal(42, reader1.ToVariantValue().AsInt32());
        }

        [Fact]
        public void GetVariantReaderThrowsOnNull()
        {
            var builder = new VariantArray.Builder();
            builder.AppendNull();
            var array = builder.Build();

            Assert.Throws<InvalidOperationException>(() => array.GetVariantReader(0));
        }

        [Fact]
        public void GetVariantReaderThrowsOnOutOfRange()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(1));
            var array = builder.Build();

            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetVariantReader(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetVariantReader(1));
        }

        [Fact]
        public void GetVariantValueThrowsOnOutOfRange()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(1));
            var array = builder.Build();

            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetVariantValue(-1));
            Assert.Throws<ArgumentOutOfRangeException>(() => array.GetVariantValue(1));
        }

        [Fact]
        public void IReadOnlyListIndexer()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(10));
            builder.AppendNull();
            builder.Append(VariantValue.FromString("hi"));
            var array = builder.Build();

            Assert.Equal(3, array.Count);

            Assert.Equal(10, array[0].AsInt32());
            Assert.True(array[1].IsNull);
            Assert.Equal("hi", array[2].AsString());
        }

        [Fact]
        public void IReadOnlyListEnumerator()
        {
            var builder = new VariantArray.Builder();
            builder.Append(VariantValue.FromInt32(1));
            builder.AppendNull();
            builder.Append(VariantValue.FromInt32(3));
            var array = builder.Build();

            var list = array.ToList();
            Assert.Equal(3, list.Count);
            Assert.Equal(1, list[0].AsInt32());
            Assert.True(list[1].IsNull);
            Assert.Equal(3, list[2].AsInt32());
        }

        [Fact]
        public void FluentBuilderApi()
        {
            var array = new VariantArray.Builder()
                .Append(VariantValue.FromInt32(1))
                .Append(VariantValue.FromString("two"))
                .AppendNull()
                .Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(1, array.NullCount);
        }

        [Fact]
        public void BuilderLengthTracksAppends()
        {
            var builder = new VariantArray.Builder();
            Assert.Equal(0, builder.Length);

            builder.Append(VariantValue.FromInt32(1));
            Assert.Equal(1, builder.Length);

            builder.AppendNull();
            Assert.Equal(2, builder.Length);

            builder.Append(VariantValue.FromString("x"));
            Assert.Equal(3, builder.Length);
        }

        [Fact]
        public void AllNullArray()
        {
            var builder = new VariantArray.Builder();
            builder.AppendNull();
            builder.AppendNull();
            builder.AppendNull();
            var array = builder.Build();

            Assert.Equal(3, array.Length);
            Assert.Equal(3, array.NullCount);

            for (int i = 0; i < 3; i++)
            {
                Assert.True(array.IsNull(i));
                Assert.True(array.GetVariantValue(i).IsNull);
            }
        }

        [Fact]
        public void RowsWithDifferentObjectKeys()
        {
            var obj1 = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["x"] = VariantValue.FromInt32(1),
            });
            var obj2 = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                ["y"] = VariantValue.FromString("two"),
                ["z"] = VariantValue.True,
            });

            var builder = new VariantArray.Builder();
            builder.Append(obj1);
            builder.Append(obj2);
            var array = builder.Build();

            Assert.Equal(2, array.Length);

            var r0 = array.GetVariantValue(0);
            Assert.True(r0.IsObject);
            Assert.Equal(1, r0.AsObject()["x"].AsInt32());

            var r1 = array.GetVariantValue(1);
            Assert.True(r1.IsObject);
            Assert.Equal("two", r1.AsObject()["y"].AsString());
            Assert.True(r1.AsObject()["z"].AsBoolean());
        }

        [Fact]
        public void ExtensionTypeProperties()
        {
            var variantType = VariantType.Default;
            Assert.Equal(ArrowTypeId.Extension, variantType.TypeId);
            Assert.Equal("arrow.parquet.variant", variantType.Name);
            Assert.Equal("", variantType.ExtensionMetadata);
            Assert.IsType<StructType>(variantType.StorageType);

            var structType = (StructType)variantType.StorageType;
            Assert.Equal(2, structType.Fields.Count);
            Assert.Equal("metadata", structType.Fields[0].Name);
            Assert.Equal("value", structType.Fields[1].Name);
        }

        [Fact]
        public void ExtensionDefinitionTryCreateType()
        {
            var structType = new StructType(new[]
            {
                new Field("metadata", BinaryType.Default, false),
                new Field("value", BinaryType.Default, false),
            });

            Assert.True(VariantExtensionDefinition.Instance.TryCreateType(
                structType, "", out var extType));
            Assert.IsType<VariantType>(extType);
        }

        [Fact]
        public void ExtensionDefinitionRejectsInvalidType()
        {
            // Wrong field names
            var structType = new StructType(new[]
            {
                new Field("meta", BinaryType.Default, false),
                new Field("val", BinaryType.Default, false),
            });

            Assert.False(VariantExtensionDefinition.Instance.TryCreateType(
                structType, "", out _));

            // Non-struct type
            Assert.False(VariantExtensionDefinition.Instance.TryCreateType(
                Int32Type.Default, "", out _));
        }

        [Fact]
        public void IpcRoundTrip()
        {
            using (ExtensionTypeRegistry.Default.RegisterTemporary(VariantExtensionDefinition.Instance))
            {
                var builder = new VariantArray.Builder();
                builder.Append(VariantValue.FromInt32(42));
                builder.Append(VariantValue.FromString("hello"));
                builder.AppendNull();
                builder.Append(VariantValue.Null);
                var array = builder.Build();

                var field = new Field("variants", VariantType.Default, true);
                var schema = new Schema(new[] { field }, null);
                var batch = new RecordBatch(schema, new IArrowArray[] { array }, array.Length);

                // Write to stream
                var stream = new MemoryStream();
                var writer = new ArrowStreamWriter(stream, schema);
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
                stream.Position = 0;

                // Read back
                var reader = new ArrowStreamReader(stream);
                var readBatch = reader.ReadNextRecordBatch();
                Assert.NotNull(readBatch);

                var readArray = readBatch.Column(0) as VariantArray;
                Assert.NotNull(readArray);
                Assert.Equal(4, readArray.Length);
                Assert.Equal(1, readArray.NullCount);

                Assert.Equal(42, readArray.GetVariantValue(0).AsInt32());
                Assert.Equal("hello", readArray.GetVariantValue(1).AsString());
                Assert.True(readArray.GetVariantValue(2).IsNull);
                Assert.True(readArray.GetVariantValue(3).IsNull);
            }
        }
    }
}
