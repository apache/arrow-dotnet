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
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    public class VariantUnshredderTests
    {
        // For typed_value-only results the metadata is unused, but the unshredder
        // still needs a valid empty metadata span. Build one once.
        private static readonly byte[] EmptyMetadata = new VariantMetadataBuilder().Build();

        // ---------------------------------------------------------------
        // Missing values
        // ---------------------------------------------------------------

        [Fact]
        public void Reconstruct_Missing_ReturnsNull()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            VariantValue? result = VariantUnshredder.Reconstruct(ShredResult.Missing, schema, EmptyMetadata);

            Assert.Null(result);
        }

        // ---------------------------------------------------------------
        // Primitive reconstruction
        // ---------------------------------------------------------------

        [Fact]
        public void Reconstruct_Boolean()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Boolean);
            ShredResult shredded = new ShredResult(null, true);

            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.True, result.Value);
        }

        [Fact]
        public void Reconstruct_Int32()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            ShredResult shredded = new ShredResult(null, 42);

            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.FromInt32(42), result.Value);
        }

        [Fact]
        public void Reconstruct_String()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.String);
            ShredResult shredded = new ShredResult(null, "hello");

            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.FromString("hello"), result.Value);
        }

        [Fact]
        public void Reconstruct_Double()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Double);
            ShredResult shredded = new ShredResult(null, Math.PI);

            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.FromDouble(Math.PI), result.Value);
        }

        [Fact]
        public void Reconstruct_Uuid()
        {
            Guid guid = Guid.NewGuid();
            ShredSchema schema = ShredSchema.Primitive(ShredType.Uuid);
            ShredResult shredded = new ShredResult(null, guid);

            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.FromUuid(guid), result.Value);
        }

        // ---------------------------------------------------------------
        // Object reconstruction
        // ---------------------------------------------------------------

        [Fact]
        public void Reconstruct_FullyShreddedObject()
        {
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredObjectResult objectResult = new ShredObjectResult(new Dictionary<string, ShredResult>
            {
                { "name", new ShredResult(null, "Alice") },
                { "age", new ShredResult(null, 30) },
            });

            ShredResult shredded = new ShredResult(null, objectResult);
            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.True(result.Value.IsObject);
            IReadOnlyDictionary<string, VariantValue> fields = result.Value.AsObject();
            Assert.Equal(2, fields.Count);
            Assert.Equal(VariantValue.FromString("Alice"), fields["name"]);
            Assert.Equal(VariantValue.FromInt32(30), fields["age"]);
        }

        [Fact]
        public void Reconstruct_ObjectWithMissingField()
        {
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredObjectResult objectResult = new ShredObjectResult(new Dictionary<string, ShredResult>
            {
                { "name", new ShredResult(null, "Alice") },
                { "age", ShredResult.Missing },
            });

            ShredResult shredded = new ShredResult(null, objectResult);
            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            IReadOnlyDictionary<string, VariantValue> fields = result.Value.AsObject();
            Assert.Single(fields);
            Assert.Equal(VariantValue.FromString("Alice"), fields["name"]);
        }

        [Fact]
        public void Reconstruct_NonObjectWithObjectSchema_DecodesFromBinary()
        {
            // Value was not an object, so it went to binary with typed_value=null.
            VariantValue original = VariantValue.FromInt32(42);

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "x", ShredSchema.Primitive(ShredType.Int32) },
            });

            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(new[] { original }, schema);
            VariantValue? result = VariantUnshredder.Reconstruct(rows[0], schema, metadata);

            Assert.True(result.HasValue);
            Assert.Equal(VariantValue.FromInt32(42), result.Value);
        }

        // ---------------------------------------------------------------
        // Array reconstruction
        // ---------------------------------------------------------------

        [Fact]
        public void Reconstruct_Array()
        {
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            ShredArrayResult arrayResult = new ShredArrayResult(new List<ShredResult>
            {
                new ShredResult(null, 1),
                new ShredResult(null, 2),
                new ShredResult(null, 3),
            });

            ShredResult shredded = new ShredResult(null, arrayResult);
            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            Assert.True(result.Value.IsArray);
            IReadOnlyList<VariantValue> elements = result.Value.AsArray();
            Assert.Equal(3, elements.Count);
            Assert.Equal(VariantValue.FromInt32(1), elements[0]);
            Assert.Equal(VariantValue.FromInt32(2), elements[1]);
            Assert.Equal(VariantValue.FromInt32(3), elements[2]);
        }

        [Fact]
        public void Reconstruct_ArrayWithMixedElements()
        {
            // Element 1 didn't match the shred type, so it's in binary.
            VariantValue original = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.FromInt32(3));

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(new[] { original }, schema);
            VariantValue? result = VariantUnshredder.Reconstruct(rows[0], schema, metadata);

            Assert.True(result.HasValue);
            IReadOnlyList<VariantValue> elements = result.Value.AsArray();
            Assert.Equal(3, elements.Count);
            Assert.Equal(VariantValue.FromInt32(1), elements[0]);
            Assert.Equal(VariantValue.FromString("two"), elements[1]);
            Assert.Equal(VariantValue.FromInt32(3), elements[2]);
        }

        // ---------------------------------------------------------------
        // Nested reconstruction
        // ---------------------------------------------------------------

        [Fact]
        public void Reconstruct_NestedObject()
        {
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "user", ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        { "name", ShredSchema.Primitive(ShredType.String) },
                    })
                },
            });

            ShredObjectResult innerObj = new ShredObjectResult(new Dictionary<string, ShredResult>
            {
                { "name", new ShredResult(null, "Alice") },
            });

            ShredObjectResult outerObj = new ShredObjectResult(new Dictionary<string, ShredResult>
            {
                { "user", new ShredResult(null, innerObj) },
            });

            ShredResult shredded = new ShredResult(null, outerObj);
            VariantValue? result = VariantUnshredder.Reconstruct(shredded, schema, EmptyMetadata);

            Assert.True(result.HasValue);
            IReadOnlyDictionary<string, VariantValue> outer = result.Value.AsObject();
            IReadOnlyDictionary<string, VariantValue> inner = outer["user"].AsObject();
            Assert.Equal(VariantValue.FromString("Alice"), inner["name"]);
        }
    }
}
