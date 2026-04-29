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
    public class VariantShredderTests
    {
        private static ShredResult ShredOne(VariantValue value, ShredSchema schema)
        {
            (_, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(new[] { value }, schema);
            return rows[0];
        }

        // ---------------------------------------------------------------
        // Column-level metadata is shared, not per-row framed
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_EmptyColumn_ProducesEmptyMetadata()
        {
            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(
                System.Array.Empty<VariantValue>(),
                ShredSchema.Primitive(ShredType.Int32));

            Assert.NotNull(metadata);
            Assert.Empty(rows);
        }

        [Fact]
        public void Shred_Column_ReturnsSharedMetadata()
        {
            // Two rows with overlapping field names should produce a single
            // metadata dictionary containing all unique names.
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Alice") },
                    { "age", VariantValue.FromInt32(30) },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString("Bob") },
                    { "city", VariantValue.FromString("NYC") },
                }),
            };

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
            });

            (byte[] metadata, IReadOnlyList<ShredResult> rows) = VariantShredder.Shred(values, schema);

            // Metadata dictionary should contain at least "age" and "city" (the residual fields).
            VariantMetadata meta = new VariantMetadata(metadata);
            HashSet<string> names = new HashSet<string>();
            for (int i = 0; i < meta.DictionarySize; i++)
            {
                names.Add(meta.GetString(i));
            }
            Assert.Contains("age", names);
            Assert.Contains("city", names);

            Assert.Equal(2, rows.Count);
        }

        // ---------------------------------------------------------------
        // Unshredded (ShredType.None)
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_Unshredded_EncodesAsBinary()
        {
            VariantValue value = VariantValue.FromInt32(42);
            ShredResult result = ShredOne(value, ShredSchema.Unshredded());

            Assert.NotNull(result.Value);
            Assert.Null(result.TypedValue);
            Assert.False(result.IsMissing);
        }

        // ---------------------------------------------------------------
        // Primitive shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_Boolean_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.True, ShredSchema.Primitive(ShredType.Boolean));

            Assert.Null(result.Value);
            Assert.NotNull(result.TypedValue);
            Assert.Equal(true, result.TypedValue);
        }

        [Fact]
        public void Shred_Boolean_False()
        {
            ShredResult result = ShredOne(VariantValue.False, ShredSchema.Primitive(ShredType.Boolean));

            Assert.Null(result.Value);
            Assert.Equal(false, result.TypedValue);
        }

        [Fact]
        public void Shred_Int32_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromInt32(42), ShredSchema.Primitive(ShredType.Int32));

            Assert.Null(result.Value);
            Assert.Equal(42, result.TypedValue);
        }

        [Fact]
        public void Shred_Int64_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromInt64(long.MaxValue), ShredSchema.Primitive(ShredType.Int64));

            Assert.Null(result.Value);
            Assert.Equal(long.MaxValue, result.TypedValue);
        }

        [Fact]
        public void Shred_Float_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromFloat(3.14f), ShredSchema.Primitive(ShredType.Float));

            Assert.Null(result.Value);
            Assert.Equal(3.14f, result.TypedValue);
        }

        [Fact]
        public void Shred_Double_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromDouble(Math.PI), ShredSchema.Primitive(ShredType.Double));

            Assert.Null(result.Value);
            Assert.Equal(Math.PI, result.TypedValue);
        }

        [Fact]
        public void Shred_String_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromString("hello"), ShredSchema.Primitive(ShredType.String));

            Assert.Null(result.Value);
            Assert.Equal("hello", result.TypedValue);
        }

        [Fact]
        public void Shred_Binary_MatchingType()
        {
            byte[] data = new byte[] { 1, 2, 3 };
            ShredResult result = ShredOne(VariantValue.FromBinary(data), ShredSchema.Primitive(ShredType.Binary));

            Assert.Null(result.Value);
            Assert.Equal(data, result.TypedValue);
        }

        [Fact]
        public void Shred_Uuid_MatchingType()
        {
            Guid guid = Guid.NewGuid();
            ShredResult result = ShredOne(VariantValue.FromUuid(guid), ShredSchema.Primitive(ShredType.Uuid));

            Assert.Null(result.Value);
            Assert.Equal(guid, result.TypedValue);
        }

        [Fact]
        public void Shred_Date_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromDate(19000), ShredSchema.Primitive(ShredType.Date));

            Assert.Null(result.Value);
            Assert.Equal(19000, result.TypedValue);
        }

        [Fact]
        public void Shred_Decimal4_MatchingType()
        {
            ShredResult result = ShredOne(VariantValue.FromDecimal4(99.99m), ShredSchema.Primitive(ShredType.Decimal4));

            Assert.Null(result.Value);
            Assert.Equal(99.99m, result.TypedValue);
        }

        [Fact]
        public void Shred_TypeMismatch_FallsBackToBinary()
        {
            // Schema expects Int32, but value is a string.
            ShredResult result = ShredOne(VariantValue.FromString("hello"), ShredSchema.Primitive(ShredType.Int32));

            Assert.NotNull(result.Value);
            Assert.Null(result.TypedValue);
        }

        [Fact]
        public void Shred_NullVariant_FallsBackToBinary()
        {
            // Variant null doesn't match any primitive shred type.
            ShredResult result = ShredOne(VariantValue.Null, ShredSchema.Primitive(ShredType.Int32));

            Assert.NotNull(result.Value);
            Assert.Null(result.TypedValue);
        }

        // ---------------------------------------------------------------
        // Object shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_Object_FullyShredded()
        {
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredResult result = ShredOne(value, schema);

            // Fully shredded: value is null.
            Assert.Null(result.Value);
            Assert.NotNull(result.TypedValue);

            ShredObjectResult objectResult = (ShredObjectResult)result.TypedValue;
            Assert.Equal(2, objectResult.Fields.Count);

            Assert.Null(objectResult.Fields["name"].Value);
            Assert.Equal("Alice", objectResult.Fields["name"].TypedValue);

            Assert.Null(objectResult.Fields["age"].Value);
            Assert.Equal(30, objectResult.Fields["age"].TypedValue);
        }

        [Fact]
        public void Shred_Object_PartiallyShredded()
        {
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
                { "age", VariantValue.FromInt32(30) },
                { "extra", VariantValue.True },
            });

            // Schema only shreds "name" — "age" and "extra" go to residual.
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
            });

            ShredResult result = ShredOne(value, schema);

            // Partially shredded: both value and typed_value are non-null.
            Assert.NotNull(result.Value);
            Assert.NotNull(result.TypedValue);

            ShredObjectResult objectResult = (ShredObjectResult)result.TypedValue;
            Assert.Equal("Alice", objectResult.Fields["name"].TypedValue);
        }

        [Fact]
        public void Shred_Object_MissingField()
        {
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("Alice") },
            });

            // Schema expects "name" and "age", but "age" is missing.
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "name", ShredSchema.Primitive(ShredType.String) },
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredResult result = ShredOne(value, schema);

            ShredObjectResult objectResult = (ShredObjectResult)result.TypedValue;
            Assert.True(objectResult.Fields["age"].IsMissing);
        }

        [Fact]
        public void Shred_NotObject_WithObjectSchema_FallsBackToBinary()
        {
            VariantValue value = VariantValue.FromInt32(42);
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "x", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredResult result = ShredOne(value, schema);

            Assert.NotNull(result.Value);
            Assert.Null(result.TypedValue);
        }

        [Fact]
        public void Shred_Object_FieldTypeMismatch()
        {
            // Field "age" is a string but schema expects Int32.
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "age", VariantValue.FromString("thirty") },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "age", ShredSchema.Primitive(ShredType.Int32) },
            });

            ShredResult result = ShredOne(value, schema);
            ShredObjectResult objectResult = (ShredObjectResult)result.TypedValue;

            // Field falls back to binary within the shredded object.
            Assert.NotNull(objectResult.Fields["age"].Value);
            Assert.Null(objectResult.Fields["age"].TypedValue);
        }

        // ---------------------------------------------------------------
        // Array shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_Array_AllMatchingType()
        {
            VariantValue value = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(3));

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            ShredResult result = ShredOne(value, schema);

            Assert.Null(result.Value);
            Assert.NotNull(result.TypedValue);

            ShredArrayResult arrayResult = (ShredArrayResult)result.TypedValue;
            Assert.Equal(3, arrayResult.Elements.Count);
            Assert.Equal(1, arrayResult.Elements[0].TypedValue);
            Assert.Equal(2, arrayResult.Elements[1].TypedValue);
            Assert.Equal(3, arrayResult.Elements[2].TypedValue);
        }

        [Fact]
        public void Shred_Array_MixedTypes()
        {
            VariantValue value = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.FromInt32(3));

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            ShredResult result = ShredOne(value, schema);
            ShredArrayResult arrayResult = (ShredArrayResult)result.TypedValue;

            // Element 0: matches, goes to typed.
            Assert.Null(arrayResult.Elements[0].Value);
            Assert.Equal(1, arrayResult.Elements[0].TypedValue);

            // Element 1: doesn't match, goes to binary.
            Assert.NotNull(arrayResult.Elements[1].Value);
            Assert.Null(arrayResult.Elements[1].TypedValue);

            // Element 2: matches.
            Assert.Null(arrayResult.Elements[2].Value);
            Assert.Equal(3, arrayResult.Elements[2].TypedValue);
        }

        [Fact]
        public void Shred_Array_NullElement_FallsToBinary()
        {
            // Variant null in an array goes to binary (it doesn't match Int32).
            VariantValue value = VariantValue.FromArray(
                VariantValue.FromInt32(1),
                VariantValue.Null,
                VariantValue.FromInt32(3));

            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            ShredResult result = ShredOne(value, schema);
            ShredArrayResult arrayResult = (ShredArrayResult)result.TypedValue;

            // Null element falls back to binary (not missing — arrays can't have missing).
            Assert.NotNull(arrayResult.Elements[1].Value);
            Assert.Null(arrayResult.Elements[1].TypedValue);
            Assert.False(arrayResult.Elements[1].IsMissing);
        }

        [Fact]
        public void Shred_NotArray_WithArraySchema_FallsBackToBinary()
        {
            VariantValue value = VariantValue.FromInt32(42);
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32));

            ShredResult result = ShredOne(value, schema);

            Assert.NotNull(result.Value);
            Assert.Null(result.TypedValue);
        }

        // ---------------------------------------------------------------
        // Nested shredding
        // ---------------------------------------------------------------

        [Fact]
        public void Shred_NestedObject()
        {
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "user", VariantValue.FromObject(new Dictionary<string, VariantValue>
                    {
                        { "name", VariantValue.FromString("Alice") },
                        { "age", VariantValue.FromInt32(30) },
                    })
                },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "user", ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        { "name", ShredSchema.Primitive(ShredType.String) },
                        { "age", ShredSchema.Primitive(ShredType.Int32) },
                    })
                },
            });

            ShredResult result = ShredOne(value, schema);

            Assert.Null(result.Value);
            ShredObjectResult outer = (ShredObjectResult)result.TypedValue;

            ShredResult userResult = outer.Fields["user"];
            Assert.Null(userResult.Value);
            ShredObjectResult userObj = (ShredObjectResult)userResult.TypedValue;

            Assert.Equal("Alice", userObj.Fields["name"].TypedValue);
            Assert.Equal(30, userObj.Fields["age"].TypedValue);
        }

        [Fact]
        public void Shred_ObjectWithArrayField()
        {
            VariantValue value = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "scores", VariantValue.FromArray(
                    VariantValue.FromInt32(95),
                    VariantValue.FromInt32(87))
                },
            });

            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "scores", ShredSchema.ForArray(ShredSchema.Primitive(ShredType.Int32)) },
            });

            ShredResult result = ShredOne(value, schema);

            ShredObjectResult obj = (ShredObjectResult)result.TypedValue;
            ShredResult scoresResult = obj.Fields["scores"];
            ShredArrayResult arr = (ShredArrayResult)scoresResult.TypedValue;
            Assert.Equal(2, arr.Elements.Count);
            Assert.Equal(95, arr.Elements[0].TypedValue);
            Assert.Equal(87, arr.Elements[1].TypedValue);
        }
    }
}
