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
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    public class ShredSchemaTests
    {
        [Fact]
        public void Unshredded_HasNoneType()
        {
            ShredSchema schema = ShredSchema.Unshredded();
            Assert.Equal(ShredType.None, schema.TypedValueType);
            Assert.Null(schema.ObjectFields);
            Assert.Null(schema.ArrayElement);
        }

        [Fact]
        public void Primitive_HasCorrectType()
        {
            ShredSchema schema = ShredSchema.Primitive(ShredType.Int32);
            Assert.Equal(ShredType.Int32, schema.TypedValueType);
            Assert.Null(schema.ObjectFields);
            Assert.Null(schema.ArrayElement);
        }

        [Theory]
        [InlineData(ShredType.None)]
        [InlineData(ShredType.Object)]
        [InlineData(ShredType.Array)]
        public void Primitive_RejectsNonPrimitiveTypes(ShredType type)
        {
            Assert.Throws<ArgumentException>(() => ShredSchema.Primitive(type));
        }

        [Fact]
        public void ForObject_HasObjectType()
        {
            ShredSchema schema = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
            {
                { "x", ShredSchema.Primitive(ShredType.Int32) },
            });

            Assert.Equal(ShredType.Object, schema.TypedValueType);
            Assert.NotNull(schema.ObjectFields);
            Assert.Single(schema.ObjectFields);
            Assert.Null(schema.ArrayElement);
        }

        [Fact]
        public void ForObject_NullThrows()
        {
            Assert.Throws<ArgumentNullException>(() => ShredSchema.ForObject(null));
        }

        [Fact]
        public void ForArray_HasArrayType()
        {
            ShredSchema schema = ShredSchema.ForArray(ShredSchema.Primitive(ShredType.String));

            Assert.Equal(ShredType.Array, schema.TypedValueType);
            Assert.Null(schema.ObjectFields);
            Assert.NotNull(schema.ArrayElement);
            Assert.Equal(ShredType.String, schema.ArrayElement.TypedValueType);
        }

        [Fact]
        public void ForArray_NullThrows()
        {
            Assert.Throws<ArgumentNullException>(() => ShredSchema.ForArray(null));
        }

        [Fact]
        public void ShredTypeFromPrimitive_MapsAllTypes()
        {
            Assert.Equal(ShredType.Boolean, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.BooleanTrue));
            Assert.Equal(ShredType.Boolean, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.BooleanFalse));
            Assert.Equal(ShredType.Int8, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Int8));
            Assert.Equal(ShredType.Int16, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Int16));
            Assert.Equal(ShredType.Int32, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Int32));
            Assert.Equal(ShredType.Int64, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Int64));
            Assert.Equal(ShredType.Float, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Float));
            Assert.Equal(ShredType.Double, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Double));
            Assert.Equal(ShredType.Decimal4, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Decimal4));
            Assert.Equal(ShredType.Decimal8, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Decimal8));
            Assert.Equal(ShredType.Decimal16, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Decimal16));
            Assert.Equal(ShredType.Date, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Date));
            Assert.Equal(ShredType.Timestamp, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Timestamp));
            Assert.Equal(ShredType.TimestampNtz, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.TimestampNtz));
            Assert.Equal(ShredType.TimeNtz, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.TimeNtz));
            Assert.Equal(ShredType.TimestampTzNanos, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.TimestampTzNanos));
            Assert.Equal(ShredType.TimestampNtzNanos, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.TimestampNtzNanos));
            Assert.Equal(ShredType.String, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.String));
            Assert.Equal(ShredType.Binary, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Binary));
            Assert.Equal(ShredType.Uuid, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.Uuid));
            Assert.Equal(ShredType.None, ShredSchema.ShredTypeFromPrimitive(VariantPrimitiveType.NullType));
        }

        // ---------------------------------------------------------------
        // FromArrowType — decimal width inference across Decimal32/64/128.
        // ---------------------------------------------------------------

        [Theory]
        // Decimal32: any precision ≤ 9 maps to Decimal4.
        [InlineData(typeof(Decimal32Type), 4, 2, ShredType.Decimal4)]
        [InlineData(typeof(Decimal32Type), 9, 4, ShredType.Decimal4)]
        // Decimal64: ≤9 still fits Decimal4; 10–18 maps to Decimal8.
        [InlineData(typeof(Decimal64Type), 9, 2, ShredType.Decimal4)]
        [InlineData(typeof(Decimal64Type), 10, 2, ShredType.Decimal8)]
        [InlineData(typeof(Decimal64Type), 18, 9, ShredType.Decimal8)]
        // Decimal128: width chosen by precision bucket.
        [InlineData(typeof(Decimal128Type), 9, 4, ShredType.Decimal4)]
        [InlineData(typeof(Decimal128Type), 18, 9, ShredType.Decimal8)]
        [InlineData(typeof(Decimal128Type), 38, 9, ShredType.Decimal16)]
        public void FromArrowType_DecimalTypes_MapToCorrectShredWidth(
            Type arrowTypeKind, int precision, int scale, ShredType expected)
        {
            IArrowType arrowType = (IArrowType)Activator.CreateInstance(arrowTypeKind, precision, scale);
            ShredSchema schema = ShredSchema.FromArrowType(arrowType);
            Assert.Equal(expected, schema.TypedValueType);
        }

        [Fact]
        public void FromArrowType_Decimal128_PrecisionGreaterThan38_Throws()
        {
            // Precision 39 exceeds the spec max (38).
            Assert.Throws<ArgumentException>(
                () => ShredSchema.FromArrowType(new Decimal128Type(39, 0)));
        }

        [Fact]
        public void FromArrowType_Decimal256_Unsupported()
        {
            // Decimal256 exists in Arrow but the variant spec only defines 4/8/16-byte
            // decimal widths, so 32-byte unscaled storage isn't a valid shred target.
            Assert.Throws<ArgumentException>(
                () => ShredSchema.FromArrowType(new Decimal256Type(10, 2)));
        }
    }
}
