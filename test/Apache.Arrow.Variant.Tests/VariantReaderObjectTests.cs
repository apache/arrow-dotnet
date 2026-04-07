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
using System.Text;
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantReaderObjectTests
    {
        // ---------------------------------------------------------------
        // Empty object
        // ---------------------------------------------------------------

        [Fact]
        public void EmptyObject_HasZeroFields()
        {
            VariantObjectReader obj = new VariantObjectReader(TestVectors.EmptyMetadata, TestVectors.ObjectEmpty);
            Assert.Equal(0, obj.FieldCount);
        }

        // ---------------------------------------------------------------
        // Object with single field: {"name": "Alice"}
        // ---------------------------------------------------------------

        [Fact]
        public void SingleField_FieldCount()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            Assert.Equal(1, obj.FieldCount);
        }

        [Fact]
        public void SingleField_FieldName()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            Assert.Equal("name", obj.GetFieldName(0));
        }

        [Fact]
        public void SingleField_FieldNameBytes()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            ReadOnlySpan<byte> nameBytes = obj.GetFieldNameBytes(0);
            Assert.True(nameBytes.SequenceEqual(Encoding.UTF8.GetBytes("name")));
        }

        [Fact]
        public void SingleField_FieldValue()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            VariantReader value = obj.GetFieldValue(0);
            Assert.True(value.IsString);
            Assert.Equal("Alice", value.GetString());
        }

        [Fact]
        public void SingleField_TryGetField_Found()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            bool found = obj.TryGetField("name", out VariantReader value);
            Assert.True(found);
            Assert.Equal("Alice", value.GetString());
        }

        [Fact]
        public void SingleField_TryGetField_NotFound()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice);

            bool found = obj.TryGetField("age", out VariantReader _);
            Assert.False(found);
        }

        // ---------------------------------------------------------------
        // Object with two fields: {"age": 30, "name": "Bob"}
        // ---------------------------------------------------------------

        [Fact]
        public void TwoFields_FieldCount()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Age_Name, TestVectors.Object_Age30_Name_Bob);

            Assert.Equal(2, obj.FieldCount);
        }

        [Fact]
        public void TwoFields_FieldNames()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Age_Name, TestVectors.Object_Age30_Name_Bob);

            Assert.Equal("age", obj.GetFieldName(0));
            Assert.Equal("name", obj.GetFieldName(1));
        }

        [Fact]
        public void TwoFields_FieldValues()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Age_Name, TestVectors.Object_Age30_Name_Bob);

            VariantReader ageValue = obj.GetFieldValue(0);
            Assert.Equal(VariantPrimitiveType.Int8, ageValue.PrimitiveType);
            Assert.Equal(30, ageValue.GetInt8());

            VariantReader nameValue = obj.GetFieldValue(1);
            Assert.True(nameValue.IsString);
            Assert.Equal("Bob", nameValue.GetString());
        }

        [Fact]
        public void TwoFields_TryGetField_Both()
        {
            VariantObjectReader obj = new VariantObjectReader(
                TestVectors.SortedMetadata_Age_Name, TestVectors.Object_Age30_Name_Bob);

            Assert.True(obj.TryGetField("age", out VariantReader ageValue));
            Assert.Equal(30, ageValue.GetInt8());

            Assert.True(obj.TryGetField("name", out VariantReader nameValue));
            Assert.Equal("Bob", nameValue.GetString());

            Assert.False(obj.TryGetField("email", out VariantReader _));
        }

        // ---------------------------------------------------------------
        // Error cases
        // ---------------------------------------------------------------

        [Fact]
        public void ConstructFromNonObject_Throws()
        {
            Assert.Throws<ArgumentException>(() =>
                new VariantObjectReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveNull));
        }

        [Fact]
        public void GetFieldValue_OutOfRange_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new VariantObjectReader(TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice).GetFieldValue(1));
        }

        [Fact]
        public void GetFieldId_OutOfRange_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new VariantObjectReader(TestVectors.SortedMetadata_Name, TestVectors.Object_Name_Alice).GetFieldId(-1));
        }
    }
}
