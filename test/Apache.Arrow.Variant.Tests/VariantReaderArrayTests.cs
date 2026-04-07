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
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    public class VariantReaderArrayTests
    {
        // ---------------------------------------------------------------
        // Empty array
        // ---------------------------------------------------------------

        [Fact]
        public void EmptyArray_HasZeroElements()
        {
            VariantArrayReader arr = new VariantArrayReader(TestVectors.EmptyMetadata, TestVectors.ArrayEmpty);
            Assert.Equal(0, arr.ElementCount);
        }

        // ---------------------------------------------------------------
        // Array with 3 Int8 elements: [1, 2, 3]
        // ---------------------------------------------------------------

        [Fact]
        public void Int8Array_ElementCount()
        {
            VariantArrayReader arr = new VariantArrayReader(
                TestVectors.EmptyMetadata, TestVectors.Array_Int8_1_2_3);

            Assert.Equal(3, arr.ElementCount);
        }

        [Fact]
        public void Int8Array_Elements()
        {
            VariantArrayReader arr = new VariantArrayReader(
                TestVectors.EmptyMetadata, TestVectors.Array_Int8_1_2_3);

            VariantReader e0 = arr.GetElement(0);
            Assert.Equal(VariantPrimitiveType.Int8, e0.PrimitiveType);
            Assert.Equal(1, e0.GetInt8());

            VariantReader e1 = arr.GetElement(1);
            Assert.Equal(2, e1.GetInt8());

            VariantReader e2 = arr.GetElement(2);
            Assert.Equal(3, e2.GetInt8());
        }

        // ---------------------------------------------------------------
        // Mixed-type array: [42, "hi", null]
        // ---------------------------------------------------------------

        [Fact]
        public void MixedArray_ElementCount()
        {
            VariantArrayReader arr = new VariantArrayReader(
                TestVectors.EmptyMetadata, TestVectors.Array_Mixed);

            Assert.Equal(3, arr.ElementCount);
        }

        [Fact]
        public void MixedArray_Elements()
        {
            VariantArrayReader arr = new VariantArrayReader(
                TestVectors.EmptyMetadata, TestVectors.Array_Mixed);

            VariantReader e0 = arr.GetElement(0);
            Assert.Equal(VariantPrimitiveType.Int8, e0.PrimitiveType);
            Assert.Equal(42, e0.GetInt8());

            VariantReader e1 = arr.GetElement(1);
            Assert.True(e1.IsString);
            Assert.Equal("hi", e1.GetString());

            VariantReader e2 = arr.GetElement(2);
            Assert.True(e2.IsNull);
        }

        // ---------------------------------------------------------------
        // Error cases
        // ---------------------------------------------------------------

        [Fact]
        public void ConstructFromNonArray_Throws()
        {
            Assert.Throws<ArgumentException>(() =>
                new VariantArrayReader(TestVectors.EmptyMetadata, TestVectors.PrimitiveNull));
        }

        [Fact]
        public void GetElement_OutOfRange_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new VariantArrayReader(TestVectors.EmptyMetadata, TestVectors.Array_Int8_1_2_3).GetElement(3));
        }

        [Fact]
        public void GetElement_NegativeIndex_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new VariantArrayReader(TestVectors.EmptyMetadata, TestVectors.Array_Int8_1_2_3).GetElement(-1));
        }
    }
}
