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
    public class VariantMetadataTests
    {
        // ---------------------------------------------------------------
        // Empty metadata
        // ---------------------------------------------------------------

        [Fact]
        public void EmptyMetadata_HasZeroDictionarySize()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.EmptyMetadata);
            Assert.Equal(0, metadata.DictionarySize);
            Assert.False(metadata.IsSorted);
        }

        [Fact]
        public void EmptyMetadata_FindString_ReturnsNegativeOne()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.EmptyMetadata);
            int index = metadata.FindString(Encoding.UTF8.GetBytes("anything"));
            Assert.Equal(-1, index);
        }

        // ---------------------------------------------------------------
        // Unsorted metadata
        // ---------------------------------------------------------------

        [Fact]
        public void UnsortedMetadata_ParsesCorrectly()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.UnsortedMetadata_B_A);

            Assert.Equal(2, metadata.DictionarySize);
            Assert.False(metadata.IsSorted);
            Assert.Equal("b", metadata.GetString(0));
            Assert.Equal("a", metadata.GetString(1));
        }

        [Fact]
        public void UnsortedMetadata_GetStringBytes_ReturnsCorrectBytes()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.UnsortedMetadata_B_A);

            ReadOnlySpan<byte> bytes0 = metadata.GetStringBytes(0);
            Assert.Equal(1, bytes0.Length);
            Assert.Equal((byte)'b', bytes0[0]);

            ReadOnlySpan<byte> bytes1 = metadata.GetStringBytes(1);
            Assert.Equal(1, bytes1.Length);
            Assert.Equal((byte)'a', bytes1[0]);
        }

        [Fact]
        public void UnsortedMetadata_FindString_LinearSearch()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.UnsortedMetadata_B_A);

            Assert.Equal(0, metadata.FindString(Encoding.UTF8.GetBytes("b")));
            Assert.Equal(1, metadata.FindString(Encoding.UTF8.GetBytes("a")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("c")));
        }

        // ---------------------------------------------------------------
        // Sorted metadata with 1-byte offsets
        // ---------------------------------------------------------------

        [Fact]
        public void SortedMetadata_ParsesCorrectly()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Alpha_Beta_Gamma);

            Assert.Equal(3, metadata.DictionarySize);
            Assert.True(metadata.IsSorted);
            Assert.Equal("alpha", metadata.GetString(0));
            Assert.Equal("beta", metadata.GetString(1));
            Assert.Equal("gamma", metadata.GetString(2));
        }

        [Fact]
        public void SortedMetadata_FindString_BinarySearch()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Alpha_Beta_Gamma);

            Assert.Equal(0, metadata.FindString(Encoding.UTF8.GetBytes("alpha")));
            Assert.Equal(1, metadata.FindString(Encoding.UTF8.GetBytes("beta")));
            Assert.Equal(2, metadata.FindString(Encoding.UTF8.GetBytes("gamma")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("delta")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("zzz")));
        }

        // ---------------------------------------------------------------
        // Sorted metadata with 2-byte offsets
        // ---------------------------------------------------------------

        [Fact]
        public void SortedMetadata2ByteOffsets_ParsesCorrectly()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata2ByteOffsets_Hello_World);

            Assert.Equal(2, metadata.DictionarySize);
            Assert.True(metadata.IsSorted);
            Assert.Equal("hello", metadata.GetString(0));
            Assert.Equal("world", metadata.GetString(1));
        }

        [Fact]
        public void SortedMetadata2ByteOffsets_FindString()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata2ByteOffsets_Hello_World);

            Assert.Equal(0, metadata.FindString(Encoding.UTF8.GetBytes("hello")));
            Assert.Equal(1, metadata.FindString(Encoding.UTF8.GetBytes("world")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("missing")));
        }

        // ---------------------------------------------------------------
        // Single-string metadata
        // ---------------------------------------------------------------

        [Fact]
        public void SingleStringMetadata_Name()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Name);

            Assert.Equal(1, metadata.DictionarySize);
            Assert.True(metadata.IsSorted);
            Assert.Equal("name", metadata.GetString(0));
            Assert.Equal(0, metadata.FindString(Encoding.UTF8.GetBytes("name")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("age")));
        }

        // ---------------------------------------------------------------
        // Two-string metadata ("age", "name")
        // ---------------------------------------------------------------

        [Fact]
        public void TwoStringMetadata_Age_Name()
        {
            VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Age_Name);

            Assert.Equal(2, metadata.DictionarySize);
            Assert.True(metadata.IsSorted);
            Assert.Equal("age", metadata.GetString(0));
            Assert.Equal("name", metadata.GetString(1));
            Assert.Equal(0, metadata.FindString(Encoding.UTF8.GetBytes("age")));
            Assert.Equal(1, metadata.FindString(Encoding.UTF8.GetBytes("name")));
            Assert.Equal(-1, metadata.FindString(Encoding.UTF8.GetBytes("address")));
        }

        // ---------------------------------------------------------------
        // Error cases
        // ---------------------------------------------------------------

        [Fact]
        public void EmptyBuffer_Throws()
        {
            Assert.Throws<ArgumentException>(() => new VariantMetadata(ReadOnlySpan<byte>.Empty));
        }

        [Fact]
        public void InvalidVersion_Throws()
        {
            // version=2 instead of 1
            byte[] data = new byte[] { 0x02, 0x00, 0x00 };
            Assert.Throws<NotSupportedException>(() => new VariantMetadata(data));
        }

        [Fact]
        public void TruncatedBuffer_MissingDictionarySize_Throws()
        {
            // Header only, no dictionary size
            byte[] data = new byte[] { 0x01 };
            Assert.Throws<ArgumentException>(() => new VariantMetadata(data));
        }

        [Fact]
        public void TruncatedBuffer_MissingOffsets_Throws()
        {
            // Header + dict_size=1 but no offsets
            byte[] data = new byte[] { 0x01, 0x01 };
            Assert.Throws<ArgumentException>(() => new VariantMetadata(data));
        }

        [Fact]
        public void GetStringBytes_IndexTooHigh_Throws()
        {
            try
            {
                VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Alpha_Beta_Gamma);
                _ = metadata.GetStringBytes(3);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
                // expected
            }
        }

        [Fact]
        public void GetStringBytes_NegativeIndex_Throws()
        {
            try
            {
                VariantMetadata metadata = new VariantMetadata(TestVectors.SortedMetadata_Alpha_Beta_Gamma);
                _ = metadata.GetStringBytes(-1);
                Assert.Fail("Expected ArgumentOutOfRangeException");
            }
            catch (ArgumentOutOfRangeException)
            {
                // expected
            }
        }

        [Fact]
        public void GetString_IndexTooHigh_Throws()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
                new VariantMetadata(TestVectors.SortedMetadata_Alpha_Beta_Gamma).GetString(3));
        }
    }
}
