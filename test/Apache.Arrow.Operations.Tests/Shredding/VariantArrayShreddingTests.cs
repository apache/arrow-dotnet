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

using System.Collections.Generic;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Tests for the array-level shredding entry points on <see cref="VariantArray"/>:
    /// <c>InferShredSchema</c>, <c>Shred</c>, <c>TryShred</c> and <c>Reassemble</c>.
    /// </summary>
    public class VariantArrayShreddingTests
    {
        private static VariantValue Obj(int a, string b = null)
        {
            var fields = new Dictionary<string, VariantValue> { ["a"] = VariantValue.FromInt32(a) };
            if (b != null) fields["b"] = VariantValue.FromString(b);
            return VariantValue.FromObject(fields);
        }

        private static VariantArray Unshredded(IEnumerable<VariantValue?> values)
        {
            var builder = new VariantArray.Builder();
            builder.AppendRange(values);
            return builder.Build();
        }

        private static void AssertRows(IReadOnlyList<VariantValue?> expected, VariantArray array)
        {
            Assert.Equal(expected.Count, array.Length);
            for (int i = 0; i < expected.Count; i++)
            {
                if (expected[i].HasValue)
                {
                    Assert.False(array.IsNull(i), $"row {i} should be valid");
                    Assert.Equal(expected[i].Value, array.GetLogicalVariantValue(i));
                }
                else
                {
                    Assert.True(array.IsNull(i), $"row {i} should be null");
                }
            }
        }

        private static string Describe(ShredSchema schema)
        {
            switch (schema.TypedValueType)
            {
                case ShredType.Object:
                    var fields = new List<string>();
                    foreach (KeyValuePair<string, ShredSchema> f in schema.ObjectFields)
                        fields.Add(f.Key + ":" + Describe(f.Value));
                    fields.Sort(System.StringComparer.Ordinal);
                    return "{" + string.Join(",", fields) + "}";
                case ShredType.Array:
                    return "[" + Describe(schema.ArrayElement) + "]";
                default:
                    return schema.TypedValueType.ToString();
            }
        }

        private static readonly ShredSchema ObjectA = ShredSchema.ForObject(
            new Dictionary<string, ShredSchema> { ["a"] = ShredSchema.Primitive(ShredType.Int32) });

        // Covers a SQL-NULL row, a present variant null, and a value that falls
        // back to the residual under an object schema.
        private static readonly List<VariantValue?> MixedRows = new List<VariantValue?>
        {
            Obj(1, "x"), null, VariantValue.Null, Obj(4), VariantValue.FromString("not an object"),
        };

        // Consistent enough to infer an object schema under the default options.
        private static readonly List<VariantValue?> ObjectRows = new List<VariantValue?>
        {
            Obj(1, "x"), null, Obj(2), Obj(3, "y"), Obj(4, "z"),
        };

        [Fact]
        public void Shred_UnshreddedInput()
        {
            VariantArray shredded = Unshredded(MixedRows).Shred(ObjectA);

            Assert.True(shredded.IsShredded);
            Assert.Equal(Describe(ObjectA), Describe(shredded.GetShredSchema()));
            Assert.Equal(1, shredded.NullCount);
            AssertRows(MixedRows, shredded);
        }

        [Fact]
        public void Shred_WithInferredSchema()
        {
            VariantArray input = Unshredded(ObjectRows);
            ShredSchema schema = input.InferShredSchema();

            Assert.Equal("{a:Int32,b:String}", Describe(schema));
            AssertRows(ObjectRows, input.Shred(schema));
        }

        [Fact]
        public void Shred_AlreadyShreddedInput_ReshredsLogicalValues()
        {
            VariantArray first = Unshredded(MixedRows).Shred(ObjectA);

            ShredSchema second = ShredSchema.ForObject(
                new Dictionary<string, ShredSchema> { ["b"] = ShredSchema.Primitive(ShredType.String) });
            VariantArray reshredded = first.Shred(second);

            Assert.Equal(Describe(second), Describe(reshredded.GetShredSchema()));
            AssertRows(MixedRows, reshredded);
        }

        [Fact]
        public void Shred_SlicedInput()
        {
            var sliced = (VariantArray)ArrowArrayFactory.Slice(Unshredded(MixedRows), 1, 3);

            VariantArray shredded = sliced.Shred(ObjectA);

            AssertRows(MixedRows.GetRange(1, 3), shredded);
        }

        [Fact]
        public void Shred_BatchesShareInferredLayout()
        {
            VariantArray batch1 = Unshredded(new List<VariantValue?> { Obj(1, "x"), Obj(2, "y") });
            VariantArray batch2 = Unshredded(new List<VariantValue?> { VariantValue.FromInt32(7), null });
            ShredSchema schema = batch1.InferShredSchema();

            VariantArray shredded1 = batch1.Shred(schema);
            VariantArray shredded2 = batch2.Shred(schema);

            Assert.Equal(Describe(schema), Describe(shredded1.GetShredSchema()));
            Assert.Equal(Describe(schema), Describe(shredded2.GetShredSchema()));
            AssertRows(new List<VariantValue?> { VariantValue.FromInt32(7), null }, shredded2);
        }

        [Fact]
        public void InferShredSchema_IgnoresNullElements()
        {
            VariantArray input = Unshredded(new List<VariantValue?>
            {
                VariantValue.FromInt32(1), null, null, null, VariantValue.FromInt32(2), null,
            });
            Assert.Equal(ShredType.Int32, input.InferShredSchema().TypedValueType);
        }

        [Fact]
        public void TryShred_ShreddableValues()
        {
            Assert.True(Unshredded(ObjectRows).TryShred(ShredOptions.Default, out VariantArray shredded));
            Assert.True(shredded.IsShredded);
            AssertRows(ObjectRows, shredded);
        }

        [Fact]
        public void TryShred_NothingToShred()
        {
            VariantArray input = Unshredded(new List<VariantValue?> { null, null });
            Assert.False(input.TryShred(null, out VariantArray shredded));
            Assert.Null(shredded);
        }

        [Fact]
        public void Reassemble_ShreddedInput()
        {
            VariantArray shredded = Unshredded(MixedRows).Shred(ObjectA);

            VariantArray reassembled = shredded.Reassemble();

            Assert.False(reassembled.IsShredded);
            Assert.Equal(1, reassembled.NullCount);
            AssertRows(MixedRows, reassembled);
            // An unshredded array supports the core (non-Operations) reader.
            Assert.Equal(MixedRows[0].Value, reassembled.GetVariantValue(0));
        }

        [Fact]
        public void Reassemble_SlicedInput()
        {
            VariantArray shredded = Unshredded(MixedRows).Shred(ObjectA);
            var sliced = (VariantArray)ArrowArrayFactory.Slice(shredded, 2, 3);

            AssertRows(MixedRows.GetRange(2, 3), sliced.Reassemble());
        }

        [Fact]
        public void Reassemble_UnshreddedInput_ReturnsSameArray()
        {
            VariantArray input = Unshredded(MixedRows);
            Assert.Same(input, input.Reassemble());
        }
    }
}
