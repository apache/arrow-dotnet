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
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    public class ShredSchemaInfererTests
    {
        private readonly ShredSchemaInferer _inferer = new ShredSchemaInferer();

        [Fact]
        public void Infer_EmptyValues_ReturnsUnshredded()
        {
            ShredSchema schema = _inferer.Infer(new List<VariantValue>());
            Assert.Equal(ShredType.None, schema.TypedValueType);
        }

        [Fact]
        public void Infer_AllSameType_ReturnsPrimitive()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromInt32(1),
                VariantValue.FromInt32(2),
                VariantValue.FromInt32(3),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Int32, schema.TypedValueType);
        }

        [Fact]
        public void Infer_MixedTypes_BelowConsistency_ReturnsUnshredded()
        {
            // 2 strings vs 2 ints = 50% consistency, below default 80% threshold.
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromInt32(1),
                VariantValue.FromString("two"),
                VariantValue.FromInt32(3),
                VariantValue.FromString("four"),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.None, schema.TypedValueType);
        }

        [Fact]
        public void Infer_MixedTypes_AboveConsistency_ReturnsDominant()
        {
            // 9 ints vs 1 string = 90% consistency, above default 80%.
            List<VariantValue> values = new List<VariantValue>();
            for (int i = 0; i < 9; i++)
            {
                values.Add(VariantValue.FromInt32(i));
            }
            values.Add(VariantValue.FromString("outlier"));

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Int32, schema.TypedValueType);
        }

        [Fact]
        public void Infer_Objects_InfersFieldSchemas()
        {
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
                    { "age", VariantValue.FromInt32(25) },
                }),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Object, schema.TypedValueType);
            Assert.NotNull(schema.ObjectFields);
            Assert.True(schema.ObjectFields.ContainsKey("name"));
            Assert.True(schema.ObjectFields.ContainsKey("age"));
            Assert.Equal(ShredType.String, schema.ObjectFields["name"].TypedValueType);
            Assert.Equal(ShredType.Int32, schema.ObjectFields["age"].TypedValueType);
        }

        [Fact]
        public void Infer_Objects_RareField_Excluded()
        {
            // "rare" appears in only 1 of 4 objects = 25%, below 50% threshold.
            List<VariantValue> values = new List<VariantValue>();
            for (int i = 0; i < 4; i++)
            {
                Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>
                {
                    { "name", VariantValue.FromString($"user{i}") },
                };
                if (i == 0)
                {
                    fields["rare"] = VariantValue.FromInt32(99);
                }
                values.Add(VariantValue.FromObject(fields));
            }

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Object, schema.TypedValueType);
            Assert.True(schema.ObjectFields.ContainsKey("name"));
            Assert.False(schema.ObjectFields.ContainsKey("rare"));
        }

        [Fact]
        public void Infer_Arrays_InfersElementSchema()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromArray(
                    VariantValue.FromInt32(1),
                    VariantValue.FromInt32(2)),
                VariantValue.FromArray(
                    VariantValue.FromInt32(3)),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Array, schema.TypedValueType);
            Assert.NotNull(schema.ArrayElement);
            Assert.Equal(ShredType.Int32, schema.ArrayElement.TypedValueType);
        }

        [Fact]
        public void Infer_NestedObjects()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "address", VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "city", VariantValue.FromString("NYC") },
                        })
                    },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "address", VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "city", VariantValue.FromString("LA") },
                        })
                    },
                }),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Object, schema.TypedValueType);
            Assert.True(schema.ObjectFields.ContainsKey("address"));

            ShredSchema addressSchema = schema.ObjectFields["address"];
            Assert.Equal(ShredType.Object, addressSchema.TypedValueType);
            Assert.True(addressSchema.ObjectFields.ContainsKey("city"));
            Assert.Equal(ShredType.String, addressSchema.ObjectFields["city"].TypedValueType);
        }

        [Fact]
        public void Infer_RespectsMaxDepth()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "deep", VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "value", VariantValue.FromInt32(1) },
                        })
                    },
                }),
                VariantValue.FromObject(new Dictionary<string, VariantValue>
                {
                    { "deep", VariantValue.FromObject(new Dictionary<string, VariantValue>
                        {
                            { "value", VariantValue.FromInt32(2) },
                        })
                    },
                }),
            };

            // MaxDepth=0 means only top level — nested objects not explored.
            ShredOptions options = new ShredOptions { MaxDepth = 0 };
            ShredSchema schema = _inferer.Infer(values, options);

            // Top level is object, but fields shouldn't be further explored.
            Assert.Equal(ShredType.Object, schema.TypedValueType);
            // "deep" field is an object, but since we can't recurse (maxDepth=0),
            // it falls back to unshredded.
            Assert.True(schema.ObjectFields.ContainsKey("deep"));
            Assert.Equal(ShredType.None, schema.ObjectFields["deep"].TypedValueType);
        }

        [Fact]
        public void Infer_CustomOptions()
        {
            // 3 strings + 1 int = 75% string. Default (80%) would reject, but custom 70% accepts.
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromString("a"),
                VariantValue.FromString("b"),
                VariantValue.FromString("c"),
                VariantValue.FromInt32(1),
            };

            ShredOptions options = new ShredOptions { MinTypeConsistency = 0.7 };
            ShredSchema schema = _inferer.Infer(values, options);
            Assert.Equal(ShredType.String, schema.TypedValueType);
        }

        [Fact]
        public void Infer_AllStrings()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.FromString("hello"),
                VariantValue.FromString("world"),
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.String, schema.TypedValueType);
        }

        [Fact]
        public void Infer_AllBooleans()
        {
            List<VariantValue> values = new List<VariantValue>
            {
                VariantValue.True,
                VariantValue.False,
                VariantValue.True,
            };

            ShredSchema schema = _inferer.Infer(values);
            Assert.Equal(ShredType.Boolean, schema.TypedValueType);
        }
    }
}
