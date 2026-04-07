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
using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Variant.Benchmarks
{
    /// <summary>
    /// Realistic mixed workload: build rows {id: int, name: string, scores: [double, double, double], active: bool}
    /// and traverse them to compute aggregate results.
    /// </summary>
    [MemoryDiagnoser]
    public class MixedWorkloadBenchmarks
    {
        [Params(100, 1_000)]
        public int RowCount;

        // ---------------------------------------------------------------
        // Build + traverse: Class (baseline)
        // ---------------------------------------------------------------

        [Benchmark(Baseline = true)]
        public double Class_BuildAndTraverse()
        {
            VariantValue[] rows = new VariantValue[RowCount];
            for (int i = 0; i < RowCount; i++)
            {
                Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>(4)
                {
                    ["id"] = VariantValue.FromInt32(i),
                    ["name"] = VariantValue.FromString("user" + i),
                    ["scores"] = VariantValue.FromArray(
                        VariantValue.FromDouble(i * 1.1),
                        VariantValue.FromDouble(i * 2.2),
                        VariantValue.FromDouble(i * 3.3)),
                    ["active"] = VariantValue.FromBoolean(i % 2 == 0),
                };
                rows[i] = VariantValue.FromObject(fields);
            }

            double totalScore = 0;
            int activeCount = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                IReadOnlyDictionary<string, VariantValue> obj = rows[i].AsObject();
                if (obj["active"].AsBoolean())
                {
                    activeCount++;
                    IReadOnlyList<VariantValue> scores = obj["scores"].AsArray();
                    for (int j = 0; j < scores.Count; j++)
                    {
                        totalScore += scores[j].AsDouble();
                    }
                }
            }

            return totalScore + activeCount;
        }

        // ---------------------------------------------------------------
        // Build + traverse: Struct
        // ---------------------------------------------------------------

        [Benchmark]
        public double Struct_BuildAndTraverse()
        {
            StructVariantValue[] rows = new StructVariantValue[RowCount];
            for (int i = 0; i < RowCount; i++)
            {
                Dictionary<string, StructVariantValue> fields = new Dictionary<string, StructVariantValue>(4)
                {
                    ["id"] = StructVariantValue.FromInt32(i),
                    ["name"] = StructVariantValue.FromString("user" + i),
                    ["scores"] = StructVariantValue.FromArray(new List<StructVariantValue>
                    {
                        StructVariantValue.FromDouble(i * 1.1),
                        StructVariantValue.FromDouble(i * 2.2),
                        StructVariantValue.FromDouble(i * 3.3),
                    }),
                    ["active"] = StructVariantValue.FromBoolean(i % 2 == 0),
                };
                rows[i] = StructVariantValue.FromObject(fields);
            }

            double totalScore = 0;
            int activeCount = 0;
            for (int i = 0; i < rows.Length; i++)
            {
                Dictionary<string, StructVariantValue> obj = rows[i].AsObject();
                if (obj["active"].AsBoolean())
                {
                    activeCount++;
                    List<StructVariantValue> scores = obj["scores"].AsArray();
                    for (int j = 0; j < scores.Count; j++)
                    {
                        totalScore += scores[j].AsDouble();
                    }
                }
            }

            return totalScore + activeCount;
        }
    }
}
