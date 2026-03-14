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

using BenchmarkDotNet.Attributes;

namespace Apache.Arrow.Variant.Benchmarks
{
    /// <summary>
    /// Fill + iterate arrays of variant values. Measures allocation scaling
    /// and cache locality effects across array sizes.
    /// </summary>
    [MemoryDiagnoser]
    public class ArrayBenchmarks
    {
        [Params(1_000, 10_000, 100_000)]
        public int Size;

        // ---------------------------------------------------------------
        // Fill: Int32
        // ---------------------------------------------------------------

        [Benchmark(Baseline = true)]
        public VariantValue[] Class_FillInt32()
        {
            VariantValue[] array = new VariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = VariantValue.FromInt32(i);
            }
            return array;
        }

        [Benchmark]
        public StructVariantValue[] Struct_FillInt32()
        {
            StructVariantValue[] array = new StructVariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = StructVariantValue.FromInt32(i);
            }
            return array;
        }

        // ---------------------------------------------------------------
        // Fill: Double
        // ---------------------------------------------------------------

        [Benchmark]
        public VariantValue[] Class_FillDouble()
        {
            VariantValue[] array = new VariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = VariantValue.FromDouble(i * 1.1);
            }
            return array;
        }

        [Benchmark]
        public StructVariantValue[] Struct_FillDouble()
        {
            StructVariantValue[] array = new StructVariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = StructVariantValue.FromDouble(i * 1.1);
            }
            return array;
        }

        // ---------------------------------------------------------------
        // Sum: Int32 (fill + iterate)
        // ---------------------------------------------------------------

        [Benchmark]
        public long Class_SumInt32()
        {
            VariantValue[] array = new VariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = VariantValue.FromInt32(i);
            }

            long sum = 0;
            for (int i = 0; i < array.Length; i++)
            {
                sum += array[i].AsInt32();
            }
            return sum;
        }

        [Benchmark]
        public long Struct_SumInt32()
        {
            StructVariantValue[] array = new StructVariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = StructVariantValue.FromInt32(i);
            }

            long sum = 0;
            for (int i = 0; i < array.Length; i++)
            {
                sum += array[i].AsInt32();
            }
            return sum;
        }

        // ---------------------------------------------------------------
        // Sum: Double (fill + iterate)
        // ---------------------------------------------------------------

        [Benchmark]
        public double Class_SumDouble()
        {
            VariantValue[] array = new VariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = VariantValue.FromDouble(i * 1.1);
            }

            double sum = 0;
            for (int i = 0; i < array.Length; i++)
            {
                sum += array[i].AsDouble();
            }
            return sum;
        }

        [Benchmark]
        public double Struct_SumDouble()
        {
            StructVariantValue[] array = new StructVariantValue[Size];
            for (int i = 0; i < Size; i++)
            {
                array[i] = StructVariantValue.FromDouble(i * 1.1);
            }

            double sum = 0;
            for (int i = 0; i < array.Length; i++)
            {
                sum += array[i].AsDouble();
            }
            return sum;
        }
    }
}
