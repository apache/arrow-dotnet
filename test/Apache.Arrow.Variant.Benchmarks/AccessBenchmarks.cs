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
    /// Measures typed accessor cost: class (unbox from object) vs
    /// struct (inline read from long field).
    /// </summary>
    [MemoryDiagnoser]
    public class AccessBenchmarks
    {
        private VariantValue _classInt32;
        private VariantValue _classDouble;
        private VariantValue _classString;
        private StructVariantValue _structInt32;
        private StructVariantValue _structDouble;
        private StructVariantValue _structString;

        [GlobalSetup]
        public void Setup()
        {
            _classInt32 = VariantValue.FromInt32(42);
            _classDouble = VariantValue.FromDouble(3.14);
            _classString = VariantValue.FromString("hello");
            _structInt32 = StructVariantValue.FromInt32(42);
            _structDouble = StructVariantValue.FromDouble(3.14);
            _structString = StructVariantValue.FromString("hello");
        }

        // --- Int32 ---

        [Benchmark(Baseline = true)]
        public int Class_AccessInt32() => _classInt32.AsInt32();

        [Benchmark]
        public int Struct_AccessInt32() => _structInt32.AsInt32();

        // --- Double ---

        [Benchmark]
        public double Class_AccessDouble() => _classDouble.AsDouble();

        [Benchmark]
        public double Struct_AccessDouble() => _structDouble.AsDouble();

        // --- String ---

        [Benchmark]
        public string Class_AccessString() => _classString.AsString();

        [Benchmark]
        public string Struct_AccessString() => _structString.AsString();
    }
}
