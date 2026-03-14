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
    /// Measures equality comparison cost: class (virtual dispatch on boxed values) vs
    /// struct (direct long comparison for inline types).
    /// </summary>
    [MemoryDiagnoser]
    public class EqualityBenchmarks
    {
        private VariantValue _classInt32A;
        private VariantValue _classInt32B;
        private VariantValue _classStringA;
        private VariantValue _classStringB;
        private StructVariantValue _structInt32A;
        private StructVariantValue _structInt32B;
        private StructVariantValue _structStringA;
        private StructVariantValue _structStringB;

        [GlobalSetup]
        public void Setup()
        {
            _classInt32A = VariantValue.FromInt32(42);
            _classInt32B = VariantValue.FromInt32(42);
            _classStringA = VariantValue.FromString("hello");
            _classStringB = VariantValue.FromString("hello");
            _structInt32A = StructVariantValue.FromInt32(42);
            _structInt32B = StructVariantValue.FromInt32(42);
            _structStringA = StructVariantValue.FromString("hello");
            _structStringB = StructVariantValue.FromString("hello");
        }

        // --- Int32 equality ---

        [Benchmark(Baseline = true)]
        public bool Class_EqualInt32() => _classInt32A.Equals(_classInt32B);

        [Benchmark]
        public bool Struct_EqualInt32() => _structInt32A.Equals(_structInt32B);

        // --- String equality ---

        [Benchmark]
        public bool Class_EqualString() => _classStringA.Equals(_classStringB);

        [Benchmark]
        public bool Struct_EqualString() => _structStringA.Equals(_structStringB);
    }
}
