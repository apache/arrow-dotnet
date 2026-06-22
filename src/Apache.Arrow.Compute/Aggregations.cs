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
using System.Numerics;
using System.Numerics.Tensors;

namespace Apache.Arrow.Compute
{
    /// <summary>
    /// Aggregation kernels over <see cref="PrimitiveArray{T}"/>. When the array has no nulls the
    /// kernels dispatch to <see cref="TensorPrimitives"/> for a SIMD-accelerated single pass over
    /// the contiguous values buffer. When nulls are present they fall back to a correct,
    /// validity-aware scalar loop.
    /// </summary>
    public static class Aggregations
    {
        public static T Sum<T>(this PrimitiveArray<T> array)
            where T : unmanaged, INumber<T>
        {
            if (array is null) throw new ArgumentNullException(nameof(array));

            ReadOnlySpan<T> values = array.Values;

            if (array.NullCount == 0)
            {
                return TensorPrimitives.Sum(values);
            }

            T acc = T.Zero;
            for (int i = 0; i < values.Length; i++)
            {
                if (array.IsValid(i))
                {
                    acc += values[i];
                }
            }
            return acc;
        }

        public static T Min<T>(this PrimitiveArray<T> array)
            where T : unmanaged, INumber<T>
        {
            if (array is null) throw new ArgumentNullException(nameof(array));

            ReadOnlySpan<T> values = array.Values;

            if (values.Length == 0 || array.Length - array.NullCount == 0)
            {
                throw new InvalidOperationException("Sequence contains no non-null elements.");
            }

            if (array.NullCount == 0)
            {
                return TensorPrimitives.Min(values);
            }

            bool set = false;
            T min = T.Zero;
            for (int i = 0; i < values.Length; i++)
            {
                if (!array.IsValid(i)) continue;
                if (!set) { min = values[i]; set = true; }
                else if (values[i] < min) { min = values[i]; }
            }
            return min;
        }

        public static T Max<T>(this PrimitiveArray<T> array)
            where T : unmanaged, INumber<T>
        {
            if (array is null) throw new ArgumentNullException(nameof(array));

            ReadOnlySpan<T> values = array.Values;

            if (values.Length == 0 || array.Length - array.NullCount == 0)
            {
                throw new InvalidOperationException("Sequence contains no non-null elements.");
            }

            if (array.NullCount == 0)
            {
                return TensorPrimitives.Max(values);
            }

            bool set = false;
            T max = T.Zero;
            for (int i = 0; i < values.Length; i++)
            {
                if (!array.IsValid(i)) continue;
                if (!set) { max = values[i]; set = true; }
                else if (values[i] > max) { max = values[i]; }
            }
            return max;
        }

        public static double Mean<T>(this PrimitiveArray<T> array)
            where T : unmanaged, INumber<T>
        {
            if (array is null) throw new ArgumentNullException(nameof(array));

            long count = array.Length - array.NullCount;
            if (count == 0)
            {
                throw new InvalidOperationException("Sequence contains no non-null elements.");
            }

            T sum = array.Sum();
            return double.CreateChecked(sum) / count;
        }
    }
}
