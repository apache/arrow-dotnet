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
#if NET8_0_OR_GREATER
using System.Numerics;
using System.Numerics.Tensors;
#endif

namespace Apache.Arrow.Compute
{
    /// <summary>
    /// Aggregation kernels over <see cref="PrimitiveArray{T}"/> (Sum/Min/Max/Mean).
    /// </summary>
    /// <remarks>
    /// <para>
    /// Null handling follows LINQ semantics: null entries are skipped and do not contribute to the
    /// result. <c>Sum</c> of an empty or all-null array returns zero. <c>Min</c>, <c>Max</c> and
    /// <c>Mean</c> throw <see cref="InvalidOperationException"/> when the array contains no non-null
    /// elements, matching <see cref="System.Linq.Enumerable.Min{TSource}(System.Collections.Generic.IEnumerable{TSource})"/>
    /// and <see cref="System.Linq.Enumerable.Average(System.Collections.Generic.IEnumerable{int})"/>.
    /// </para>
    /// <para>
    /// On net8.0 and later the kernels are generic over <see cref="INumber{TSelf}"/> and, when the
    /// array has no nulls, dispatch to <see cref="TensorPrimitives"/> for a SIMD-accelerated single
    /// pass over the contiguous values buffer; when nulls are present they fall back to a correct,
    /// validity-aware scalar loop. On netstandard2.0 and net462 (where generic math and
    /// <see cref="TensorPrimitives"/> are unavailable) the kernels are provided as per-type overloads
    /// (<see cref="Int32Array"/>, <see cref="Int64Array"/>, <see cref="FloatArray"/>,
    /// <see cref="DoubleArray"/>) backed by scalar loops with the same null semantics.
    /// </para>
    /// </remarks>
    public static class Aggregations
    {
#if NET8_0_OR_GREATER
        /// <summary>Sums the non-null elements. Returns zero for an empty or all-null array.</summary>
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

        /// <summary>Returns the smallest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
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

        /// <summary>Returns the largest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
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

        /// <summary>Returns the arithmetic mean of the non-null elements as a <see cref="double"/>.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
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
#else
        // netstandard2.0 / net462 fallback: generic math and TensorPrimitives are unavailable, so the
        // kernels are provided as per-type overloads backed by validity-aware scalar loops. The null
        // semantics match the generic net8.0+ implementation above.

        private const string NoElements = "Sequence contains no non-null elements.";

        #region Int32Array

        /// <summary>Sums the non-null elements. Returns zero for an empty or all-null array.</summary>
        public static int Sum(this Int32Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<int> values = array.Values;
            int acc = 0;
            bool noNulls = array.NullCount == 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (noNulls || array.IsValid(i)) acc += values[i];
            }
            return acc;
        }

        /// <summary>Returns the smallest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static int Min(this Int32Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<int> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            int min = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { min = values[i]; set = true; }
                else if (values[i] < min) min = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return min;
        }

        /// <summary>Returns the largest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static int Max(this Int32Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<int> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            int max = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { max = values[i]; set = true; }
                else if (values[i] > max) max = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return max;
        }

        /// <summary>Returns the arithmetic mean of the non-null elements as a <see cref="double"/>.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Mean(this Int32Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            long count = array.Length - array.NullCount;
            if (count == 0) throw new InvalidOperationException(NoElements);
            return (double)array.Sum() / count;
        }

        #endregion

        #region Int64Array

        /// <summary>Sums the non-null elements. Returns zero for an empty or all-null array.</summary>
        public static long Sum(this Int64Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<long> values = array.Values;
            long acc = 0;
            bool noNulls = array.NullCount == 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (noNulls || array.IsValid(i)) acc += values[i];
            }
            return acc;
        }

        /// <summary>Returns the smallest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static long Min(this Int64Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<long> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            long min = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { min = values[i]; set = true; }
                else if (values[i] < min) min = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return min;
        }

        /// <summary>Returns the largest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static long Max(this Int64Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<long> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            long max = 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { max = values[i]; set = true; }
                else if (values[i] > max) max = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return max;
        }

        /// <summary>Returns the arithmetic mean of the non-null elements as a <see cref="double"/>.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Mean(this Int64Array array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            long count = array.Length - array.NullCount;
            if (count == 0) throw new InvalidOperationException(NoElements);
            return (double)array.Sum() / count;
        }

        #endregion

        #region FloatArray

        /// <summary>Sums the non-null elements. Returns zero for an empty or all-null array.</summary>
        public static float Sum(this FloatArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<float> values = array.Values;
            float acc = 0f;
            bool noNulls = array.NullCount == 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (noNulls || array.IsValid(i)) acc += values[i];
            }
            return acc;
        }

        /// <summary>Returns the smallest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static float Min(this FloatArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<float> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            float min = 0f;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { min = values[i]; set = true; }
                else if (values[i] < min) min = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return min;
        }

        /// <summary>Returns the largest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static float Max(this FloatArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<float> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            float max = 0f;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { max = values[i]; set = true; }
                else if (values[i] > max) max = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return max;
        }

        /// <summary>Returns the arithmetic mean of the non-null elements as a <see cref="double"/>.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Mean(this FloatArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            long count = array.Length - array.NullCount;
            if (count == 0) throw new InvalidOperationException(NoElements);
            return (double)array.Sum() / count;
        }

        #endregion

        #region DoubleArray

        /// <summary>Sums the non-null elements. Returns zero for an empty or all-null array.</summary>
        public static double Sum(this DoubleArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<double> values = array.Values;
            double acc = 0d;
            bool noNulls = array.NullCount == 0;
            for (int i = 0; i < values.Length; i++)
            {
                if (noNulls || array.IsValid(i)) acc += values[i];
            }
            return acc;
        }

        /// <summary>Returns the smallest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Min(this DoubleArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<double> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            double min = 0d;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { min = values[i]; set = true; }
                else if (values[i] < min) min = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return min;
        }

        /// <summary>Returns the largest non-null element.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Max(this DoubleArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            ReadOnlySpan<double> values = array.Values;
            bool noNulls = array.NullCount == 0;
            bool set = false;
            double max = 0d;
            for (int i = 0; i < values.Length; i++)
            {
                if (!noNulls && !array.IsValid(i)) continue;
                if (!set) { max = values[i]; set = true; }
                else if (values[i] > max) max = values[i];
            }
            if (!set) throw new InvalidOperationException(NoElements);
            return max;
        }

        /// <summary>Returns the arithmetic mean of the non-null elements as a <see cref="double"/>.</summary>
        /// <exception cref="InvalidOperationException">The array contains no non-null elements.</exception>
        public static double Mean(this DoubleArray array)
        {
            if (array is null) throw new ArgumentNullException(nameof(array));
            long count = array.Length - array.NullCount;
            if (count == 0) throw new InvalidOperationException(NoElements);
            return array.Sum() / count;
        }

        #endregion
#endif
    }
}
