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

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Options controlling how <see cref="ShredSchemaInferer"/> infers a shredding schema.
    /// </summary>
    public sealed class ShredOptions
    {
        /// <summary>
        /// Maximum nesting depth for shredded objects and arrays.
        /// 0 means only top-level fields are shredded.
        /// Default is 3.
        /// </summary>
        public int MaxDepth { get; set; } = 3;

        /// <summary>
        /// Minimum fraction of values (0.0–1.0) in which a field must appear
        /// to be considered for shredding. Fields appearing less frequently
        /// than this threshold are left in the binary residual.
        /// Default is 0.5 (50%).
        /// </summary>
        public double MinFieldFrequency { get; set; } = 0.5;

        /// <summary>
        /// Minimum fraction of non-null values (0.0–1.0) for a field that must
        /// share the same type for the field to be shredded as a typed column.
        /// If the type consistency is below this threshold, the field gets a
        /// <see cref="ShredType.None"/> schema (binary-only).
        /// Default is 0.8 (80%).
        /// </summary>
        public double MinTypeConsistency { get; set; } = 0.8;

        /// <summary>Default options.</summary>
        public static ShredOptions Default => new ShredOptions();
    }
}
