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
using System.Collections.Generic;
using Apache.Arrow.Scalars.Variant;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Analyzes a batch of <see cref="VariantValue"/>s and infers an optimal
    /// <see cref="ShredSchema"/> for shredding them.
    /// </summary>
    public sealed class ShredSchemaInferer
    {
        /// <summary>
        /// Infers a shredding schema by analyzing the given values.
        /// </summary>
        /// <param name="values">The variant values to analyze.</param>
        /// <param name="options">Options controlling depth, frequency, and type consistency thresholds.</param>
        /// <returns>An inferred <see cref="ShredSchema"/>.</returns>
        public ShredSchema Infer(IEnumerable<VariantValue> values, ShredOptions options = null)
        {
            if (values == null) throw new ArgumentNullException(nameof(values));
            if (options == null) options = ShredOptions.Default;

            TypeStats stats = new TypeStats();
            int totalCount = 0;

            foreach (VariantValue value in values)
            {
                CollectStats(value, stats, 0, options.MaxDepth);
                totalCount++;
            }

            if (totalCount == 0)
            {
                return ShredSchema.Unshredded();
            }

            return BuildSchema(stats, totalCount, options, 0);
        }

        private void CollectStats(VariantValue value, TypeStats stats, int depth, int maxDepth)
        {
            ShredType type = VariantShredder.GetShredType(value);
            stats.TypeCounts.TryGetValue(type, out int count);
            stats.TypeCounts[type] = count + 1;

            if (type == ShredType.Object && depth <= maxDepth && value.IsObject)
            {
                if (stats.ObjectFieldStats == null)
                {
                    stats.ObjectFieldStats = new Dictionary<string, TypeStats>();
                }

                foreach (KeyValuePair<string, VariantValue> field in value.AsObject())
                {
                    if (!stats.ObjectFieldStats.TryGetValue(field.Key, out TypeStats fieldStats))
                    {
                        fieldStats = new TypeStats();
                        stats.ObjectFieldStats[field.Key] = fieldStats;
                    }
                    CollectStats(field.Value, fieldStats, depth + 1, maxDepth);
                }
            }
            else if (type == ShredType.Array && depth <= maxDepth && value.IsArray)
            {
                if (stats.ArrayElementStats == null)
                {
                    stats.ArrayElementStats = new TypeStats();
                }

                foreach (VariantValue element in value.AsArray())
                {
                    CollectStats(element, stats.ArrayElementStats, depth + 1, maxDepth);
                }
            }
        }

        private ShredSchema BuildSchema(TypeStats stats, int totalCount, ShredOptions options, int depth)
        {
            // Find the dominant type.
            ShredType dominantType = ShredType.None;
            int dominantCount = 0;
            int nonNullCount = 0;

            foreach (KeyValuePair<ShredType, int> entry in stats.TypeCounts)
            {
                if (entry.Key != ShredType.None)
                {
                    nonNullCount += entry.Value;
                    if (entry.Value > dominantCount)
                    {
                        dominantCount = entry.Value;
                        dominantType = entry.Key;
                    }
                }
            }

            if (nonNullCount == 0)
            {
                return ShredSchema.Unshredded();
            }

            // Check type consistency.
            double consistency = (double)dominantCount / nonNullCount;
            if (consistency < options.MinTypeConsistency)
            {
                return ShredSchema.Unshredded();
            }

            if (dominantType == ShredType.Object && stats.ObjectFieldStats != null)
            {
                return BuildObjectSchema(stats, totalCount, dominantCount, options, depth);
            }

            if (dominantType == ShredType.Array && stats.ArrayElementStats != null)
            {
                return BuildArraySchema(stats, dominantCount, options, depth);
            }

            // Object/Array without collected sub-stats (e.g., maxDepth reached) — can't shred further.
            if (dominantType == ShredType.Object || dominantType == ShredType.Array)
            {
                return ShredSchema.Unshredded();
            }

            // Primitive type — shred as that type.
            return ShredSchema.Primitive(dominantType);
        }

        private ShredSchema BuildObjectSchema(TypeStats stats, int totalCount, int objectCount, ShredOptions options, int depth)
        {
            Dictionary<string, ShredSchema> fields = new Dictionary<string, ShredSchema>();

            foreach (KeyValuePair<string, TypeStats> fieldEntry in stats.ObjectFieldStats)
            {
                // Check field frequency: how often does this field appear relative to the number of objects?
                int fieldAppearances = 0;
                foreach (KeyValuePair<ShredType, int> tc in fieldEntry.Value.TypeCounts)
                {
                    fieldAppearances += tc.Value;
                }

                double frequency = (double)fieldAppearances / objectCount;
                if (frequency < options.MinFieldFrequency)
                {
                    continue;
                }

                ShredSchema fieldSchema = BuildSchema(fieldEntry.Value, fieldAppearances, options, depth + 1);
                fields[fieldEntry.Key] = fieldSchema;
            }

            if (fields.Count == 0)
            {
                return ShredSchema.Unshredded();
            }

            return ShredSchema.ForObject(fields);
        }

        private ShredSchema BuildArraySchema(TypeStats stats, int arrayCount, ShredOptions options, int depth)
        {
            // Count total elements across all arrays.
            int totalElements = 0;
            foreach (KeyValuePair<ShredType, int> entry in stats.ArrayElementStats.TypeCounts)
            {
                totalElements += entry.Value;
            }

            if (totalElements == 0)
            {
                return ShredSchema.Unshredded();
            }

            ShredSchema elementSchema = BuildSchema(stats.ArrayElementStats, totalElements, options, depth + 1);
            return ShredSchema.ForArray(elementSchema);
        }

        private sealed class TypeStats
        {
            public Dictionary<ShredType, int> TypeCounts { get; } = new Dictionary<ShredType, int>();
            public Dictionary<string, TypeStats> ObjectFieldStats { get; set; }
            public TypeStats ArrayElementStats { get; set; }
        }
    }
}
