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

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// The result of shredding a single variant value: a (value, typed_value) pair.
    /// <para>
    /// Follows the Parquet variant shredding spec encoding matrix:
    /// <list type="bullet">
    /// <item>Both null → missing (only valid for object sub-fields)</item>
    /// <item>value non-null, typed_value null → unshredded (value in binary)</item>
    /// <item>value null, typed_value non-null → fully shredded into typed column</item>
    /// <item>Both non-null → partially shredded object (typed_value has shredded fields, value has residual)</item>
    /// </list>
    /// </para>
    /// </summary>
    public sealed class ShredResult
    {
        /// <summary>
        /// The residual variant value bytes. These reference the column-level metadata
        /// returned by <see cref="VariantShredder.Shred(IEnumerable{Scalars.Variant.VariantValue}, ShredSchema)"/>;
        /// they are NOT self-contained. Non-null when the value (or part of it) could
        /// not be shredded into the typed column. For partially shredded objects this
        /// contains only the unshredded fields.
        /// </summary>
        public byte[] Value { get; }

        /// <summary>
        /// The typed value extracted according to the schema. The runtime type depends
        /// on the <see cref="ShredSchema.TypedValueType"/>:
        /// <list type="bullet">
        /// <item>Primitives: the corresponding CLR type (bool, int, long, double, string, etc.)</item>
        /// <item>Object: <see cref="ShredObjectResult"/></item>
        /// <item>Array: <see cref="ShredArrayResult"/></item>
        /// </list>
        /// Null when the value does not match the schema type (falls back to binary).
        /// </summary>
        public object TypedValue { get; }

        /// <summary>
        /// True when both <see cref="Value"/> and <see cref="TypedValue"/> are null,
        /// indicating the field is missing (only valid for object sub-fields).
        /// </summary>
        public bool IsMissing => Value == null && TypedValue == null;

        /// <summary>Creates a shred result.</summary>
        public ShredResult(byte[] value, object typedValue)
        {
            Value = value;
            TypedValue = typedValue;
        }

        /// <summary>A missing result (both null).</summary>
        public static readonly ShredResult Missing = new ShredResult(null, null);
    }

    /// <summary>
    /// The typed_value result for a shredded object. Contains one <see cref="ShredResult"/>
    /// per field defined in the object's <see cref="ShredSchema"/>.
    /// </summary>
    public sealed class ShredObjectResult
    {
        /// <summary>
        /// Shredded fields, keyed by field name matching the <see cref="ShredSchema.ObjectFields"/>.
        /// Each entry is the shredded (value, typed_value) pair for that field.
        /// </summary>
        public IReadOnlyDictionary<string, ShredResult> Fields { get; }

        /// <summary>Creates a shredded object result.</summary>
        public ShredObjectResult(IReadOnlyDictionary<string, ShredResult> fields)
        {
            Fields = fields;
        }
    }

    /// <summary>
    /// The typed_value result for a shredded array. Contains one <see cref="ShredResult"/>
    /// per element in the source array.
    /// </summary>
    public sealed class ShredArrayResult
    {
        /// <summary>
        /// Shredded elements. Each entry is the shredded (value, typed_value) pair for that element.
        /// Array elements are never missing — null elements are encoded as variant null in the value column.
        /// </summary>
        public IReadOnlyList<ShredResult> Elements { get; }

        /// <summary>Creates a shredded array result.</summary>
        public ShredArrayResult(IReadOnlyList<ShredResult> elements)
        {
            Elements = elements;
        }
    }
}
