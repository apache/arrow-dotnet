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
    /// Reconstructs <see cref="VariantValue"/>s from shredded (value, typed_value) pairs
    /// according to a <see cref="ShredSchema"/>. The residual <see cref="ShredResult.Value"/>
    /// bytes are interpreted against the column-level variant metadata supplied to
    /// <see cref="Reconstruct(ShredResult, ShredSchema, ReadOnlySpan{byte})"/>.
    /// </summary>
    public static class VariantUnshredder
    {
        /// <summary>
        /// Reconstructs a variant value from a shredded result.
        /// </summary>
        /// <param name="shredded">The shredded (value, typed_value) pair.</param>
        /// <param name="schema">The shredding schema that was used to produce the result.</param>
        /// <param name="metadata">The column-level variant metadata bytes.</param>
        /// <returns>
        /// The reconstructed <see cref="VariantValue"/>, or null if the field is missing
        /// (both value and typed_value are null).
        /// </returns>
        public static VariantValue? Reconstruct(ShredResult shredded, ShredSchema schema, ReadOnlySpan<byte> metadata)
        {
            if (shredded == null) throw new ArgumentNullException(nameof(shredded));
            if (schema == null) throw new ArgumentNullException(nameof(schema));

            if (shredded.IsMissing)
            {
                return null;
            }

            switch (schema.TypedValueType)
            {
                case ShredType.Object:
                    return ReconstructObject(shredded, schema, metadata);
                case ShredType.Array:
                    return ReconstructArray(shredded, schema, metadata);
                case ShredType.None:
                    return DecodeValue(metadata, shredded.Value);
                default:
                    return ReconstructPrimitive(shredded, schema, metadata);
            }
        }

        private static VariantValue ReconstructPrimitive(ShredResult shredded, ShredSchema schema, ReadOnlySpan<byte> metadata)
        {
            if (shredded.TypedValue != null)
            {
                return CreateVariantFromTyped(shredded.TypedValue, schema.TypedValueType);
            }
            return DecodeValue(metadata, shredded.Value);
        }

        private static VariantValue ReconstructObject(ShredResult shredded, ShredSchema schema, ReadOnlySpan<byte> metadata)
        {
            if (shredded.TypedValue == null)
            {
                // Source value wasn't an object — the whole thing is in the residual.
                return DecodeValue(metadata, shredded.Value);
            }

            ShredObjectResult objectResult = (ShredObjectResult)shredded.TypedValue;
            Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>();

            foreach (KeyValuePair<string, ShredResult> fieldEntry in objectResult.Fields)
            {
                if (!schema.ObjectFields.TryGetValue(fieldEntry.Key, out ShredSchema fieldSchema))
                {
                    throw new InvalidOperationException(
                        $"Shredded object contains field '{fieldEntry.Key}' not in schema.");
                }

                VariantValue? fieldValue = Reconstruct(fieldEntry.Value, fieldSchema, metadata);
                if (fieldValue.HasValue)
                {
                    fields[fieldEntry.Key] = fieldValue.Value;
                }
                // If null (missing), the field is omitted from the result.
            }

            if (shredded.Value != null)
            {
                VariantValue residual = DecodeValue(metadata, shredded.Value);
                if (!residual.IsObject)
                {
                    throw new InvalidOperationException(
                        "Residual value for a partially shredded object must be an object.");
                }
                foreach (KeyValuePair<string, VariantValue> residualField in residual.AsObject())
                {
                    fields[residualField.Key] = residualField.Value;
                }
            }

            return VariantValue.FromObject(fields);
        }

        private static VariantValue ReconstructArray(ShredResult shredded, ShredSchema schema, ReadOnlySpan<byte> metadata)
        {
            if (shredded.TypedValue == null)
            {
                return DecodeValue(metadata, shredded.Value);
            }

            ShredArrayResult arrayResult = (ShredArrayResult)shredded.TypedValue;
            List<VariantValue> elements = new List<VariantValue>(arrayResult.Elements.Count);

            for (int i = 0; i < arrayResult.Elements.Count; i++)
            {
                VariantValue? elementValue = Reconstruct(arrayResult.Elements[i], schema.ArrayElement, metadata);
                if (!elementValue.HasValue)
                {
                    throw new InvalidOperationException(
                        $"Array element at index {i} is missing, but array elements cannot be missing.");
                }
                elements.Add(elementValue.Value);
            }

            return VariantValue.FromArray(elements);
        }

        private static VariantValue DecodeValue(ReadOnlySpan<byte> metadata, byte[] value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
            VariantReader reader = new VariantReader(metadata, value);
            return reader.ToVariantValue();
        }

        /// <summary>
        /// Creates a <see cref="VariantValue"/> from a typed CLR value and its <see cref="ShredType"/>.
        /// </summary>
        internal static VariantValue CreateVariantFromTyped(object typedValue, ShredType shredType)
        {
            switch (shredType)
            {
                case ShredType.Boolean: return VariantValue.FromBoolean((bool)typedValue);
                case ShredType.Int8: return VariantValue.FromInt8((sbyte)typedValue);
                case ShredType.Int16: return VariantValue.FromInt16((short)typedValue);
                case ShredType.Int32: return VariantValue.FromInt32((int)typedValue);
                case ShredType.Int64: return VariantValue.FromInt64((long)typedValue);
                case ShredType.Float: return VariantValue.FromFloat((float)typedValue);
                case ShredType.Double: return VariantValue.FromDouble((double)typedValue);
                case ShredType.Decimal4: return VariantValue.FromDecimal4((decimal)typedValue);
                case ShredType.Decimal8: return VariantValue.FromDecimal8((decimal)typedValue);
                case ShredType.Decimal16: return VariantValue.FromDecimal16((decimal)typedValue);
                case ShredType.Date: return VariantValue.FromDate((int)typedValue);
                case ShredType.Timestamp: return VariantValue.FromTimestamp((long)typedValue);
                case ShredType.TimestampNtz: return VariantValue.FromTimestampNtz((long)typedValue);
                case ShredType.TimeNtz: return VariantValue.FromTimeNtz((long)typedValue);
                case ShredType.TimestampTzNanos: return VariantValue.FromTimestampTzNanos((long)typedValue);
                case ShredType.TimestampNtzNanos: return VariantValue.FromTimestampNtzNanos((long)typedValue);
                case ShredType.String: return VariantValue.FromString((string)typedValue);
                case ShredType.Binary: return VariantValue.FromBinary((byte[])typedValue);
                case ShredType.Uuid: return VariantValue.FromUuid((Guid)typedValue);
                default:
                    throw new InvalidOperationException($"Cannot create VariantValue for ShredType.{shredType}.");
            }
        }
    }
}
