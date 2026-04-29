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
using Apache.Arrow;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;

namespace Apache.Arrow.Operations.Shredding
{
    /// <summary>
    /// Reader for a single row of a shredded-object slot. Provides field-wise
    /// access to both typed sub-columns and residual unshredded fields.
    /// </summary>
    public ref struct ShreddedObject
    {
        private readonly ShredSchema _schema;
        private readonly ReadOnlySpan<byte> _metadata;
        // The typed_value struct (one field per shredded field, each itself a {value, typed_value} struct).
        // May be null if this row's typed_value column is null (i.e., the whole slot is in residual).
        private readonly StructArray _fields;
        // The residual value at this level (a binary column holding unshredded fields). May be null.
        private readonly IArrowArray _residual;
        private readonly int _index;

        internal ShreddedObject(
            ShredSchema schema,
            ReadOnlySpan<byte> metadata,
            StructArray typedValueStruct,
            IArrowArray residualValue,
            int index)
        {
            _schema = schema;
            _metadata = metadata;
            _fields = typedValueStruct;
            _residual = residualValue;
            _index = index;
        }

        /// <summary>The names of the shredded fields, in schema order.</summary>
        public IEnumerable<string> FieldNames => _schema.ObjectFields.Keys;

        /// <summary>
        /// Gets the shredded reader for a named field. The field must exist in the schema.
        /// </summary>
        /// <exception cref="KeyNotFoundException">If <paramref name="name"/> is not a shredded field.</exception>
        public ShreddedVariant GetField(string name)
        {
            if (!TryGetField(name, out ShreddedVariant field))
            {
                throw new KeyNotFoundException($"Field '{name}' is not in the shredded object schema.");
            }
            return field;
        }

        /// <summary>
        /// Tries to get a reader for a shredded sub-field by name. Returns false if
        /// <paramref name="name"/> isn't a shredded field (it may still exist in the
        /// residual — use <see cref="TryGetResidualReader"/> to inspect).
        /// </summary>
        public bool TryGetField(string name, out ShreddedVariant field)
        {
            if (!_schema.ObjectFields.TryGetValue(name, out ShredSchema fieldSchema))
            {
                field = default;
                return false;
            }
            if (_fields == null || _fields.IsNull(_index))
            {
                // typed_value is null at this row — the field is effectively missing
                // from the typed column. Return a slot with no typed/residual set.
                field = new ShreddedVariant(fieldSchema, _metadata, null, null, _index);
                return true;
            }
            StructType fieldsStructType = (StructType)_fields.Data.DataType;
            int fieldIdx = fieldsStructType.GetFieldIndex(name);
            StructArray elementGroup = (StructArray)_fields.Fields[fieldIdx];
            field = ShreddingHelpers.BuildSlot(fieldSchema, _metadata, elementGroup, _index);
            return true;
        }

        /// <summary>
        /// If the object's residual binary is populated at this row, returns a
        /// <see cref="VariantReader"/> over it. The residual holds whatever fields
        /// were not shredded (or, for a non-object row, the whole value).
        /// </summary>
        public bool TryGetResidualReader(out VariantReader reader)
        {
            if (_residual == null || _residual.IsNull(_index))
            {
                reader = default;
                return false;
            }
            ReadOnlySpan<byte> bytes = ((BinaryArray)_residual).GetBytes(_index, out _);
            reader = new VariantReader(_metadata, bytes);
            return true;
        }

        /// <summary>
        /// Materializes the whole shredded object into a <see cref="VariantValue"/>,
        /// merging typed-column fields with residual unshredded fields. When the
        /// <c>typed_value</c> column is null at this row, the residual is returned
        /// as-is (it may be any variant type, not just an object).
        /// </summary>
        public VariantValue ToVariantValue()
        {
            bool typedPopulated = _fields != null && !_fields.IsNull(_index);
            bool residualPopulated = _residual != null && !_residual.IsNull(_index);

            if (!typedPopulated && !residualPopulated)
            {
                return VariantValue.Null;
            }

            // No shredded fields at this row — whatever is in the residual IS the value.
            if (!typedPopulated)
            {
                BinaryArray binary = (BinaryArray)_residual;
                ReadOnlySpan<byte> bytes = binary.GetBytes(_index, out _);
                return new VariantReader(_metadata, bytes).ToVariantValue();
            }

            Dictionary<string, VariantValue> fields = new Dictionary<string, VariantValue>();

            // Shredded fields (from typed_value).
            StructType fieldsStructType = (StructType)_fields.Data.DataType;
            foreach (KeyValuePair<string, ShredSchema> entry in _schema.ObjectFields)
            {
                int fieldIdx = fieldsStructType.GetFieldIndex(entry.Key);
                StructArray elementGroup = (StructArray)_fields.Fields[fieldIdx];
                ShreddedVariant slot = ShreddingHelpers.BuildSlot(entry.Value, _metadata, elementGroup, _index);
                if (!slot.IsMissing)
                {
                    fields[entry.Key] = slot.ToVariantValue();
                }
            }

            // Partially shredded object — merge residual unshredded fields.
            if (residualPopulated)
            {
                BinaryArray residualBinary = (BinaryArray)_residual;
                ReadOnlySpan<byte> residualBytes = residualBinary.GetBytes(_index, out _);
                VariantReader residualReader = new VariantReader(_metadata, residualBytes);
                if (!residualReader.IsObject)
                {
                    throw new InvalidOperationException(
                        "Residual value for a partially shredded object must itself be a variant object.");
                }
                VariantValue residual = residualReader.ToVariantValue();
                foreach (KeyValuePair<string, VariantValue> kv in residual.AsObject())
                {
                    fields[kv.Key] = kv.Value;
                }
            }

            return VariantValue.FromObject(fields);
        }
    }
}
