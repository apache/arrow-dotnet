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

#nullable enable

namespace Apache.Arrow.Serialization.Generator
{
    internal enum TypeKind2
    {
        Unknown,
        String,
        Bool,
        Byte,
        SByte,
        Int16,
        UInt16,
        Int32,
        UInt32,
        Int64,
        UInt64,
        Float,
        Double,
        Decimal,
        Binary,
        DateTime,
        DateTimeOffset,
        DateOnly,
        TimeOnly,
        TimeSpan,
        Guid,
        Half,
        Enum,
        List,
        Array,
        Dictionary,
        Set,
        NestedRecord,
        Custom,
    }

    internal class TypeInfo
    {
        public TypeKind2 Kind { get; set; }
        public string FullTypeName { get; set; } = "";
        public bool IsNullable { get; set; }
        public string? ArrowTypeOverride { get; set; }
        public TypeInfo? ElementType { get; set; }
        public TypeInfo? KeyType { get; set; }
        public TypeInfo? ValueType { get; set; }

        public TypeInfo WithOverride(string arrowTypeOverride)
        {
            return new TypeInfo
            {
                Kind = Kind,
                FullTypeName = FullTypeName,
                IsNullable = IsNullable,
                ArrowTypeOverride = arrowTypeOverride,
                ElementType = ElementType,
                KeyType = KeyType,
                ValueType = ValueType,
            };
        }
    }

    internal class PropertyModel
    {
        public string PropertyName { get; set; } = "";
        public string FieldName { get; set; } = "";
        public int Order { get; set; } = int.MaxValue;
        public int DeclOrder { get; set; }
        public TypeInfo Type { get; set; } = new TypeInfo();
        public bool IsNullable { get; set; }
        public bool HasDefaultValue { get; set; }
        public List<KeyValuePair<string, string>> Metadata { get; set; } = new List<KeyValuePair<string, string>>();
        /// <summary>Fully qualified type name of the IArrowConverter implementation, if specified.</summary>
        public string? ConverterTypeName { get; set; }
        /// <summary>True if this member is a field (not a property). Fields require constructor-based deserialization.</summary>
        public bool IsField { get; set; }
    }

    internal class ConstructorParamModel
    {
        public string Name { get; set; } = "";
        public bool HasDefaultValue { get; set; }
    }

    internal class DiagnosticInfo
    {
        public string Id { get; set; } = "";
        public string Message { get; set; } = "";
        public bool IsError { get; set; }
    }

    internal class TypeModel
    {
        public string? Namespace { get; set; }
        public string TypeName { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public List<PropertyModel> Properties { get; set; } = new List<PropertyModel>();
        public bool IsRecord { get; set; }
        public bool IsValueType { get; set; }
        public List<KeyValuePair<string, string>> Metadata { get; set; } = new List<KeyValuePair<string, string>>();
        public bool HasArrowSerializableBase { get; set; }
        /// <summary>Constructor to use for deserialization. Null means use object initializer.</summary>
        public List<ConstructorParamModel>? ConstructorParams { get; set; }
        /// <summary>True if the type implements IArrowSerializationCallback.</summary>
        public bool HasSerializationCallback { get; set; }
        public List<DiagnosticInfo> Diagnostics { get; set; } = new List<DiagnosticInfo>();
    }

    internal class DerivedTypeInfo
    {
        public string TypeDiscriminator { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public string TypeName { get; set; } = "";
        public List<PropertyModel> Properties { get; set; } = new List<PropertyModel>();
        public bool IsRecord { get; set; }
        public bool IsValueType { get; set; }
    }

    internal class PolymorphicModel
    {
        public string? Namespace { get; set; }
        public string TypeName { get; set; } = "";
        public string FullTypeName { get; set; } = "";
        public string TypeDiscriminatorFieldName { get; set; } = "$type";
        public bool IsInterface { get; set; }
        public bool IsRecord { get; set; }
        public List<DerivedTypeInfo> DerivedTypes { get; set; } = new List<DerivedTypeInfo>();
        /// <summary>
        /// Union of all derived type properties (all made nullable). Deduplicated by FieldName.
        /// </summary>
        public List<PropertyModel> UnionProperties { get; set; } = new List<PropertyModel>();
        public List<KeyValuePair<string, string>> Metadata { get; set; } = new List<KeyValuePair<string, string>>();
    }
}
