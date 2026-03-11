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

namespace Apache.Arrow.Serialization;

/// <summary>
/// Marks a type for Arrow serialization. The source generator will emit
/// schema derivation, serialization, and deserialization code.
/// Supports record, record struct, class, and struct types.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class ArrowSerializableAttribute : Attribute;

/// <summary>
/// Overrides the field name and/or order for Arrow serialization.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = false)]
public sealed class ArrowFieldAttribute : Attribute
{
    /// <summary>
    /// The Arrow field name. If null, the property name is used.
    /// </summary>
    public string? Name { get; }

    /// <summary>
    /// The field order in the Arrow schema. Lower values come first.
    /// If not set, properties are ordered by declaration order.
    /// </summary>
    public int Order { get; set; } = int.MaxValue;

    public ArrowFieldAttribute() { }

    public ArrowFieldAttribute(string name)
    {
        Name = name;
    }
}

/// <summary>
/// Overrides the inferred Arrow type for a property.
/// The type parameter must be a static property or field returning an Apache.Arrow IArrowType.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field | AttributeTargets.Parameter, Inherited = false)]
public sealed class ArrowTypeAttribute : Attribute
{
    /// <summary>
    /// A well-known Arrow type name (e.g. "int32", "float32", "binary", "decimal128(38,18)").
    /// </summary>
    public string? TypeName { get; }

    /// <summary>
    /// A custom converter type implementing IArrowConverter&lt;T&gt; for the property type.
    /// The converter must have a parameterless constructor.
    /// </summary>
    public Type? Converter { get; set; }

    /// <summary>
    /// Overrides the Arrow type of the element in a List, Array, or HashSet property.
    /// E.g. <c>[ArrowType(ElementType = "string_view")]</c> on a <c>List&lt;string&gt;</c>.
    /// </summary>
    public string? ElementType { get; set; }

    /// <summary>
    /// Overrides the Arrow type of the key in a Dictionary property.
    /// E.g. <c>[ArrowType(KeyType = "string_view")]</c> on a <c>Dictionary&lt;string, int&gt;</c>.
    /// </summary>
    public string? KeyType { get; set; }

    /// <summary>
    /// Overrides the Arrow type of the value in a Dictionary property.
    /// E.g. <c>[ArrowType(ValueType = "timestamp[ns, UTC]")]</c> on a <c>Dictionary&lt;string, DateTime&gt;</c>.
    /// </summary>
    public string? ValueType { get; set; }

    public ArrowTypeAttribute(string typeName)
    {
        TypeName = typeName;
    }

    public ArrowTypeAttribute()
    {
    }
}

/// <summary>
/// Adds key-value metadata to the Arrow schema (on class) or field (on property).
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true, Inherited = false)]
public sealed class ArrowMetadataAttribute : Attribute
{
    public string Key { get; }
    public string Value { get; }

    public ArrowMetadataAttribute(string key, string value)
    {
        Key = key;
        Value = value;
    }
}

/// <summary>
/// Excludes a property from Arrow serialization. The property must have a default value.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = false)]
public sealed class ArrowIgnoreAttribute : Attribute;

/// <summary>
/// Marks a base type (abstract record or interface) for polymorphic Arrow serialization.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, Inherited = false)]
public sealed class ArrowPolymorphicAttribute : Attribute
{
    /// <summary>
    /// The name of the type discriminator field in the Arrow schema.
    /// Defaults to "$type".
    /// </summary>
    public string TypeDiscriminatorFieldName { get; set; } = "$type";
}

/// <summary>
/// Registers a derived type for polymorphic Arrow serialization on the base type.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, AllowMultiple = true, Inherited = false)]
public sealed class ArrowDerivedTypeAttribute : Attribute
{
    /// <summary>
    /// The derived type to register.
    /// </summary>
    public Type DerivedType { get; }

    /// <summary>
    /// The type discriminator value (string tag) for this derived type.
    /// </summary>
    public string TypeDiscriminator { get; }

    public ArrowDerivedTypeAttribute(Type derivedType, string typeDiscriminator)
    {
        DerivedType = derivedType;
        TypeDiscriminator = typeDiscriminator;
    }
}

/// <summary>
/// Implement this interface to receive callbacks during Arrow serialization/deserialization.
/// <see cref="OnBeforeSerialize"/> is called before the object is serialized to a RecordBatch.
/// <see cref="OnAfterDeserialize"/> is called after the object is deserialized from a RecordBatch.
/// </summary>
public interface IArrowSerializationCallback
{
    /// <summary>
    /// Called before the object is serialized to a RecordBatch.
    /// Use this to compute derived fields, flush lazy state, or validate before serialization.
    /// </summary>
    void OnBeforeSerialize();

    /// <summary>
    /// Called after the object is deserialized from a RecordBatch.
    /// Use this to rebuild computed/cached fields, initialize transient state, or validate invariants.
    /// </summary>
    void OnAfterDeserialize();
}
