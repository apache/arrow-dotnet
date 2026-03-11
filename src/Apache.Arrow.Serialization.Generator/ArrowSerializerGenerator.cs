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
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

#nullable enable

namespace Apache.Arrow.Serialization.Generator
{

[Generator(LanguageNames.CSharp)]
public class ArrowSerializerGenerator : IIncrementalGenerator
{
    private static readonly DiagnosticDescriptor NonPartialType = new DiagnosticDescriptor(
        "ARROW001", "Type must be partial",
        "[ArrowSerializable] type '{0}' must be declared as partial",
        "ArrowSerialization", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor NoMatchingConstructor = new DiagnosticDescriptor(
        "ARROW002", "No matching constructor for readonly fields",
        "[ArrowSerializable] type '{0}' has readonly fields but no public constructor with matching parameters",
        "ArrowSerialization", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor UnsupportedMemberType = new DiagnosticDescriptor(
        "ARROW003", "Unsupported member type",
        "Member '{0}' on type '{1}' has unsupported type '{2}'",
        "ArrowSerialization", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor DuplicateFieldName = new DiagnosticDescriptor(
        "ARROW004", "Duplicate Arrow field name",
        "Members '{0}' and '{1}' on type '{2}' both map to Arrow field name '{3}'",
        "ArrowSerialization", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor NonSettableProperty = new DiagnosticDescriptor(
        "ARROW005", "Property is not settable",
        "Property '{0}' on type '{1}' has no set or init accessor and cannot be deserialized",
        "ArrowSerialization", DiagnosticSeverity.Error, true);

    private static readonly DiagnosticDescriptor ArrowAttributeOnIgnoredMember = new DiagnosticDescriptor(
        "ARROW006", "Arrow attribute on non-serialized member",
        "Member '{0}' on type '{1}' has Arrow attributes but is not serialized because it is {2}",
        "ArrowSerialization", DiagnosticSeverity.Warning, true);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var emitSchemaJson = context.AnalyzerConfigOptionsProvider.Select(static (options, _) =>
        {
            options.GlobalOptions.TryGetValue("build_property.ArrowSerializerEmitSchemaJson", out var value);
            return string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
        });

        var types = context.SyntaxProvider.ForAttributeWithMetadataName(
            "Apache.Arrow.Serialization.ArrowSerializableAttribute",
            predicate: static (node, _) => node is RecordDeclarationSyntax || node is ClassDeclarationSyntax || node is StructDeclarationSyntax,
            transform: static (ctx, _) => GetTypeModel(ctx))
            .Where(static m => m != null)
            .Select(static (m, _) => m!);

        var typesWithConfig = types.Combine(emitSchemaJson);
        context.RegisterSourceOutput(typesWithConfig, static (spc, pair) => Execute(spc, pair.Left, pair.Right));

        var polyTypes = context.SyntaxProvider.ForAttributeWithMetadataName(
            "Apache.Arrow.Serialization.ArrowPolymorphicAttribute",
            predicate: static (node, _) => node is RecordDeclarationSyntax || node is ClassDeclarationSyntax || node is StructDeclarationSyntax || node is InterfaceDeclarationSyntax,
            transform: static (ctx, _) => GetPolymorphicModel(ctx))
            .Where(static m => m != null)
            .Select(static (m, _) => m!);

        context.RegisterSourceOutput(polyTypes, static (spc, model) => ExecutePolymorphic(spc, model));
    }

    private static TypeModel? GetTypeModel(GeneratorAttributeSyntaxContext ctx)
    {
        if (!(ctx.TargetSymbol is INamedTypeSymbol typeSymbol))
            return null;

        var properties = new List<PropertyModel>();
        var ignoredMemberWarnings = new List<DiagnosticInfo>();
        int declOrder = 0;
        foreach (var member in typeSymbol.GetMembers())
        {
            // Support both properties and fields
            ITypeSymbol memberType;
            string memberName;
            bool isField = false;
            if (member is IPropertySymbol prop)
            {
                string? skipReason = null;
                if (prop.IsStatic) skipReason = "static";
                else if (prop.IsIndexer) skipReason = "an indexer";
                else if (prop.DeclaredAccessibility != Accessibility.Public) skipReason = "not public";

                if (skipReason != null)
                {
                    if (HasArrowAttributes(prop))
                        ignoredMemberWarnings.Add(new DiagnosticInfo { Id = "ARROW006", Message = $"{prop.Name}\t{typeSymbol.Name}\t{skipReason}", IsError = false });
                    continue;
                }
                memberType = prop.Type;
                memberName = prop.Name;
            }
            else if (member is IFieldSymbol field)
            {
                if (field.IsImplicitlyDeclared)
                    continue;

                string? skipReason = null;
                if (field.IsStatic || field.IsConst) skipReason = "static or const";
                else if (field.DeclaredAccessibility != Accessibility.Public) skipReason = "not public";

                if (skipReason != null)
                {
                    if (HasArrowAttributes(field))
                        ignoredMemberWarnings.Add(new DiagnosticInfo { Id = "ARROW006", Message = $"{field.Name}\t{typeSymbol.Name}\t{skipReason}", IsError = false });
                    continue;
                }
                memberType = field.Type;
                memberName = field.Name;
                isField = true;
            }
            else
            {
                continue;
            }

            // Check for [ArrowIgnore]
            bool isTransient = false;
            string? arrowTypeName = null;
            string? converterTypeName = null;
            string? elementTypeOverride = null;
            string? keyTypeOverride = null;
            string? valueTypeOverride = null;
            string? fieldName = null;
            int order = int.MaxValue;
            var propMetadata = new List<KeyValuePair<string, string>>();

            foreach (var attr in member.GetAttributes())
            {
                var attrName = attr.AttributeClass?.ToDisplayString();
                if (attrName == "Apache.Arrow.Serialization.ArrowIgnoreAttribute")
                {
                    isTransient = true;
                }
                else if (attrName == "Apache.Arrow.Serialization.ArrowTypeAttribute")
                {
                    if (attr.ConstructorArguments.Length > 0)
                        arrowTypeName = attr.ConstructorArguments[0].Value as string;
                    foreach (var named in attr.NamedArguments)
                    {
                        if (named.Key == "Converter" && named.Value.Value is INamedTypeSymbol converterSym)
                            converterTypeName = converterSym.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                        else if (named.Key == "ElementType" && named.Value.Value is string et)
                            elementTypeOverride = et;
                        else if (named.Key == "KeyType" && named.Value.Value is string kt)
                            keyTypeOverride = kt;
                        else if (named.Key == "ValueType" && named.Value.Value is string vt)
                            valueTypeOverride = vt;
                    }
                }
                else if (attrName == "Apache.Arrow.Serialization.ArrowFieldAttribute")
                {
                    if (attr.ConstructorArguments.Length > 0)
                        fieldName = attr.ConstructorArguments[0].Value as string;
                    foreach (var named in attr.NamedArguments)
                    {
                        if (named.Key == "Order" && named.Value.Value is int o)
                            order = o;
                    }
                }
                else if (attrName == "Apache.Arrow.Serialization.ArrowMetadataAttribute")
                {
                    if (attr.ConstructorArguments.Length >= 2)
                    {
                        var key = attr.ConstructorArguments[0].Value as string;
                        var val = attr.ConstructorArguments[1].Value as string;
                        if (key != null && val != null)
                            propMetadata.Add(new KeyValuePair<string, string>(key, val));
                    }
                }
            }

            if (isTransient)
                continue;

            bool isNullable = memberType.NullableAnnotation == NullableAnnotation.Annotated;
            if (isNullable && memberType is INamedTypeSymbol nullableType && nullableType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
            {
                memberType = nullableType.TypeArguments[0];
            }

            var typeInfo = AnalyzeType(memberType, isNullable);
            if (converterTypeName != null)
                typeInfo = new TypeInfo { Kind = TypeKind2.Custom, FullTypeName = typeInfo.FullTypeName, IsNullable = isNullable };
            else if (arrowTypeName != null)
                typeInfo = typeInfo.WithOverride(arrowTypeName);

            // Apply element/key/value type overrides for collections
            if (elementTypeOverride != null && typeInfo.ElementType != null)
                typeInfo.ElementType = typeInfo.ElementType.WithOverride(elementTypeOverride);
            if (keyTypeOverride != null && typeInfo.KeyType != null)
                typeInfo.KeyType = typeInfo.KeyType.WithOverride(keyTypeOverride);
            if (valueTypeOverride != null && typeInfo.ValueType != null)
                typeInfo.ValueType = typeInfo.ValueType.WithOverride(valueTypeOverride);

            properties.Add(new PropertyModel
            {
                PropertyName = memberName,
                FieldName = fieldName ?? memberName,
                Order = order,
                DeclOrder = declOrder++,
                Type = typeInfo,
                IsNullable = isNullable,
                HasDefaultValue = isField ? false : HasDefaultValue((IPropertySymbol)member, typeSymbol),
                Metadata = propMetadata,
                ConverterTypeName = converterTypeName,
                IsField = isField,
            });
        }

        properties.Sort((a, b) =>
        {
            int cmp = a.Order.CompareTo(b.Order);
            return cmp != 0 ? cmp : a.DeclOrder.CompareTo(b.DeclOrder);
        });

        var typeMetadata = new List<KeyValuePair<string, string>>();
        foreach (var attr in typeSymbol.GetAttributes())
        {
            if (attr.AttributeClass?.ToDisplayString() == "Apache.Arrow.Serialization.ArrowMetadataAttribute"
                && attr.ConstructorArguments.Length >= 2)
            {
                var key = attr.ConstructorArguments[0].Value as string;
                var val = attr.ConstructorArguments[1].Value as string;
                if (key != null && val != null)
                    typeMetadata.Add(new KeyValuePair<string, string>(key, val));
            }
        }

        // Determine if constructor-based deserialization is needed
        // This is required when any member is a readonly field (can't use object initializer)
        bool needsConstructor = false;
        foreach (var p in properties)
        {
            if (p.IsField) { needsConstructor = true; break; }
        }

        List<ConstructorParamModel>? ctorParams = null;
        if (needsConstructor)
        {
            ctorParams = ResolveConstructor(typeSymbol, properties);
        }

        // Validate and collect diagnostics
        var diagnostics = new List<DiagnosticInfo>(ignoredMemberWarnings);
        var typeName = typeSymbol.Name;

        // ARROW001: non-partial type
        if (ctx.TargetNode is TypeDeclarationSyntax tds && !tds.Modifiers.Any(Microsoft.CodeAnalysis.CSharp.SyntaxKind.PartialKeyword))
        {
            diagnostics.Add(new DiagnosticInfo { Id = "ARROW001", Message = typeName, IsError = true });
        }

        // ARROW002: readonly fields with no matching constructor
        if (needsConstructor && ctorParams == null)
        {
            diagnostics.Add(new DiagnosticInfo { Id = "ARROW002", Message = typeName, IsError = true });
        }

        // ARROW003: unsupported member types
        foreach (var p in properties)
        {
            if (p.Type.Kind == TypeKind2.Unknown)
                diagnostics.Add(new DiagnosticInfo { Id = "ARROW003", Message = $"{p.PropertyName}\t{typeName}\t{p.Type.FullTypeName}", IsError = true });
        }

        // ARROW004: duplicate Arrow field names
        var seenFields = new Dictionary<string, string>();
        foreach (var p in properties)
        {
            if (seenFields.TryGetValue(p.FieldName, out var existingMember))
                diagnostics.Add(new DiagnosticInfo { Id = "ARROW004", Message = $"{existingMember}\t{p.PropertyName}\t{typeName}\t{p.FieldName}", IsError = true });
            else
                seenFields[p.FieldName] = p.PropertyName;
        }

        // ARROW005: non-settable properties (no set/init, not in constructor)
        if (ctorParams == null)
        {
            foreach (var member in typeSymbol.GetMembers())
            {
                if (!(member is IPropertySymbol ps) || ps.IsStatic || ps.IsIndexer)
                    continue;
                if (ps.DeclaredAccessibility != Accessibility.Public)
                    continue;
                if (ps.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "Apache.Arrow.Serialization.ArrowIgnoreAttribute"))
                    continue;
                if (ps.SetMethod == null)
                    diagnostics.Add(new DiagnosticInfo { Id = "ARROW005", Message = $"{ps.Name}\t{typeName}", IsError = true });
            }
        }

        return new TypeModel
        {
            Namespace = typeSymbol.ContainingNamespace.IsGlobalNamespace ? null : typeSymbol.ContainingNamespace.ToDisplayString(),
            TypeName = typeSymbol.Name,
            FullTypeName = typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            Properties = properties,
            IsRecord = typeSymbol.IsRecord,
            IsValueType = typeSymbol.IsValueType,
            Metadata = typeMetadata,
            HasArrowSerializableBase = HasArrowSerializableBaseType(typeSymbol),
            ConstructorParams = ctorParams,
            HasSerializationCallback = ImplementsInterface(typeSymbol, "Apache.Arrow.Serialization.IArrowSerializationCallback"),
            Diagnostics = diagnostics,
        };
    }

    private static bool HasArrowSerializableBaseType(INamedTypeSymbol typeSymbol)
    {
        var baseType = typeSymbol.BaseType;
        while (baseType != null && baseType.SpecialType != SpecialType.System_Object)
        {
            foreach (var attr in baseType.GetAttributes())
            {
                var attrName = attr.AttributeClass?.ToDisplayString();
                if (attrName == "Apache.Arrow.Serialization.ArrowSerializableAttribute" ||
                    attrName == "Apache.Arrow.Serialization.ArrowPolymorphicAttribute")
                    return true;
            }
            baseType = baseType.BaseType;
        }
        return false;
    }

    private static bool ImplementsInterface(INamedTypeSymbol typeSymbol, string interfaceFullName)
    {
        foreach (var iface in typeSymbol.AllInterfaces)
        {
            if (iface.ToDisplayString() == interfaceFullName)
                return true;
        }
        return false;
    }

    private static bool HasArrowAttributes(ISymbol member)
    {
        foreach (var attr in member.GetAttributes())
        {
            var name = attr.AttributeClass?.ToDisplayString();
            if (name != null && name.StartsWith("Apache.Arrow.Serialization.Arrow"))
                return true;
        }
        return false;
    }

    private static bool HasDefaultValue(IPropertySymbol prop, INamedTypeSymbol containingType)
    {
        // Check if the property has a default in the constructor or an initializer
        // For simplicity, we check if it's nullable or if there are constructors with optional params
        if (prop.NullableAnnotation == NullableAnnotation.Annotated)
            return true;

        foreach (var ctor in containingType.Constructors)
        {
            if (ctor.IsImplicitlyDeclared) continue;
            foreach (var param in ctor.Parameters)
            {
                if (param.Name.Equals(prop.Name, StringComparison.OrdinalIgnoreCase) && param.HasExplicitDefaultValue)
                    return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Find the best constructor for deserialization. Picks the constructor whose parameters
    /// best match the serialized members (by name, case-insensitive). Prefers the constructor
    /// with the most matching parameters.
    /// </summary>
    private static List<ConstructorParamModel>? ResolveConstructor(INamedTypeSymbol typeSymbol, List<PropertyModel> properties)
    {
        var memberNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in properties)
            memberNames.Add(p.PropertyName);

        IMethodSymbol? bestCtor = null;
        int bestMatch = -1;

        foreach (var ctor in typeSymbol.Constructors)
        {
            if (ctor.IsStatic || ctor.IsImplicitlyDeclared)
                continue;
            if (ctor.DeclaredAccessibility != Accessibility.Public)
                continue;

            // Count how many required params match members
            int matchCount = 0;
            bool allRequiredMatch = true;
            foreach (var param in ctor.Parameters)
            {
                if (memberNames.Contains(param.Name))
                    matchCount++;
                else if (!param.HasExplicitDefaultValue)
                {
                    allRequiredMatch = false;
                    break;
                }
            }

            if (allRequiredMatch && matchCount > bestMatch)
            {
                bestMatch = matchCount;
                bestCtor = ctor;
            }
        }

        if (bestCtor == null)
            return null;

        var result = new List<ConstructorParamModel>();
        foreach (var param in bestCtor.Parameters)
        {
            result.Add(new ConstructorParamModel
            {
                Name = param.Name,
                HasDefaultValue = param.HasExplicitDefaultValue,
            });
        }
        return result;
    }

    private static TypeInfo AnalyzeType(ITypeSymbol type, bool isNullable)
    {
        // Unwrap Nullable<T> for value types
        if (type is INamedTypeSymbol nt && nt.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
        {
            type = nt.TypeArguments[0];
            isNullable = true;
        }

        var fullName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        // Check for enum
        if (type.TypeKind == TypeKind.Enum)
        {
            return new TypeInfo
            {
                Kind = TypeKind2.Enum,
                FullTypeName = fullName,
                IsNullable = isNullable,
            };
        }

        // Check for nested ArrowSerializable
        foreach (var attr in type.GetAttributes())
        {
            if (attr.AttributeClass?.ToDisplayString() == "Apache.Arrow.Serialization.ArrowSerializableAttribute")
            {
                return new TypeInfo
                {
                    Kind = TypeKind2.NestedRecord,
                    FullTypeName = fullName,
                    IsNullable = isNullable,
                };
            }
        }

        // Check for collections
        if (type is INamedTypeSymbol namedType)
        {
            var origDef = namedType.OriginalDefinition.ToDisplayString();

            // List<T>, IList<T>, IReadOnlyList<T>
            if (origDef == "System.Collections.Generic.List<T>"
                || origDef == "System.Collections.Generic.IList<T>"
                || origDef == "System.Collections.Generic.IReadOnlyList<T>"
                || origDef == "System.Collections.Generic.IEnumerable<T>"
                || origDef == "System.Collections.Generic.ICollection<T>"
                || origDef == "System.Collections.Generic.IReadOnlyCollection<T>")
            {
                var elemType = AnalyzeType(namedType.TypeArguments[0], false);
                return new TypeInfo
                {
                    Kind = TypeKind2.List,
                    FullTypeName = fullName,
                    IsNullable = isNullable,
                    ElementType = elemType,
                };
            }

            // Dictionary<K,V>, IReadOnlyDictionary<K,V>
            if (origDef == "System.Collections.Generic.Dictionary<TKey, TValue>"
                || origDef == "System.Collections.Generic.IDictionary<TKey, TValue>"
                || origDef == "System.Collections.Generic.IReadOnlyDictionary<TKey, TValue>")
            {
                var keyType = AnalyzeType(namedType.TypeArguments[0], false);
                var valueType = AnalyzeType(namedType.TypeArguments[1], false);
                return new TypeInfo
                {
                    Kind = TypeKind2.Dictionary,
                    FullTypeName = fullName,
                    IsNullable = isNullable,
                    KeyType = keyType,
                    ValueType = valueType,
                };
            }

            // HashSet<T>, ISet<T>, IReadOnlySet<T>
            if (origDef == "System.Collections.Generic.HashSet<T>"
                || origDef == "System.Collections.Generic.ISet<T>"
                || origDef == "System.Collections.Generic.IReadOnlySet<T>"
                || origDef == "System.Collections.Frozen.FrozenSet<T>")
            {
                var elemType = AnalyzeType(namedType.TypeArguments[0], false);
                return new TypeInfo
                {
                    Kind = TypeKind2.Set,
                    FullTypeName = fullName,
                    IsNullable = isNullable,
                    ElementType = elemType,
                };
            }
        }

        // Array T[] — but byte[] is Binary, not Array
        if (type is IArrayTypeSymbol arrayType)
        {
            if (arrayType.ElementType.SpecialType == SpecialType.System_Byte)
            {
                return new TypeInfo
                {
                    Kind = TypeKind2.Binary,
                    FullTypeName = fullName,
                    IsNullable = isNullable,
                };
            }

            var elemType = AnalyzeType(arrayType.ElementType, false);
            return new TypeInfo
            {
                Kind = TypeKind2.Array,
                FullTypeName = fullName,
                IsNullable = isNullable,
                ElementType = elemType,
            };
        }

        // Primitive types
        var kind = fullName switch
        {
            "string" => TypeKind2.String,
            "bool" => TypeKind2.Bool,
            "byte" => TypeKind2.Byte,
            "sbyte" => TypeKind2.SByte,
            "short" => TypeKind2.Int16,
            "ushort" => TypeKind2.UInt16,
            "int" => TypeKind2.Int32,
            "uint" => TypeKind2.UInt32,
            "long" => TypeKind2.Int64,
            "ulong" => TypeKind2.UInt64,
            "float" => TypeKind2.Float,
            "double" => TypeKind2.Double,
            "decimal" => TypeKind2.Decimal,
            "byte[]" => TypeKind2.Binary,
            "System.ReadOnlyMemory<byte>" => TypeKind2.Binary,
            "global::System.ReadOnlyMemory<byte>" => TypeKind2.Binary,
            "System.DateTime" => TypeKind2.DateTime,
            "global::System.DateTime" => TypeKind2.DateTime,
            "System.DateTimeOffset" => TypeKind2.DateTimeOffset,
            "global::System.DateTimeOffset" => TypeKind2.DateTimeOffset,
            "System.DateOnly" => TypeKind2.DateOnly,
            "global::System.DateOnly" => TypeKind2.DateOnly,
            "System.TimeOnly" => TypeKind2.TimeOnly,
            "global::System.TimeOnly" => TypeKind2.TimeOnly,
            "System.TimeSpan" => TypeKind2.TimeSpan,
            "global::System.TimeSpan" => TypeKind2.TimeSpan,
            "System.Guid" => TypeKind2.Guid,
            "global::System.Guid" => TypeKind2.Guid,
            "System.Half" => TypeKind2.Half,
            "global::System.Half" => TypeKind2.Half,
            _ => TypeKind2.Unknown,
        };

        return new TypeInfo
        {
            Kind = kind,
            FullTypeName = fullName,
            IsNullable = isNullable,
        };
    }

    private static void Execute(SourceProductionContext spc, TypeModel model, bool emitSchemaJson)
    {
        // Report diagnostics
        foreach (var diag in model.Diagnostics)
        {
            var parts = diag.Message.Split('\t');
            var descriptor = diag.Id switch
            {
                "ARROW001" => NonPartialType,
                "ARROW002" => NoMatchingConstructor,
                "ARROW003" => UnsupportedMemberType,
                "ARROW004" => DuplicateFieldName,
                "ARROW005" => NonSettableProperty,
                "ARROW006" => ArrowAttributeOnIgnoredMember,
                _ => null,
            };
            if (descriptor != null)
                spc.ReportDiagnostic(Diagnostic.Create(descriptor, Location.None, parts));
        }

        // Don't emit code if there are errors
        bool hasErrors = false;
        foreach (var d in model.Diagnostics)
        {
            if (d.IsError) { hasErrors = true; break; }
        }
        if (hasErrors) return;

        var sb = new StringBuilder();
        var emitter = new CodeEmitter(sb, model);
        emitter.Emit();
        spc.AddSource($"{model.TypeName}.ArrowSerializer.g.cs", sb.ToString());

        if (emitSchemaJson)
        {
            var jsonSb = new StringBuilder();
            JsonSchemaEmitter.Emit(jsonSb, model);
            spc.AddSource($"{model.TypeName}.ArrowSchemaJson.g.cs", jsonSb.ToString());
        }
    }

    private static PolymorphicModel? GetPolymorphicModel(GeneratorAttributeSyntaxContext ctx)
    {
        if (!(ctx.TargetSymbol is INamedTypeSymbol typeSymbol))
            return null;

        // Read [ArrowPolymorphic] attribute
        string discriminatorFieldName = "$type";
        foreach (var attr in typeSymbol.GetAttributes())
        {
            if (attr.AttributeClass?.ToDisplayString() == "Apache.Arrow.Serialization.ArrowPolymorphicAttribute")
            {
                foreach (var named in attr.NamedArguments)
                {
                    if (named.Key == "TypeDiscriminatorFieldName" && named.Value.Value is string s)
                        discriminatorFieldName = s;
                }
            }
        }

        // Collect [ArrowDerivedType] attributes
        var derivedTypes = new List<DerivedTypeInfo>();
        foreach (var attr in typeSymbol.GetAttributes())
        {
            if (attr.AttributeClass?.ToDisplayString() != "Apache.Arrow.Serialization.ArrowDerivedTypeAttribute")
                continue;
            if (attr.ConstructorArguments.Length < 2)
                continue;

            var derivedTypeSymbol = attr.ConstructorArguments[0].Value as INamedTypeSymbol;
            var discriminator = attr.ConstructorArguments[1].Value as string;
            if (derivedTypeSymbol == null || discriminator == null)
                continue;

            var props = CollectProperties(derivedTypeSymbol);
            derivedTypes.Add(new DerivedTypeInfo
            {
                TypeDiscriminator = discriminator,
                FullTypeName = derivedTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                TypeName = derivedTypeSymbol.Name,
                Properties = props,
                IsRecord = derivedTypeSymbol.IsRecord,
                IsValueType = derivedTypeSymbol.IsValueType,
            });
        }

        // Build union of all properties (deduplicated by FieldName, all nullable)
        var unionProps = new List<PropertyModel>();
        var seenFields = new HashSet<string>();
        foreach (var dt in derivedTypes)
        {
            foreach (var prop in dt.Properties)
            {
                if (seenFields.Add(prop.FieldName))
                {
                    // Make a nullable copy for the union schema
                    unionProps.Add(new PropertyModel
                    {
                        PropertyName = prop.PropertyName,
                        FieldName = prop.FieldName,
                        Order = prop.Order,
                        DeclOrder = prop.DeclOrder,
                        Type = prop.Type,
                        IsNullable = true, // always nullable in union schema
                        HasDefaultValue = true,
                        Metadata = prop.Metadata,
                    });
                }
            }
        }

        // Collect metadata
        var typeMetadata = new List<KeyValuePair<string, string>>();
        foreach (var attr in typeSymbol.GetAttributes())
        {
            if (attr.AttributeClass?.ToDisplayString() == "Apache.Arrow.Serialization.ArrowMetadataAttribute"
                && attr.ConstructorArguments.Length >= 2)
            {
                var key = attr.ConstructorArguments[0].Value as string;
                var val = attr.ConstructorArguments[1].Value as string;
                if (key != null && val != null)
                    typeMetadata.Add(new KeyValuePair<string, string>(key, val));
            }
        }

        return new PolymorphicModel
        {
            Namespace = typeSymbol.ContainingNamespace.IsGlobalNamespace ? null : typeSymbol.ContainingNamespace.ToDisplayString(),
            TypeName = typeSymbol.Name,
            FullTypeName = typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
            TypeDiscriminatorFieldName = discriminatorFieldName,
            IsInterface = typeSymbol.TypeKind == TypeKind.Interface,
            IsRecord = typeSymbol.IsRecord,
            DerivedTypes = derivedTypes,
            UnionProperties = unionProps,
            Metadata = typeMetadata,
        };
    }

    /// <summary>
    /// Collects properties from a type symbol (same logic as GetTypeModel but returns just the properties).
    /// </summary>
    private static List<PropertyModel> CollectProperties(INamedTypeSymbol typeSymbol)
    {
        var properties = new List<PropertyModel>();
        int declOrder = 0;
        foreach (var member in typeSymbol.GetMembers())
        {
            if (!(member is IPropertySymbol prop))
                continue;
            if (prop.IsStatic || prop.IsIndexer)
                continue;
            if (prop.DeclaredAccessibility != Accessibility.Public)
                continue;

            bool isTransient = false;
            string? arrowTypeName = null;
            string? converterTypeName2 = null;
            string? fieldName = null;
            int order = int.MaxValue;
            var propMetadata = new List<KeyValuePair<string, string>>();

            foreach (var attr in prop.GetAttributes())
            {
                var attrName = attr.AttributeClass?.ToDisplayString();
                if (attrName == "Apache.Arrow.Serialization.ArrowIgnoreAttribute")
                    isTransient = true;
                else if (attrName == "Apache.Arrow.Serialization.ArrowTypeAttribute")
                {
                    if (attr.ConstructorArguments.Length > 0)
                        arrowTypeName = attr.ConstructorArguments[0].Value as string;
                    foreach (var named in attr.NamedArguments)
                    {
                        if (named.Key == "Converter" && named.Value.Value is INamedTypeSymbol converterSym)
                            converterTypeName2 = converterSym.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    }
                }
                else if (attrName == "Apache.Arrow.Serialization.ArrowFieldAttribute")
                {
                    if (attr.ConstructorArguments.Length > 0)
                        fieldName = attr.ConstructorArguments[0].Value as string;
                    foreach (var named in attr.NamedArguments)
                    {
                        if (named.Key == "Order" && named.Value.Value is int o)
                            order = o;
                    }
                }
                else if (attrName == "Apache.Arrow.Serialization.ArrowMetadataAttribute")
                {
                    if (attr.ConstructorArguments.Length >= 2)
                    {
                        var key = attr.ConstructorArguments[0].Value as string;
                        var val = attr.ConstructorArguments[1].Value as string;
                        if (key != null && val != null)
                            propMetadata.Add(new KeyValuePair<string, string>(key, val));
                    }
                }
            }

            if (isTransient)
                continue;

            var propType = prop.Type;
            bool isNullable = propType.NullableAnnotation == NullableAnnotation.Annotated;
            if (isNullable && propType is INamedTypeSymbol nullableType && nullableType.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T)
                propType = nullableType.TypeArguments[0];

            var typeInfo = AnalyzeType(propType, isNullable);
            if (converterTypeName2 != null)
                typeInfo = new TypeInfo { Kind = TypeKind2.Custom, FullTypeName = typeInfo.FullTypeName, IsNullable = isNullable };
            else if (arrowTypeName != null)
                typeInfo = typeInfo.WithOverride(arrowTypeName);

            properties.Add(new PropertyModel
            {
                PropertyName = prop.Name,
                FieldName = fieldName ?? prop.Name,
                Order = order,
                DeclOrder = declOrder++,
                Type = typeInfo,
                IsNullable = isNullable,
                HasDefaultValue = HasDefaultValue(prop, typeSymbol),
                Metadata = propMetadata,
                ConverterTypeName = converterTypeName2,
            });
        }

        properties.Sort((a, b) =>
        {
            int cmp = a.Order.CompareTo(b.Order);
            return cmp != 0 ? cmp : a.DeclOrder.CompareTo(b.DeclOrder);
        });

        return properties;
    }

    private static void ExecutePolymorphic(SourceProductionContext spc, PolymorphicModel model)
    {
        var sb = new StringBuilder();
        var emitter = new PolymorphicCodeEmitter(sb, model);
        emitter.Emit();
        spc.AddSource($"{model.TypeName}.ArrowPolymorphic.g.cs", sb.ToString());
    }
}
} // namespace
