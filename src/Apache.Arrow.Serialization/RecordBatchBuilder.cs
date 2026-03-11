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

using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Apache.Arrow.Serialization;

/// <summary>
/// Reflection-based serializer for converting arbitrary .NET objects (including anonymous types)
/// to Arrow RecordBatches. Analogous to System.Text.Json's reflection-based path —
/// works without attributes or source generation but is not AOT-safe.
/// </summary>
public static class RecordBatchBuilder
{
    /// <summary>
    /// Convert a collection of objects to a RecordBatch. Schema is inferred from the
    /// public readable properties of <typeparamref name="T"/>.
    /// Works with anonymous types, records, classes, and structs.
    /// </summary>
    [RequiresUnreferencedCode("Uses reflection to inspect properties. Use [ArrowSerializable] for AOT-safe serialization.")]
    public static RecordBatch FromObjects<T>(IEnumerable<T> items)
    {
        var list = items as IReadOnlyList<T> ?? items.ToList();
        if (list.Count == 0)
            throw new ArgumentException("Cannot infer schema from empty collection.", nameof(items));

        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .ToArray();

        var fields = new List<Field>();
        var builders = new List<IColumnBuilder>();

        foreach (var prop in properties)
        {
            var propType = prop.PropertyType;
            var (arrowType, nullable) = InferArrowType(propType);
            fields.Add(new Field(prop.Name, arrowType, nullable));
            builders.Add(CreateColumnBuilder(propType, arrowType));
        }

        var schema = new Schema.Builder();
        foreach (var f in fields) schema.Field(f);

        // Populate builders
        for (int row = 0; row < list.Count; row++)
        {
            var item = list[row]!;
            for (int col = 0; col < properties.Length; col++)
            {
                var value = properties[col].GetValue(item);
                builders[col].Append(value);
            }
        }

        var arrays = builders.Select(b => b.Build()).ToArray();
        return new RecordBatch(schema.Build(), arrays, list.Count);
    }

    /// <summary>
    /// Convert a single object to a single-row RecordBatch.
    /// </summary>
    [RequiresUnreferencedCode("Uses reflection to inspect properties. Use [ArrowSerializable] for AOT-safe serialization.")]
    public static RecordBatch FromObject<T>(T item)
        => FromObjects(new[] { item });

    private static (IArrowType Type, bool Nullable) InferArrowType(Type clrType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying is not null)
        {
            var (inner, _) = InferArrowType(underlying);
            return (inner, true);
        }

        if (clrType == typeof(string)) return (StringType.Default, true);
        if (clrType == typeof(bool)) return (BooleanType.Default, false);
        if (clrType == typeof(sbyte)) return (Int8Type.Default, false);
        if (clrType == typeof(byte)) return (UInt8Type.Default, false);
        if (clrType == typeof(short)) return (Int16Type.Default, false);
        if (clrType == typeof(ushort)) return (UInt16Type.Default, false);
        if (clrType == typeof(int)) return (Int32Type.Default, false);
        if (clrType == typeof(uint)) return (UInt32Type.Default, false);
        if (clrType == typeof(long)) return (Int64Type.Default, false);
        if (clrType == typeof(ulong)) return (UInt64Type.Default, false);
        if (clrType == typeof(Half)) return (HalfFloatType.Default, false);
        if (clrType == typeof(float)) return (FloatType.Default, false);
        if (clrType == typeof(double)) return (DoubleType.Default, false);
        if (clrType == typeof(decimal)) return (new Decimal128Type(38, 18), false);
        if (clrType == typeof(DateTime)) return (new TimestampType(TimeUnit.Microsecond, "UTC"), false);
        if (clrType == typeof(DateTimeOffset)) return (new TimestampType(TimeUnit.Microsecond, "UTC"), false);
        if (clrType == typeof(DateOnly)) return (Date32Type.Default, false);
        if (clrType == typeof(TimeOnly)) return (new Time64Type(TimeUnit.Microsecond), false);
        if (clrType == typeof(TimeSpan)) return (DurationType.Microsecond, false);
        if (clrType == typeof(Guid)) return (new GuidType(), false);
        if (clrType == typeof(byte[])) return (BinaryType.Default, true);
        if (clrType == typeof(ReadOnlyMemory<byte>)) return (BinaryType.Default, false);

        if (clrType.IsEnum)
            return (new DictionaryType(Int16Type.Default, StringType.Default, false), false);

        // T[] arrays (not byte[] which is handled above)
        if (clrType.IsArray)
        {
            var elemType = clrType.GetElementType()!;
            var (elemArrow, elemNullable) = InferArrowType(elemType);
            return (new ListType(new Field("item", elemArrow, elemNullable)), true);
        }

        if (clrType.IsGenericType)
        {
            var genDef = clrType.GetGenericTypeDefinition();
            if (genDef == typeof(List<>) || genDef == typeof(HashSet<>))
            {
                var elemType = clrType.GetGenericArguments()[0];
                var (elemArrow, elemNullable) = InferArrowType(elemType);
                return (new ListType(new Field("item", elemArrow, elemNullable)), true);
            }
            if (genDef == typeof(Dictionary<,>))
            {
                var args = clrType.GetGenericArguments();
                var (keyArrow, _) = InferArrowType(args[0]);
                var (valArrow, valNullable) = InferArrowType(args[1]);
                return (new MapType(new Field("key", keyArrow, false), new Field("value", valArrow, valNullable)), true);
            }
        }

        // Check for [ArrowSerializable] types with source-generated IArrowSerializer<T>
        var genSchema = GetGeneratedArrowSchema(clrType);
        if (genSchema is not null)
        {
            var structFields = new List<Field>(genSchema.FieldsList);
            return (new StructType(structFields), true);
        }

        // Nested object type (anonymous, record, class, struct with readable properties)
        if (clrType.IsClass || clrType.IsValueType)
        {
            var nestedProps = clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead)
                .ToArray();
            if (nestedProps.Length > 0)
            {
                var nestedFields = nestedProps.Select(p =>
                {
                    var (ft, fn) = InferArrowType(p.PropertyType);
                    return new Field(p.Name, ft, fn);
                }).ToList();
                return (new StructType(nestedFields), true);
            }
        }

        throw new NotSupportedException($"Cannot infer Arrow type for {clrType.FullName}");
    }

    /// <summary>
    /// Check if a type implements IArrowSerializer&lt;T&gt; (i.e. has [ArrowSerializable] source-generated code)
    /// and return its static ArrowSchema if so.
    /// </summary>
    private static Schema? GetGeneratedArrowSchema(Type clrType)
    {
        var iface = clrType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IArrowSerializer<>));
        if (iface is null) return null;

        var schemaProp = clrType.GetProperty("ArrowSchema", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
        return schemaProp?.GetValue(null) as Schema;
    }

    /// <summary>
    /// Try to get the static ToRecordBatch(IReadOnlyList&lt;T&gt;) method from a source-generated type.
    /// </summary>
    private static MethodInfo? GetGeneratedToRecordBatchList(Type clrType)
    {
        var listType = typeof(IReadOnlyList<>).MakeGenericType(clrType);
        return clrType.GetMethod("ToRecordBatch", BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy, [listType]);
    }

    private static IColumnBuilder CreateColumnBuilder(Type clrType, IArrowType arrowType)
    {
        var underlying = Nullable.GetUnderlyingType(clrType);
        if (underlying is not null)
            return CreateColumnBuilder(underlying, arrowType); // inner builders all handle null

        if (clrType == typeof(string)) return new StringColumnBuilder();
        if (clrType == typeof(bool)) return new BoolColumnBuilder();
        if (clrType == typeof(sbyte)) return new TypedColumnBuilder<sbyte, Int8Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(byte)) return new TypedColumnBuilder<byte, UInt8Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(short)) return new TypedColumnBuilder<short, Int16Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(ushort)) return new TypedColumnBuilder<ushort, UInt16Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(int)) return new TypedColumnBuilder<int, Int32Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(uint)) return new TypedColumnBuilder<uint, UInt32Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(long)) return new TypedColumnBuilder<long, Int64Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(ulong)) return new TypedColumnBuilder<ulong, UInt64Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(Half)) return new TypedColumnBuilder<Half, HalfFloatArray.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(float)) return new TypedColumnBuilder<float, FloatArray.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(double)) return new TypedColumnBuilder<double, DoubleArray.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(decimal)) return new DecimalColumnBuilder();
        if (clrType == typeof(DateTime)) return new DateTimeColumnBuilder();
        if (clrType == typeof(DateTimeOffset)) return new DateTimeOffsetColumnBuilder();
        if (clrType == typeof(DateOnly)) return new TypedColumnBuilder<DateOnly, Date32Array.Builder>(new(), (b, v) => b.Append(v), b => b.AppendNull(), b => b.Build());
        if (clrType == typeof(TimeOnly)) return new TimeOnlyColumnBuilder();
        if (clrType == typeof(TimeSpan)) return new TimeSpanColumnBuilder();
        if (clrType == typeof(Guid)) return new GuidColumnBuilder();
        if (clrType == typeof(byte[])) return new BinaryColumnBuilder();
        if (clrType == typeof(ReadOnlyMemory<byte>)) return new ReadOnlyMemoryByteColumnBuilder();
        if (clrType.IsEnum) return new EnumColumnBuilder();

        // List<T>, T[], HashSet<T> → ListArray
        if (arrowType is ListType listType)
        {
            var elemClrType = clrType.IsArray
                ? clrType.GetElementType()!
                : clrType.GetGenericArguments()[0];
            var elemBuilder = CreateColumnBuilder(elemClrType, listType.ValueDataType);
            return new ListColumnBuilder(listType, elemClrType, elemBuilder);
        }

        // Dictionary<K,V> → MapArray
        if (arrowType is MapType mapType)
        {
            var args = clrType.GetGenericArguments();
            var keyBuilder = CreateColumnBuilder(args[0], mapType.KeyField.DataType);
            var valBuilder = CreateColumnBuilder(args[1], mapType.ValueField.DataType);
            return new MapColumnBuilder(mapType, args[0], args[1], keyBuilder, valBuilder);
        }

        // Nested object → StructArray
        if (arrowType is StructType structType)
        {
            // If the type has source-generated IArrowSerializer<T>, delegate to it
            var toRecordBatchList = GetGeneratedToRecordBatchList(clrType);
            if (toRecordBatchList is not null)
                return new SourceGenStructColumnBuilder(clrType, structType, toRecordBatchList);

            // Otherwise, fall back to reflection-based struct builder
            var nestedProps = clrType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead)
                .ToArray();
            var childBuilders = new List<IColumnBuilder>();
            for (int i = 0; i < nestedProps.Length; i++)
            {
                var childArrowType = structType.Fields[i].DataType;
                childBuilders.Add(CreateColumnBuilder(nestedProps[i].PropertyType, childArrowType));
            }
            return new StructColumnBuilder(structType, nestedProps, childBuilders);
        }

        throw new NotSupportedException($"Column builder not available for {clrType.FullName}");
    }

    // --- Column builder interface and implementations ---

    private interface IColumnBuilder
    {
        void Append(object? value);
        IArrowArray Build();
    }

    private sealed class StringColumnBuilder : IColumnBuilder
    {
        private readonly StringArray.Builder _b = new();
        public void Append(object? value) { if (value is null) _b.AppendNull(); else _b.Append((string)value); }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class BoolColumnBuilder : IColumnBuilder
    {
        private readonly BooleanArray.Builder _b = new();
        public void Append(object? value) { if (value is null) _b.AppendNull(); else _b.Append((bool)value); }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class TypedColumnBuilder<T, TBuilder> : IColumnBuilder
        where T : struct
        where TBuilder : class
    {
        private readonly TBuilder _builder;
        private readonly Action<TBuilder, T> _append;
        private readonly Action<TBuilder> _appendNull;
        private readonly Func<TBuilder, IArrowArray> _build;

        public TypedColumnBuilder(TBuilder builder, Action<TBuilder, T> append,
            Action<TBuilder> appendNull, Func<TBuilder, IArrowArray> build)
        {
            _builder = builder;
            _append = append;
            _appendNull = appendNull;
            _build = build;
        }

        public void Append(object? value)
        {
            if (value is null) _appendNull(_builder);
            else _append(_builder, (T)value);
        }

        public IArrowArray Build() => _build(_builder);
    }

    private sealed class DecimalColumnBuilder : IColumnBuilder
    {
        private readonly List<(decimal Value, bool IsNull)> _values = new();
        public void Append(object? value)
        {
            if (value is null) _values.Add((0, true));
            else _values.Add(((decimal)value, false));
        }
        public IArrowArray Build()
        {
            var b = new Decimal128Array.Builder(new Decimal128Type(38, 18));
            foreach (var (v, isNull) in _values)
                if (isNull) b.AppendNull(); else b.Append(v);
            return b.Build();
        }
    }

    private sealed class DateTimeColumnBuilder : IColumnBuilder
    {
        private readonly TimestampArray.Builder _b = new(new TimestampType(TimeUnit.Microsecond, "UTC"));
        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append(new DateTimeOffset((DateTime)value, TimeSpan.Zero));
        }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class DateTimeOffsetColumnBuilder : IColumnBuilder
    {
        private readonly TimestampArray.Builder _b = new(new TimestampType(TimeUnit.Microsecond, "UTC"));
        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((DateTimeOffset)value);
        }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class TimeOnlyColumnBuilder : IColumnBuilder
    {
        private readonly List<(TimeOnly Value, bool IsNull)> _values = new();
        public void Append(object? value)
        {
            if (value is null) _values.Add((default, true));
            else _values.Add(((TimeOnly)value, false));
        }
        public IArrowArray Build()
        {
            var b = new Time64Array.Builder(new Time64Type(TimeUnit.Microsecond));
            foreach (var (v, isNull) in _values)
                if (isNull) b.AppendNull(); else b.Append(v);
            return b.Build();
        }
    }

    private sealed class TimeSpanColumnBuilder : IColumnBuilder
    {
        private readonly List<(TimeSpan Value, bool IsNull)> _values = new();
        public void Append(object? value)
        {
            if (value is null) _values.Add((default, true));
            else _values.Add(((TimeSpan)value, false));
        }
        public IArrowArray Build()
        {
            var b = new DurationArray.Builder(DurationType.Microsecond);
            foreach (var (v, isNull) in _values)
                if (isNull) b.AppendNull(); else b.Append(v);
            return b.Build();
        }
    }

    private sealed class GuidColumnBuilder : IColumnBuilder
    {
        private readonly GuidArray.Builder _b = new();
        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((Guid)value);
        }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class BinaryColumnBuilder : IColumnBuilder
    {
        private readonly BinaryArray.Builder _b = new();
        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append((ReadOnlySpan<byte>)(byte[])value);
        }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class EnumColumnBuilder : IColumnBuilder
    {
        private readonly Dictionary<string, short> _dict = new();
        private readonly List<string?> _values = new();

        public void Append(object? value)
        {
            if (value is null) { _values.Add(null); return; }
            var name = value.ToString()!;
            if (!_dict.ContainsKey(name))
                _dict[name] = (short)_dict.Count;
            _values.Add(name);
        }

        public IArrowArray Build()
        {
            var dictNames = _dict.OrderBy(kv => kv.Value).Select(kv => kv.Key).ToArray();
            var dictBuilder = new StringArray.Builder();
            foreach (var n in dictNames) dictBuilder.Append(n);
            var dictArray = dictBuilder.Build();

            var idxBuilder = new Int16Array.Builder();
            foreach (var v in _values)
            {
                if (v is null) idxBuilder.AppendNull();
                else idxBuilder.Append(_dict[v]);
            }

            return new DictionaryArray(
                new DictionaryType(Int16Type.Default, StringType.Default, false),
                idxBuilder.Build(), dictArray);
        }
    }

    private sealed class ReadOnlyMemoryByteColumnBuilder : IColumnBuilder
    {
        private readonly BinaryArray.Builder _b = new();
        public void Append(object? value)
        {
            if (value is null) _b.AppendNull();
            else _b.Append(((ReadOnlyMemory<byte>)value).Span);
        }
        public IArrowArray Build() => _b.Build();
    }

    private sealed class ListColumnBuilder : IColumnBuilder
    {
        private readonly ListType _listType;
        private readonly IColumnBuilder _elemBuilder;
        private readonly List<int> _offsets = new() { 0 };
        private readonly List<bool> _validity = new();
        private int _totalElements;

        public ListColumnBuilder(ListType listType, Type elemClrType, IColumnBuilder elemBuilder)
        {
            _listType = listType;
            _elemBuilder = elemBuilder;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                _offsets.Add(_totalElements);
                return;
            }

            _validity.Add(true);
            var enumerable = (System.Collections.IEnumerable)value;
            foreach (var item in enumerable)
            {
                _elemBuilder.Append(item);
                _totalElements++;
            }
            _offsets.Add(_totalElements);
        }

        public IArrowArray Build()
        {
            var valueArray = _elemBuilder.Build();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            var offsetBuffer = new ArrowBuffer(
                _offsets.SelectMany(BitConverter.GetBytes).ToArray());

            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                var bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            var data = new ArrayData(_listType, length, nullCount,
                0, [nullBitmap, offsetBuffer], [valueArray.Data]);
            return new ListArray(data);
        }
    }

    private sealed class MapColumnBuilder : IColumnBuilder
    {
        private readonly MapType _mapType;
        private readonly IColumnBuilder _keyBuilder;
        private readonly IColumnBuilder _valBuilder;
        private readonly List<int> _offsets = new() { 0 };
        private readonly List<bool> _validity = new();
        private int _totalEntries;

        public MapColumnBuilder(MapType mapType, Type keyClrType, Type valClrType,
            IColumnBuilder keyBuilder, IColumnBuilder valBuilder)
        {
            _mapType = mapType;
            _keyBuilder = keyBuilder;
            _valBuilder = valBuilder;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                _offsets.Add(_totalEntries);
                return;
            }

            _validity.Add(true);
            var dict = (System.Collections.IDictionary)value;
            foreach (System.Collections.DictionaryEntry entry in dict)
            {
                _keyBuilder.Append(entry.Key);
                _valBuilder.Append(entry.Value);
                _totalEntries++;
            }
            _offsets.Add(_totalEntries);
        }

        public IArrowArray Build()
        {
            var keyArray = _keyBuilder.Build();
            var valArray = _valBuilder.Build();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            var offsetBuffer = new ArrowBuffer(
                _offsets.SelectMany(BitConverter.GetBytes).ToArray());

            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                var bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            // MapArray's child is a StructArray of (key, value) entries
            var entryType = new StructType(new List<Field> { _mapType.KeyField, _mapType.ValueField });
            var entryArray = new StructArray(entryType, _totalEntries,
                new IArrowArray[] { keyArray, valArray }, ArrowBuffer.Empty, 0);

            var data = new ArrayData(_mapType, length, nullCount,
                0, [nullBitmap, offsetBuffer], [entryArray.Data]);
            return new MapArray(data);
        }
    }

    /// <summary>
    /// Column builder that delegates to source-generated ToRecordBatch(IReadOnlyList&lt;T&gt;)
    /// for [ArrowSerializable] types, then wraps the RecordBatch columns into a StructArray.
    /// </summary>
    private sealed class SourceGenStructColumnBuilder : IColumnBuilder
    {
        private readonly Type _clrType;
        private readonly StructType _structType;
        private readonly MethodInfo _toRecordBatchList;
        private readonly List<object?> _items = new();

        public SourceGenStructColumnBuilder(Type clrType, StructType structType, MethodInfo toRecordBatchList)
        {
            _clrType = clrType;
            _structType = structType;
            _toRecordBatchList = toRecordBatchList;
        }

        public void Append(object? value) => _items.Add(value);

        public IArrowArray Build()
        {
            int length = _items.Count;
            int nullCount = _items.Count(v => v is null);

            // Build a typed list for the source-generated method
            var listType = typeof(List<>).MakeGenericType(_clrType);
            var typedList = (System.Collections.IList)Activator.CreateInstance(listType, length)!;

            // For null slots, we need a stand-in value (first non-null item)
            object? standIn = _items.FirstOrDefault(v => v is not null);
            foreach (var item in _items)
                typedList.Add(item ?? standIn!);

            // Call the generated ToRecordBatch(IReadOnlyList<T>)
            var batch = (RecordBatch)_toRecordBatchList.Invoke(null, [typedList])!;

            // Extract columns as child arrays for the StructArray
            var childArrays = new IArrowArray[batch.ColumnCount];
            for (int i = 0; i < batch.ColumnCount; i++)
                childArrays[i] = batch.Column(i);

            // Build null bitmap
            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                var bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                {
                    if (_items[i] is not null)
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                }
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            return new StructArray(_structType, length, childArrays, nullBitmap, nullCount);
        }
    }

    private sealed class StructColumnBuilder : IColumnBuilder
    {
        private readonly StructType _structType;
        private readonly PropertyInfo[] _properties;
        private readonly List<IColumnBuilder> _childBuilders;
        private readonly List<bool> _validity = new();

        public StructColumnBuilder(StructType structType, PropertyInfo[] properties, List<IColumnBuilder> childBuilders)
        {
            _structType = structType;
            _properties = properties;
            _childBuilders = childBuilders;
        }

        public void Append(object? value)
        {
            if (value is null)
            {
                _validity.Add(false);
                // Append nulls/defaults to all children to keep lengths aligned
                for (int i = 0; i < _childBuilders.Count; i++)
                    _childBuilders[i].Append(null);
            }
            else
            {
                _validity.Add(true);
                for (int i = 0; i < _properties.Length; i++)
                    _childBuilders[i].Append(_properties[i].GetValue(value));
            }
        }

        public IArrowArray Build()
        {
            var childArrays = _childBuilders.Select(b => b.Build()).ToArray();
            int length = _validity.Count;
            int nullCount = _validity.Count(v => !v);

            // Build null bitmap
            ArrowBuffer nullBitmap;
            if (nullCount == 0)
            {
                nullBitmap = ArrowBuffer.Empty;
            }
            else
            {
                var bitmapBytes = new byte[(length + 7) / 8];
                for (int i = 0; i < length; i++)
                {
                    if (_validity[i])
                        bitmapBytes[i / 8] |= (byte)(1 << (i % 8));
                }
                nullBitmap = new ArrowBuffer(bitmapBytes);
            }

            return new StructArray(_structType, length, childArrays, nullBitmap, nullCount);
        }
    }
}
