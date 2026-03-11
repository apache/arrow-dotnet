<!---
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache.Arrow.Serialization

Source-generated [Apache Arrow](https://arrow.apache.org/) serialization for .NET.

Mark any type with `[ArrowSerializable]` and a Roslyn source generator will emit compile-time Arrow schema derivation, serialization, and deserialization — zero reflection on the hot path, fully AOT-compatible.

```csharp
[ArrowSerializable]
public partial record Person
{
    public string Name { get; init; } = "";
    public int Age { get; init; }
}

// Single-row
var batch = Person.ToRecordBatch(new Person { Name = "Alice", Age = 30 });
var alice = Person.FromRecordBatch(batch);

// Multi-row
var people = new[] { alice, new Person { Name = "Bob", Age = 25 } };
var table = Person.ToRecordBatch(people);
IReadOnlyList<Person> restored = Person.ListFromRecordBatch(table);

// Arrow IPC bytes (cross-language compatible)
byte[] bytes = alice.SerializeToBytes();
var roundTrip = ArrowSerializerExtensions.DeserializeFromBytes<Person>(bytes);
```

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Supported Types](#supported-types)
  - [Type Declarations](#type-declarations)
  - [Built-in Type Mappings](#built-in-type-mappings)
  - [Collections](#collections)
  - [Nullable Types](#nullable-types)
- [Attributes](#attributes)
  - [ArrowSerializable](#arrowserializable)
  - [ArrowField](#arrowfield)
  - [ArrowType](#arrowtype)
  - [ArrowIgnore](#arrowignore)
  - [ArrowMetadata](#arrowmetadata)
- [Nested Types](#nested-types)
- [Readonly Fields and Constructors](#readonly-fields-and-constructors)
- [Enum Serialization](#enum-serialization)
- [Polymorphism](#polymorphism)
- [Custom Converters](#custom-converters)
- [Serialization Callbacks](#serialization-callbacks)
- [JSON Schema Emission](#json-schema-emission)
- [RecordBatchBuilder (Reflection-Based)](#recordbatchbuilder-reflection-based)
- [Extension Methods](#extension-methods)
- [Source Generator Diagnostics](#source-generator-diagnostics)
- [Cross-Language Compatibility](#cross-language-compatibility)

## Installation

```
dotnet add package Apache.Arrow.Serialization
```

The NuGet package includes both the runtime library and the Roslyn source generator. Targets `net8.0`.

## Quick Start

1. Add `[ArrowSerializable]` to your type
2. Make the type `partial` (required for source generation)
3. The generator emits `IArrowSerializer<T>` — giving you `ArrowSchema`, `ToRecordBatch`, `FromRecordBatch`, and `ListFromRecordBatch`

```csharp
using Apache.Arrow.Serialization;

[ArrowSerializable]
public partial record SensorReading
{
    public string SensorId { get; init; } = "";
    public double Temperature { get; init; }
    public DateTime Timestamp { get; init; }
}
```

The source generator produces a `partial` implementation with these static members:

```csharp
partial record SensorReading : IArrowSerializer<SensorReading>
{
    public static Schema ArrowSchema { get; }
    public static RecordBatch ToRecordBatch(SensorReading value);
    public static SensorReading FromRecordBatch(RecordBatch batch);
    public static RecordBatch ToRecordBatch(IReadOnlyList<SensorReading> values);
    public static IReadOnlyList<SensorReading> ListFromRecordBatch(RecordBatch batch);
}
```

## Supported Types

### Type Declarations

All four C# type kinds are supported:

```csharp
[ArrowSerializable]
public partial record MyRecord { ... }

[ArrowSerializable]
public partial record struct MyRecordStruct { ... }

[ArrowSerializable]
public partial class MyClass { ... }

[ArrowSerializable]
public partial struct MyStruct { ... }
```

Records use `{ get; init; }` properties. Classes and structs use `{ get; set; }`.

### Built-in Type Mappings

| C# Type | Arrow Type | Notes |
|---------|-----------|-------|
| `string` | `Utf8` | Override to `StringView` via `[ArrowType("string_view")]` |
| `bool` | `Boolean` | Override to `Bool8` via `[ArrowType("bool8")]` |
| `byte` | `UInt8` | |
| `sbyte` | `Int8` | |
| `short` | `Int16` | |
| `ushort` | `UInt16` | |
| `int` | `Int32` | |
| `uint` | `UInt32` | |
| `long` | `Int64` | |
| `ulong` | `UInt64` | |
| `float` | `Float32` | |
| `double` | `Float64` | |
| `Half` | `Float16` | |
| `decimal` | `Decimal128(38, 18)` | Configurable via `[ArrowType("decimal128(28, 10)")]` |
| `DateTime` | `Timestamp(us, UTC)` | Configurable resolution and timezone |
| `DateTimeOffset` | `Timestamp(us, UTC)` | Configurable resolution and timezone |
| `DateOnly` | `Date32` | Override to `Date64` via `[ArrowType("date64")]` |
| `TimeOnly` | `Time64(us)` | Override to `Time32` via `[ArrowType("time32[ms]")]` |
| `TimeSpan` | `Duration(us)` | |
| `Guid` | `FixedSizeBinary(16)` | UUID extension type |
| `byte[]` | `Binary` | Override to `BinaryView` via `[ArrowType("binary_view")]` |
| `ReadOnlyMemory<byte>` | `Binary` | Override to `BinaryView` via `[ArrowType("binary_view")]` |
| `enum` | `Dictionary(Int16, Utf8)` | Name-based encoding |

### Collections

| C# Type | Arrow Type |
|---------|-----------|
| `List<T>`, `T[]`, `IList<T>`, `IReadOnlyList<T>` | `List(T)` |
| `IEnumerable<T>`, `ICollection<T>`, `IReadOnlyCollection<T>` | `List(T)` |
| `HashSet<T>`, `ISet<T>`, `IReadOnlySet<T>` | `List(T)` |
| `Dictionary<K,V>`, `IDictionary<K,V>`, `IReadOnlyDictionary<K,V>` | `Map(K,V)` |

Collections support nested types, enums, and nullable elements:

```csharp
[ArrowSerializable]
public partial record DataSet
{
    public List<int> Values { get; init; } = new();
    public Dictionary<string, double> Metrics { get; init; } = new();
    public List<Inner?> OptionalItems { get; init; } = new();
    public HashSet<Color> UniqueColors { get; init; } = new();
}
```

### Nullable Types

Both nullable value types and nullable reference types are supported. Nullable properties produce nullable Arrow fields:

```csharp
[ArrowSerializable]
public partial record WithNullables
{
    public int? MaybeCount { get; init; }
    public string? MaybeName { get; init; }
    public Inner? MaybeNested { get; init; }
}
```

## Attributes

### ArrowSerializable

Marks a type for source generation. Required on all types you want to serialize.

```csharp
[ArrowSerializable]
public partial record MyType { ... }
```

### ArrowField

Overrides the Arrow field name and/or controls field ordering:

```csharp
[ArrowSerializable]
public partial record Measurement
{
    [ArrowField("sensor_id")]
    public string SensorId { get; init; } = "";

    [ArrowField("value", Order = 0)]
    public double Value { get; init; }

    [ArrowField(Order = 1)]
    public DateTime Timestamp { get; init; }
}
```

- `Name` — the Arrow field name (defaults to C# property name)
- `Order` — controls field position in the schema (lower values first; default is declaration order)

### ArrowType

Overrides the inferred Arrow type for a property:

```csharp
[ArrowSerializable]
public partial record Precise
{
    [ArrowType("decimal128(28, 10)")]
    public decimal Value { get; init; }

    [ArrowType("timestamp[ns, UTC]")]
    public DateTime Created { get; init; }

    [ArrowType("timestamp[us]")]
    public DateTime LocalTime { get; init; }  // wall-clock (no timezone)

    [ArrowType("date64")]
    public DateOnly Birthday { get; init; }

    [ArrowType("time32[ms]")]
    public TimeOnly Alarm { get; init; }

    [ArrowType("string_view")]
    public string Name { get; init; } = "";

    [ArrowType("binary_view")]
    public byte[] Payload { get; init; } = [];

    [ArrowType("bool8")]
    public bool Flag { get; init; }
}
```

For collections, override element, key, or value types:

```csharp
[ArrowSerializable]
public partial record WithOverrides
{
    [ArrowType(ElementType = "string_view")]
    public List<string> Tags { get; init; } = new();

    [ArrowType(KeyType = "string_view", ValueType = "timestamp[ns, UTC]")]
    public Dictionary<string, DateTime> Events { get; init; } = new();
}
```

#### Timestamp Semantics

| ArrowType | Semantics | Serialize | Deserialize |
|-----------|-----------|-----------|-------------|
| `timestamp[us, UTC]` (default) | Instant | UTC-normalized | `.UtcDateTime` |
| `timestamp[us]` | Wall-clock | Raw ticks preserved | `.DateTime` |

### ArrowIgnore

Excludes a property from serialization. The property must have a default value:

```csharp
[ArrowSerializable]
public partial record WithCache
{
    public int Value { get; init; }

    [ArrowIgnore]
    public int CachedDouble { get; set; }
}
```

### ArrowMetadata

Adds key-value metadata to the Arrow schema (on class) or field (on property). Multiple entries are supported:

```csharp
[ArrowSerializable]
[ArrowMetadata("version", "2")]
[ArrowMetadata("source", "sensor-array")]
public partial record Annotated
{
    [ArrowMetadata("unit", "celsius")]
    public double Temperature { get; init; }

    [ArrowMetadata("unit", "hPa")]
    public double Pressure { get; init; }
}
```

Metadata is stored in the Arrow schema and is accessible from any Arrow implementation.

## Nested Types

Any `[ArrowSerializable]` type can be used as a property of another, mapping to Arrow `Struct`:

```csharp
[ArrowSerializable]
public partial record Address
{
    public string Street { get; init; } = "";
    public string City { get; init; } = "";
}

[ArrowSerializable]
public partial record Customer
{
    public string Name { get; init; } = "";
    public Address Address { get; init; } = new();
    public Address? BillingAddress { get; init; }  // nullable nested
}
```

Nesting works to any depth. Nested types in collections are also supported (`List<Address>`, `Dictionary<string, Address>`).

## Readonly Fields and Constructors

Structs with `readonly` fields are supported. The generator resolves a constructor by matching parameter names to field names (case-insensitive):

```csharp
[ArrowSerializable]
public partial struct Vector3
{
    public readonly float X;
    public readonly float Y;
    public readonly float Z;

    public Vector3(float x, float y, float z)
    {
        X = x;
        Y = y;
        Z = z;
    }
}
```

Fields and properties can be mixed. Fields support all the same attributes as properties (`[ArrowField]`, `[ArrowType]`, `[ArrowIgnore]`, `[ArrowMetadata]`).

If readonly fields exist without a matching constructor, the generator reports diagnostic [`ARROW002`](#source-generator-diagnostics).

## Enum Serialization

Enums are serialized as Arrow `Dictionary(Int16, Utf8)` using their string name:

```csharp
public enum Priority { Low, Medium, High }

[ArrowSerializable]
public partial record Task
{
    public string Title { get; init; } = "";
    public Priority Priority { get; init; }
}
```

This produces compact dictionary-encoded arrays and is compatible with Python's string-based enum encoding.

## Polymorphism

For type hierarchies, use `[ArrowPolymorphic]` on the base type and `[ArrowDerivedType]` to register subtypes:

```csharp
[ArrowPolymorphic]
[ArrowDerivedType(typeof(Circle), "circle")]
[ArrowDerivedType(typeof(Rectangle), "rectangle")]
public abstract partial record Shape;

[ArrowSerializable]
public partial record Circle : Shape
{
    public double Radius { get; init; }
}

[ArrowSerializable]
public partial record Rectangle : Shape
{
    public double Width { get; init; }
    public double Height { get; init; }
}
```

Serialize and deserialize through the base type:

```csharp
var shapes = new Shape[] { new Circle { Radius = 5 }, new Rectangle { Width = 3, Height = 4 } };
var batch = Shape.ToRecordBatch(shapes);
IReadOnlyList<Shape> restored = Shape.ListFromRecordBatch(batch);
```

**Wire format:** Flat schema with a string discriminator column (`$type` by default) plus the union of all derived type fields (all made nullable). Customize the discriminator name:

```csharp
[ArrowPolymorphic(TypeDiscriminatorFieldName = "kind")]
```

## Custom Converters

For types that don't have a built-in mapping, implement `IArrowConverter<T>`:

```csharp
public struct Point2D
{
    public double X { get; set; }
    public double Y { get; set; }
}

public class Point2DConverter : IArrowConverter<Point2D>
{
    public IArrowType ArrowType => StringType.Default;

    public IArrowArray ToArray(Point2D value) =>
        new StringArray.Builder().Append($"{value.X},{value.Y}").Build();

    public IArrowArray ToArray(IReadOnlyList<Point2D> values)
    {
        var b = new StringArray.Builder();
        foreach (var v in values) b.Append($"{v.X},{v.Y}");
        return b.Build();
    }

    public Point2D FromArray(IArrowArray array, int index)
    {
        var parts = ((StringArray)array).GetString(index)!.Split(',');
        return new Point2D
        {
            X = double.Parse(parts[0]),
            Y = double.Parse(parts[1]),
        };
    }
}
```

Apply via `[ArrowType(Converter = ...)]`:

```csharp
[ArrowSerializable]
public partial record WithLocation
{
    public string Name { get; init; } = "";

    [ArrowType(Converter = typeof(Point2DConverter))]
    public Point2D Location { get; init; }
}
```

The converter class must implement `IArrowConverter<T>` and have a parameterless constructor.

## Serialization Callbacks

Types implementing `IArrowSerializationCallback` receive calls during serialization and deserialization:

```csharp
[ArrowSerializable]
public partial class Computed : IArrowSerializationCallback
{
    public int Value { get; set; }

    [ArrowIgnore]
    public int DoubleValue { get; set; }

    public void OnBeforeSerialize()
    {
        // Called before serialization — validate, flush lazy state, etc.
    }

    public void OnAfterDeserialize()
    {
        // Called after deserialization — rebuild computed/cached fields
        DoubleValue = Value * 2;
    }
}
```

- `OnBeforeSerialize()` is called once per instance before it is written to a RecordBatch
- `OnAfterDeserialize()` is called once per instance after it is read from a RecordBatch

Callbacks fire for both single-row and multi-row serialization.

## JSON Schema Emission

Opt in to generate a `static string ArrowSchemaJson` property for cross-language tooling:

```xml
<PropertyGroup>
    <ArrowSerializerEmitSchemaJson>true</ArrowSerializerEmitSchemaJson>
</PropertyGroup>
```

This produces a JSON descriptor of the Arrow schema:

```csharp
string json = SensorReading.ArrowSchemaJson;
```

```json
{
  "type": "SensorReading",
  "namespace": "MyApp",
  "typeName": "SensorReading",
  "metadata": {},
  "fields": [
    {
      "name": "SensorId",
      "propertyName": "SensorId",
      "arrowType": "utf8",
      "typeKind": "String",
      "nullable": false,
      "csharpType": "string"
    },
    {
      "name": "Temperature",
      "propertyName": "Temperature",
      "arrowType": "float64",
      "typeKind": "Double",
      "nullable": false,
      "csharpType": "double"
    }
  ]
}
```

The JSON schema is off by default — it's intended for external tooling (code generators, schema registries), not runtime use.

## RecordBatchBuilder (Reflection-Based)

For quick prototyping or anonymous types, `RecordBatchBuilder` provides reflection-based serialization without any attributes:

```csharp
var data = new[]
{
    new { Name = "Alice", Score = 95.5 },
    new { Name = "Bob", Score = 87.0 },
};

RecordBatch batch = RecordBatchBuilder.FromObjects(data);
```

```csharp
// Single object
RecordBatch single = RecordBatchBuilder.FromObject(new { X = 1, Y = 2 });
```

This is **serialize-only** — anonymous types can't be deserialized. Annotated with `[RequiresUnreferencedCode]` (not AOT-safe). For production use, prefer `[ArrowSerializable]` with the source generator.

When `RecordBatchBuilder` encounters a nested `[ArrowSerializable]` type, it delegates to the generated serializer for correct attribute handling and performance.

## Extension Methods

`ArrowSerializerExtensions` provides convenience methods for any `IArrowSerializer<T>` type:

| Method | Description |
|--------|-------------|
| `value.SerializeToBytes()` | Serialize to Arrow IPC stream bytes |
| `DeserializeFromBytes<T>(bytes)` | Deserialize from Arrow IPC stream bytes |
| `value.SerializeToStream(stream)` | Serialize to a `Stream` |
| `DeserializeFromStream<T>(stream)` | Deserialize from a `Stream` |
| `items.ToRecordBatch()` | `IEnumerable<T>` to multi-row RecordBatch |
| `batch.ToList<T>()` | RecordBatch to `IReadOnlyList<T>` |
| `items.SerializeListToBytes()` | Serialize list to Arrow IPC bytes |
| `DeserializeListFromBytes<T>(bytes)` | Deserialize list from Arrow IPC bytes |

```csharp
// Round-trip through bytes
byte[] bytes = person.SerializeToBytes();
var restored = ArrowSerializerExtensions.DeserializeFromBytes<Person>(bytes);

// Multi-row convenience
var people = new[] { alice, bob };
byte[] listBytes = people.SerializeListToBytes();
IReadOnlyList<Person> all = ArrowSerializerExtensions.DeserializeListFromBytes<Person>(listBytes);
```

## Source Generator Diagnostics

The source generator reports compile-time diagnostics for common mistakes:

| ID | Severity | Description |
|----|----------|-------------|
| `ARROW001` | Error | Type with `[ArrowSerializable]` must be declared `partial` |
| `ARROW002` | Error | Readonly fields found but no constructor with matching parameters |
| `ARROW003` | Warning | Unsupported property type (e.g. `Queue<T>`, `Stack<T>`) — property will be skipped |
| `ARROW004` | Error | Duplicate Arrow field names (e.g. two properties with `[ArrowField("same")]`) |
| `ARROW005` | Warning | Get-only property without `init` accessor — property will be skipped |
| `ARROW006` | Warning | Arrow attribute on private or static member — attribute is ignored |

## Cross-Language Compatibility

Serialized data uses the standard [Arrow IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc) and is compatible with any Arrow implementation (Python, Java, Rust, C++, Go, etc.).

```python
# Python — read data serialized by C#
import pyarrow as pa

with open("data.arrow", "rb") as f:
    reader = pa.ipc.open_stream(f)
    table = reader.read_all()

print(table.to_pandas())
```

Type mappings are designed to match Python `pyarrow` conventions:
- Enums as `Dictionary(Int16, Utf8)`
- Timestamps as `Timestamp(us, UTC)` by default
- Nested records as `Struct`
- Lists, maps, and sets follow standard Arrow nested types

## License

Apache License, Version 2.0
