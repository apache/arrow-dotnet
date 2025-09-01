---
_layout: landing
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Arrow .NET

An implementation of Arrow targeting .NET.

See our current [feature matrix](https://github.com/apache/arrow/blob/main/docs/source/status.rst)
for currently available features.

## Implementation

- Arrow specification 1.0.0. (Support for reading 0.11+.)
- C# 11
- .NET Standard 2.0, .NET 6.0, .NET 8.0 and .NET Framework 4.6.2
- Asynchronous I/O
- Uses modern .NET runtime features such as **Span&lt;T&gt;**, **Memory&lt;T&gt;**, **MemoryManager&lt;T&gt;**, and **System.Buffers** primitives for memory allocation, memory storage, and fast serialization.
- Uses **Acyclic Visitor Pattern** for array types and arrays to facilitate serialization, record batch traversal, and format growth.

## Known Issues

- Cannot read Arrow files containing tensors.
- Cannot easily modify allocation strategy without implementing a custom memory pool. All allocations are currently 64-byte aligned and padded to 8-bytes.
- Default memory allocation strategy uses an over-allocation strategy with pointer fixing, which results in significant memory overhead for small buffers. A buffer that requires a single byte for storage may be backed by an allocation of up to 64-bytes to satisfy alignment requirements.
- There are currently few builder APIs available for specific array types. Arrays must be built manually with an arrow buffer builder abstraction.
- FlatBuffer code generation is not included in the build process.
- Serialization implementation does not perform exhaustive validation checks during deserialization in every scenario.
- Throws exceptions with vague, inconsistent, or non-localized messages in many situations
- Throws exceptions that are non-specific to the Arrow implementation in some circumstances where it probably should (eg. does not throw ArrowException exceptions)
- Lack of code documentation
- Lack of usage examples

## Usage

Example demonstrating reading [RecordBatches](xref:Apache.Arrow.RecordBatch) from an Arrow IPC file using an
[ArrowFileReader](xref:Apache.Arrow.Ipc.ArrowFileReader):

    using System.Diagnostics;
    using System.IO;
    using System.Threading.Tasks;
    using Apache.Arrow;
    using Apache.Arrow.Ipc;

    public static async Task<RecordBatch> ReadArrowAsync(string filename)
    {
        using (var stream = File.OpenRead(filename))
        using (var reader = new ArrowFileReader(stream))
        {
            var recordBatch = await reader.ReadNextRecordBatchAsync();
            Debug.WriteLine("Read record batch with {0} column(s)", recordBatch.ColumnCount);
            return recordBatch;
        }
    }


## Status

### Memory Management

- Allocations are 64-byte aligned and padded to 8-bytes.
- Allocations are automatically garbage collected

### Arrays

#### Primitive Types

- [Int8](xref:Apache.Arrow.Types.Int8Type), [Int16](xref:Apache.Arrow.Types.Int16Type), [Int32](xref:Apache.Arrow.Types.Int32Type), [Int64](xref:Apache.Arrow.Types.Int64Type)
- [UInt8](xref:Apache.Arrow.Types.UInt8Type), [UInt16](xref:Apache.Arrow.Types.UInt16Type), [UInt32](xref:Apache.Arrow.Types.UInt32Type), [UInt64](xref:Apache.Arrow.Types.UInt64Type)
- [Float](xref:Apache.Arrow.Types.FloatType), [Double](xref:Apache.Arrow.Types.DoubleType), [Half-float](xref:Apache.Arrow.Types.HalfFloatType) (.NET 6+)
- [Binary](xref:Apache.Arrow.Types.BinaryType) (variable-length)
- [String](xref:Apache.Arrow.Types.StringType) (utf-8)
- [Null](xref:Apache.Arrow.Types.NullType)

#### Parametric Types

- [Timestamp](xref:Apache.Arrow.Types.TimestampType)
- [Date32](xref:Apache.Arrow.Types.Date32Type), [Date64](xref:Apache.Arrow.Types.Date64Type)
- [Decimal32](xref:Apache.Arrow.Types.Decimal32Type), [Decimal64](xref:Apache.Arrow.Types.Decimal64Type), [Decimal128](xref:Apache.Arrow.Types.Decimal128Type), [Decimal256](xref:Apache.Arrow.Types.Decimal256Type)
- [Time32](xref:Apache.Arrow.Types.Time32Type), [Time64](xref:Apache.Arrow.Types.Time64Type)
- [Binary](xref:Apache.Arrow.Types.BinaryType) (fixed-length)
- [List](xref:Apache.Arrow.Types.ListType)
- [Struct](xref:Apache.Arrow.Types.StructType)
- [Union](xref:Apache.Arrow.Types.UnionType)
- [Map](xref:Apache.Arrow.Types.MapType)
- [Duration](xref:Apache.Arrow.Types.DurationType)
- [Interval](xref:Apache.Arrow.Types.IntervalType)

#### Type Metadata

- Data Types
- [Fields](xref:Apache.Arrow.Field)
- [Schema](xref:Apache.Arrow.Schema)

#### Serialization

- File [Reader](xref:Apache.Arrow.Ipc.ArrowFileReader) and [Writer](xref:Apache.Arrow.Ipc.ArrowFileWriter)
- Stream [Reader](xref:Apache.Arrow.Ipc.ArrowStreamReader) and [Writer](xref:Apache.Arrow.Ipc.ArrowStreamWriter)

### IPC Format

#### Compression

- Buffer compression and decompression is supported, but requires installing the `Apache.Arrow.Compression` package.
  When reading compressed data, you must pass an [CompressionCodecFactory](xref:Apache.Arrow.Compression.CompressionCodecFactory)
  instance to the [ArrowFileReader](xref:Apache.Arrow.Ipc.ArrowFileReader) or
  [ArrowStreamReader](xref:Apache.Arrow.Ipc.ArrowStreamReader) constructor, and when writing compressed data a
  [CompressionCodecFactory](xref:Apache.Arrow.Compression.CompressionCodecFactory) must be set in the
  [IpcOptions](xref:Apache.Arrow.Ipc.IpcOptions).
  Alternatively, a custom implementation of [ICompressionCodecFactory](xref:Apache.Arrow.Ipc.ICompressionCodecFactory) can be used.

### Not Implemented

- Serialization
    - Exhaustive validation
    - Run End Encoding
- Types
    - Tensor
- Arrays
    - Large Arrays. There are large array types provided to help with interoperability with other libraries,
      but these do not support buffers larger than 2 GiB and an exception will be raised if trying to import an array that is too large.
        - [Large Binary](xref:Apache.Arrow.Types.LargeBinaryType)
        - [Large List](xref:Apache.Arrow.Types.LargeListType)
        - [Large String](xref:Apache.Arrow.Types.LargeStringType)
    - Views
        - [Binary View](xref:Apache.Arrow.Types.BinaryViewType)
        - [List View](xref:Apache.Arrow.Types.ListViewType)
        - [String View](xref:Apache.Arrow.Types.StringViewType)
- Array Operations
    - Equality / Comparison
    - Casting
- Compute
    - There is currently no API available for a compute / kernel abstraction.
