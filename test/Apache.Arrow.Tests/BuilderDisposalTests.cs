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
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Tests
{
    /// <summary>
    /// Tests that an explicitly passed allocator controls allocation exactly: every buffer of the
    /// result comes from it, and disposing the builders, arrays, readers and writers involved
    /// releases everything it handed out. This is what makes caller-owned allocators usable.
    /// </summary>
    public class BuilderDisposalTests
    {
        [Fact]
        public void PrimitiveBuilderAndArrayReleaseEverything()
        {
            Run(allocator =>
            {
                Int32Array array;
                using (Int32Array.Builder builder = new Int32Array.Builder(0, allocator))
                {
                    array = builder.Append(1).AppendNull().Append(3).Build();
                }

                allocator.AssertOwns(array);
                Assert.Equal(3, array.GetValue(2));
                array.Dispose();
            });
        }

        [Fact]
        public void BuilderCanBeDisposedBeforeOrAfterArray()
        {
            Run(allocator =>
            {
                Int64Array.Builder builder = new Int64Array.Builder(8, allocator).Append(1).Append(2);
                Int64Array array = builder.Build();

                array.Dispose();
                builder.Dispose();
            });

            Run(allocator =>
            {
                Int64Array.Builder builder = new Int64Array.Builder(8, allocator).Append(1).Append(2);
                Int64Array array = builder.Build();

                builder.Dispose();
                Assert.Equal(2, array.GetValue(1));
                array.Dispose();
            });
        }

        [Fact]
        public void DisposingTheArrayReleasesItsMemoryWhenTheBuilderIsNeverDisposed()
        {
            // The usual chained style never disposes the builder. With a caller-owned allocator,
            // a builder that held a reference to what it built would leak it permanently.
            Run(allocator =>
            {
                using (Int32Array array = new Int32Array.Builder(16, allocator).Append(1).AppendNull().Build())
                {
                    Assert.Equal(1, array.GetValue(0));
                }

                using (StringArray strings = new StringArray.Builder(allocator).Append("a").AppendNull().Build())
                {
                    Assert.Equal("a", strings.GetString(0));
                }
            });
        }

        [Fact]
        public void WrappedBuilderReleasesInnerBuilder()
        {
            Run(allocator =>
            {
                using TimestampArray.Builder builder = new TimestampArray.Builder(TimestampType.Default, 4, allocator);
                using TimestampArray array = builder.Append(DateTimeOffset.FromUnixTimeMilliseconds(0)).Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void StringBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using StringArray.Builder builder = new StringArray.Builder(allocator);
                using StringArray array = builder.Append("a").AppendNull().Append("bcd").Build();
                allocator.AssertOwns(array);
                Assert.Equal("bcd", array.GetString(2));
            });
        }

        [Fact]
        public void BooleanBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using BooleanArray.Builder builder = new BooleanArray.Builder(allocator);
                using BooleanArray array = builder.Append(true).AppendNull().Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void DecimalBuilderUsesItsAllocator()
        {
            Run(allocator =>
            {
                using Decimal128Array.Builder builder = new Decimal128Array.Builder(new Decimal128Type(10, 2), allocator);
                using Decimal128Array array = builder.Append(1.25m).AppendNull().Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void ListBuilderPassesItsAllocatorToItsChild()
        {
            Run(allocator =>
            {
                using ListArray.Builder builder = new ListArray.Builder(Int32Type.Default, allocator);
                Int32Array.Builder values = (Int32Array.Builder)builder.ValueBuilder;
                builder.Append();
                values.Append(1).Append(2);
                builder.AppendNull();

                using ListArray array = builder.Build();
                allocator.AssertOwns(array);
                Assert.Equal(2, array.GetValueLength(0));
            });
        }

        [Fact]
        public void MapBuilderPassesItsAllocatorToItsChildren()
        {
            Run(allocator =>
            {
                using MapArray.Builder builder = new MapArray.Builder(new MapType(StringType.Default, Int32Type.Default), allocator);
                builder.Append();
                ((StringArray.Builder)builder.KeyBuilder).Append("k");
                ((Int32Array.Builder)builder.ValueBuilder).Append(1);

                using MapArray array = builder.Build();
                allocator.AssertOwns(array);
            });
        }

        [Fact]
        public void ListOfTimestampsKeepsTheElementType()
        {
            // The builder factory used to ignore a timestamp element type's unit and time zone.
            TimestampType elementType = new TimestampType(TimeUnit.Microsecond, "UTC");
            using ListArray.Builder builder = new ListArray.Builder(elementType);
            builder.Append();
            ((TimestampArray.Builder)builder.ValueBuilder).Append(DateTimeOffset.FromUnixTimeMilliseconds(1));

            using ListArray array = builder.Build();

            TimestampType actual = Assert.IsType<TimestampType>(array.Values.Data.DataType);
            Assert.Equal(TimeUnit.Microsecond, actual.Unit);
            Assert.Equal("UTC", actual.Timezone);
        }

        [Fact]
        public void RecordBatchColumnHelpersUseTheBatchAllocator()
        {
            Run(allocator =>
            {
                using RecordBatch batch = new RecordBatch.Builder(allocator)
                    .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                    .Append("strings", true, col => col.String(b => b.Append("x").AppendNull()))
                    .Append("times", true, col => col.Timestamp(b => b.Append(DateTimeOffset.FromUnixTimeMilliseconds(5))))
                    .Build();

                allocator.AssertOwns(batch);
                Assert.Equal(2, batch.Length);
            });
        }

        [Fact]
        public void ConcatenationUsesItsAllocator()
        {
            Run(allocator =>
            {
                using Int32Array.Builder firstBuilder = new Int32Array.Builder(0, allocator);
                using Int32Array.Builder secondBuilder = new Int32Array.Builder(0, allocator);
                using Int32Array first = firstBuilder.Append(1).AppendNull().Build();
                using Int32Array second = secondBuilder.Append(3).Build();

                using IArrowArray result = ArrowArrayConcatenator.Concatenate(new IArrowArray[] { first, second }, allocator);
                allocator.AssertOwns(result);
                Assert.Equal(3, result.Length);
            });
        }

        [Fact]
        public void CloneUsesItsAllocator()
        {
            Run(allocator =>
            {
                using ArrowBuffer source = new ArrowBuffer(new byte[] { 1, 2, 3 });
                using ArrowBuffer clone = source.Clone(allocator);
                allocator.AssertOwns(clone);
                Assert.Equal(3, clone.Span[2]);
            });
        }

        [Fact]
        public void RecordBatchCloneUsesItsAllocator()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).AppendNull()))
                .Append("strings", true, col => col.String(b => b.Append("x")))
                .Build();

            Run(allocator =>
            {
                using RecordBatch clone = original.Clone(allocator);
                allocator.AssertOwns(clone);
            });
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public void IpcContextsControlReadAndWriteAllocations(bool fileFormat, bool compress)
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2).Append(3)))
                .Append("strings", true, col => col.String(b => b.Append("x").AppendNull().Append("yz")))
                .Build();

            IpcOptions options = compress ? new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame } : null;
            MemoryStream stream = new MemoryStream();

            // The codec factory comes from the context, not the options: the writer falls back to it.
            Run(writeAllocator =>
            {
                ArrowContext context = new ArrowContext(writeAllocator, new PassThroughCodecFactory());
                using ArrowStreamWriter writer = fileFormat
                    ? new ArrowFileWriter(context, stream, original.Schema, leaveOpen: true, options)
                    : new ArrowStreamWriter(context, stream, original.Schema, leaveOpen: true, options);
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }, expectAllocations: compress);

            Run(readAllocator =>
            {
                ArrowContext context = new ArrowContext(readAllocator, new PassThroughCodecFactory());

                // Wrapping the MemoryStream stops the reader from slicing its buffer directly,
                // so that message bodies are read into memory from the context's allocator.
                stream.Position = 0;
                using Stream wrapped = new BufferedStream(stream);
                using ArrowStreamReader reader = fileFormat
                    ? new ArrowFileReader(context, wrapped, leaveOpen: true)
                    : new ArrowStreamReader(context, wrapped, leaveOpen: true);
                using RecordBatch batch = reader.ReadNextRecordBatch();

                readAllocator.AssertOwns(batch);
                Assert.Equal(3, batch.Length);
                Assert.Equal("yz", ((StringArray)batch.Column(1)).GetString(2));
            });
        }

        [Fact]
        public void InMemoryReaderUsesContextAllocatorForDecompression()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                .Build();

            MemoryStream stream = new MemoryStream();
            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = new PassThroughCodecFactory(),
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, original.Schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            Run(allocator =>
            {
                ArrowContext context = new ArrowContext(allocator, new PassThroughCodecFactory());
                using ArrowStreamReader reader = new ArrowStreamReader(context, new ReadOnlyMemory<byte>(stream.ToArray()));
                using RecordBatch batch = reader.ReadNextRecordBatch();

                allocator.AssertOwns(batch);
                Assert.Equal(2, ((Int32Array)batch.Column(0)).GetValue(1));
            });
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void FailedWritesReleaseTheWritersTemporaryBuffers(bool async)
        {
            // A sliced string column makes the writer re-base its offsets, and compression gives
            // it an output buffer per array buffer, so the writer holds several temporary buffers.
            StringArray strings = new StringArray.Builder().Append("a").Append("bb").Append("ccc").Append("dddd").Build();
            using RecordBatch batch = new RecordBatch.Builder()
                .Append("s", false, (StringArray)strings.Slice(1, 3))
                .Append("i", false, col => col.Int32(b => b.Append(1).Append(2).Append(3)))
                .Build();
            IpcOptions options = new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame };

            // Count the writes a successful run makes, then fail at each of them in turn.
            FailingStream counter = new FailingStream(failOnWrite: int.MaxValue);
            Write(counter, new TrackingAllocator());
            Assert.True(counter.Writes > 3);

            for (int failOn = 1; failOn <= counter.Writes; failOn++)
            {
                int failure = failOn;
                Run(allocator =>
                {
                    FailingStream stream = new FailingStream(failure);
                    Assert.ThrowsAny<IOException>(() => Write(stream, allocator));
                }, expectAllocations: false);
            }

            void Write(Stream stream, TrackingAllocator allocator)
            {
                ArrowContext context = new ArrowContext(allocator, new PassThroughCodecFactory());
                using ArrowStreamWriter writer = new ArrowStreamWriter(context, stream, batch.Schema, leaveOpen: true, options);
                if (async)
                {
                    writer.WriteRecordBatchAsync(batch).GetAwaiter().GetResult();
                }
                else
                {
                    writer.WriteRecordBatch(batch);
                }
            }
        }

        [Fact]
        public void TruncatedStreamReleasesTheMessageBody()
        {
            byte[] data = WriteBatch(compress: false);
            byte[] truncated = new byte[data.Length - 16];
            System.Array.Copy(data, truncated, truncated.Length);

            Run(allocator =>
            {
                // A stream that is not a MemoryStream makes the reader allocate the body.
                using Stream stream = new BufferedStream(new MemoryStream(truncated));
                using ArrowStreamReader reader = new ArrowStreamReader(new ArrowContext(allocator), stream);
                Assert.ThrowsAny<Exception>(() => reader.ReadNextRecordBatch());
            });
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void FailedParsingReleasesTheMessageBody(bool fromMemoryStream)
        {
            byte[] data = WriteBatch(compress: true);

            // Decompression fails while the message is being parsed, after the body is allocated.
            Run(allocator =>
            {
                ArrowContext context = new ArrowContext(allocator, new FailingCodecFactory());
                using Stream stream = fromMemoryStream
                    ? new MemoryStream(data)
                    : new BufferedStream(new MemoryStream(data));
                using ArrowStreamReader reader = new ArrowStreamReader(context, stream);
                Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
            });
        }

        private static byte[] WriteBatch(bool compress)
        {
            using RecordBatch batch = new RecordBatch.Builder()
                .Append("i", false, col => col.Int32(b => b.Append(1).Append(2).Append(3)))
                .Build();
            IpcOptions options = compress
                ? new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame, CompressionCodecFactory = new PassThroughCodecFactory() }
                : null;

            MemoryStream stream = new MemoryStream();
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, batch.Schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            return stream.ToArray();
        }

        [Theory]
        [InlineData(false, Compression.None)]
        [InlineData(true, Compression.None)]
        [InlineData(false, Compression.Compressed)]
        [InlineData(false, Compression.StoredUncompressed)]
        [InlineData(true, Compression.StoredUncompressed)]
        public void ReadingDictionaryBatchesReleasesTheirBodies(bool fromMemoryStream, Compression compression)
        {
            byte[] data = Concat(WriteDictionaryStream(compression, "x", "y"), WriteDictionaryStream(compression, "p", "q"));

            Run(allocator =>
            {
                List<RecordBatch> batches = ReadAll(data, Context(allocator, compression), fromMemoryStream, out ArrowStreamReader reader);
                reader.Dispose();
                Assert.Equal(2, batches.Count);
                batches.ForEach(b => b.Dispose());
            });
        }

        [Theory]
        [InlineData(false, Compression.None)]
        [InlineData(true, Compression.None)]
        [InlineData(false, Compression.StoredUncompressed)]
        public void BatchesKeepTheirDictionaryAfterItIsReplacedAndTheReaderIsDisposed(bool fromMemoryStream, Compression compression)
        {
            // The second stream's dictionary replaces the first's (both have id 0), and the
            // reader is disposed while both batches are still in use.
            byte[] data = Concat(WriteDictionaryStream(compression, "x", "y"), WriteDictionaryStream(compression, "p", "q"));
            PooledManagedAllocator allocator = new PooledManagedAllocator(new PooledManagedAllocatorOptions { PoisonDisposedBuffers = true });

            List<RecordBatch> batches = ReadAll(data, Context(allocator, compression), fromMemoryStream, out ArrowStreamReader reader);
            reader.Dispose();

            // Churn the pool, so that released memory would be reused and overwritten.
            for (int i = 0; i < 4; i++)
            {
                using StringArray filler = new StringArray.Builder(allocator).Append("zzzzzzzzzzzzzzzz").Build();
            }

            AssertDictionaryValues(batches[0], "x", "y");
            AssertDictionaryValues(batches[1], "p", "q");

            // Each batch holds its own reference: disposing one leaves the other intact.
            batches[0].Dispose();
            AssertDictionaryValues(batches[1], "p", "q");
            batches[1].Dispose();
        }

        public enum Compression
        {
            None,
            Compressed,

            /// <summary>Compressed, but every buffer is stored uncompressed (length -1).</summary>
            StoredUncompressed,
        }

        private static ICompressionCodecFactory Codecs(Compression compression) => compression switch
        {
            Compression.Compressed => new PassThroughCodecFactory(),
            Compression.StoredUncompressed => new IncompressibleCodecFactory(),
            _ => null,
        };

        private static ArrowContext Context(MemoryAllocator allocator, Compression compression) =>
            compression == Compression.None ? new ArrowContext(allocator) : new ArrowContext(allocator, Codecs(compression));

        private static void AssertDictionaryValues(RecordBatch batch, params string[] expected)
        {
            DictionaryArray column = (DictionaryArray)batch.Column(0);
            StringArray dictionary = (StringArray)column.Dictionary;
            Assert.Equal(expected.Length, dictionary.Length);
            for (int i = 0; i < expected.Length; i++)
            {
                Assert.Equal(expected[i], dictionary.GetString(i));
            }
            Assert.Equal(1, ((Int32Array)column.Indices).GetValue(1));
        }

        private static List<RecordBatch> ReadAll(byte[] data, ArrowContext context, bool fromMemoryStream, out ArrowStreamReader reader)
        {
            // A stream that is not a MemoryStream makes the reader read through a different path.
            Stream stream = fromMemoryStream
                ? new MemoryStream(data)
                : new BufferedStream(new MemoryStream(data));
            reader = new ArrowStreamReader(context, stream);

            List<RecordBatch> batches = new List<RecordBatch>();
            RecordBatch batch;
            while ((batch = reader.ReadNextRecordBatch()) != null)
            {
                batches.Add(batch);
            }
            return batches;
        }

        /// <summary>
        /// Writes a stream holding one batch with a dictionary-encoded column, without the
        /// end-of-stream marker, so that streams can be concatenated. Later schema messages are
        /// ignored by the reader, and each stream's dictionary has id 0, so each replaces the last.
        /// </summary>
        private static byte[] WriteDictionaryStream(Compression compression, params string[] values)
        {
            StringArray.Builder dictionaryBuilder = new StringArray.Builder();
            foreach (string value in values)
            {
                dictionaryBuilder.Append(value);
            }
            DictionaryType type = new DictionaryType(Int32Type.Default, StringType.Default, false);
            DictionaryArray column = new DictionaryArray(
                type,
                new Int32Array.Builder().Append(0).Append(1).Append(0).Build(),
                dictionaryBuilder.Build());
            Schema schema = new Schema.Builder().Field(f => f.Name("d").DataType(type).Nullable(false)).Build();
            using RecordBatch batch = new RecordBatch(schema, new IArrowArray[] { column }, column.Length);

            MemoryStream stream = new MemoryStream();
            IpcOptions options = compression == Compression.None
                ? null
                : new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame, CompressionCodecFactory = Codecs(compression) };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(batch);
            }
            return stream.ToArray();
        }

        private static byte[] Concat(params byte[][] parts)
        {
            MemoryStream result = new MemoryStream();
            foreach (byte[] part in parts)
            {
                result.Write(part, 0, part.Length);
            }
            return result.ToArray();
        }

        [Fact]
        public void FailingPartwayThroughAFieldReleasesItsBuffers()
        {
            // Nulls give the columns a validity buffer as well as their data buffers, and the list
            // column has a child, so failures land between a field's buffers and in its children.
            ListArray.Builder listBuilder = new ListArray.Builder(Int32Type.Default);
            Int32Array.Builder listValues = (Int32Array.Builder)listBuilder.ValueBuilder;
            listBuilder.Append();
            listValues.Append(1).Append(2);
            listBuilder.AppendNull();
            listBuilder.Append();
            listValues.Append(3);
            IArrowArray[] columns =
            {
                new Int32Array.Builder().Append(1).AppendNull().Append(3).Build(),
                new StringArray.Builder().Append("a").AppendNull().Append("ccc").Build(),
                listBuilder.Build(),
            };
            Schema schema = new Schema.Builder()
                .Field(f => f.Name("i").DataType(Int32Type.Default))
                .Field(f => f.Name("s").DataType(StringType.Default))
                .Field(f => f.Name("l").DataType(new ListType(Int32Type.Default)))
                .Build();
            using RecordBatch batch = new RecordBatch(schema, columns, 3);

            IpcOptions options = new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame, CompressionCodecFactory = new PassThroughCodecFactory() };
            MemoryStream written = new MemoryStream();
            using (ArrowStreamWriter writer = new ArrowStreamWriter(written, schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(batch);
                writer.WriteEnd();
            }
            byte[] data = written.ToArray();

            // Count the decompressions a successful read makes, then fail at each in turn.
            FailOnNthDecompressionFactory counter = new FailOnNthDecompressionFactory(int.MaxValue);
            using (ArrowStreamReader reader = new ArrowStreamReader(new ArrowContext(new TrackingAllocator(), counter), new MemoryStream(data)))
            using (RecordBatch read = reader.ReadNextRecordBatch())
            {
                Assert.Equal(3, read.Length);
            }
            Assert.True(counter.Decompressions > 4);

            for (int failOn = 1; failOn <= counter.Decompressions; failOn++)
            {
                int failure = failOn;
                Run(allocator =>
                {
                    ArrowContext context = new ArrowContext(allocator, new FailOnNthDecompressionFactory(failure));
                    using ArrowStreamReader reader = new ArrowStreamReader(context, new BufferedStream(new MemoryStream(data)));
                    Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
                });
            }
        }

        public static TheoryData<string> CompositeTypes => new TheoryData<string> { "List", "LargeList", "ListView", "LargeListView", "FixedSizeList", "Map" };

        [Theory]
        [MemberData(nameof(CompositeTypes))]
        public void FailingToBuildAChildReleasesTheParentsBuffers(string kind)
        {
            // The child is a fixed-size list holding one value where it needs two, so its Build
            // throws, after the parent has built its own buffers. The parent's null makes it build
            // a validity bitmap as well.
            FixedSizeListType pair = new FixedSizeListType(Int32Type.Default, 2);
            IArrowType type = kind switch
            {
                "List" => new ListType(pair),
                "LargeList" => new LargeListType(pair),
                "ListView" => new ListViewType(pair),
                "LargeListView" => new LargeListViewType(pair),
                "FixedSizeList" => new FixedSizeListType(pair, 1),
                _ => new MapType(pair, Int32Type.Default),
            };

            Run(allocator =>
            {
                IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> builder = ArrowArrayBuilderFactory.Build(type, allocator);
                IArrowArrayBuilder<IArrowArray, IArrowArrayBuilder<IArrowArray>> child;
                switch (builder)
                {
                    case ListArray.Builder b: b.AppendNull(); child = b.ValueBuilder; break;
                    case LargeListArray.Builder b: b.AppendNull(); child = b.ValueBuilder; break;
                    case ListViewArray.Builder b: b.AppendNull(); child = b.ValueBuilder; break;
                    case LargeListViewArray.Builder b: b.AppendNull(); child = b.ValueBuilder; break;
                    case FixedSizeListArray.Builder b: b.AppendNull(); child = b.ValueBuilder; break;
                    case MapArray.Builder b: b.AppendNull(); child = b.KeyBuilder; break;
                    default: throw new InvalidOperationException(builder.GetType().Name);
                }
                ((Int32Array.Builder)((FixedSizeListArray.Builder)child).ValueBuilder).Append(1);

                Assert.Throws<ArgumentOutOfRangeException>(() => builder.Build(allocator));
                ((IDisposable)builder).Dispose();
            });
        }

        [Fact]
        public void FailedDecompressionReleasesItsOutputBuffer()
        {
            using RecordBatch original = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1).Append(2)))
                .Build();

            MemoryStream stream = new MemoryStream();
            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = new PassThroughCodecFactory(),
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(stream, original.Schema, leaveOpen: true, options))
            {
                writer.WriteRecordBatch(original);
                writer.WriteEnd();
            }

            // The in-memory reader allocates nothing but decompression output, so anything left
            // outstanding after the failure is a leaked output buffer.
            Run(allocator =>
            {
                ArrowContext context = new ArrowContext(allocator, new FailingCodecFactory());
                using ArrowStreamReader reader = new ArrowStreamReader(context, new ReadOnlyMemory<byte>(stream.ToArray()));

                Assert.Throws<InvalidDataException>(() => reader.ReadNextRecordBatch());
            });
        }

        [Fact]
        public void CodecFactoryInOptionsTakesPrecedenceOverContext()
        {
            PassThroughCodecFactory fromOptions = new PassThroughCodecFactory();
            PassThroughCodecFactory fromContext = new PassThroughCodecFactory();
            using RecordBatch batch = new RecordBatch.Builder()
                .Append("ints", false, col => col.Int32(b => b.Append(1)))
                .Build();

            IpcOptions options = new IpcOptions
            {
                CompressionCodec = CompressionCodecType.Lz4Frame,
                CompressionCodecFactory = fromOptions,
            };
            using (ArrowStreamWriter writer = new ArrowStreamWriter(
                new ArrowContext(compressionCodecFactory: fromContext), new MemoryStream(), batch.Schema, options: options))
            {
                writer.WriteRecordBatch(batch);
            }

            Assert.True(fromOptions.CodecsCreated > 0);
            Assert.Equal(0, fromContext.CodecsCreated);
        }

        [Fact]
        public void WriterWithoutAnyCodecFactoryStillRejectsCompression()
        {
            Schema schema = new Schema.Builder().Field(f => f.Name("x").DataType(Int32Type.Default)).Build();
            IpcOptions options = new IpcOptions { CompressionCodec = CompressionCodecType.Lz4Frame };

            Assert.Throws<ArgumentException>(() => new ArrowStreamWriter(new ArrowContext(), new MemoryStream(), schema, options: options));
        }

        [Fact]
        public void WriterContextConstructorsRejectNullContext()
        {
            Schema schema = new Schema.Builder().Field(f => f.Name("x").DataType(Int32Type.Default)).Build();

            Assert.Throws<ArgumentNullException>(() => new ArrowStreamWriter((ArrowContext)null, new MemoryStream(), schema));
            Assert.Throws<ArgumentNullException>(() => new ArrowFileWriter((ArrowContext)null, new MemoryStream(), schema));
        }

        /// <summary>
        /// Runs <paramref name="test"/> with a fresh caller-owned allocator, then asserts that it
        /// was used and that every allocation it handed out has been released.
        /// </summary>
        private static void Run(Action<TrackingAllocator> test, bool expectAllocations = true)
        {
            TrackingAllocator allocator = new TrackingAllocator();
            test(allocator);

            if (expectAllocations)
            {
                Assert.True(allocator.Statistics.Allocations > 0, "The test should have allocated.");
            }
            Assert.Equal(0, allocator.LiveAllocations);
        }

        /// <summary>
        /// A caller-owned allocator that records the arrays it hands out and counts those not yet
        /// released. Its memory is managed, so a leak fails the test rather than the process.
        /// </summary>
        private sealed class TrackingAllocator : MemoryAllocator
        {
            private readonly HashSet<byte[]> _handedOut = new HashSet<byte[]>(ReferenceComparer.Instance);
            private int _live;

            public int LiveAllocations => Volatile.Read(ref _live);

            public override AllocationLifetime Lifetime => AllocationLifetime.CallerOwned;

            protected override IMemoryOwner<byte> AllocateInternal(int length, out int bytesAllocated)
            {
                byte[] buffer = new byte[length];
                lock (_handedOut)
                {
                    _handedOut.Add(buffer);
                }
                Interlocked.Increment(ref _live);
                bytesAllocated = length;
                return new Owner(this, buffer);
            }

            public void AssertOwns(RecordBatch batch)
            {
                for (int i = 0; i < batch.ColumnCount; i++)
                {
                    AssertOwns(batch.Column(i));
                }
            }

            public void AssertOwns(IArrowArray array) => AssertOwns(array.Data);

            public void AssertOwns(ArrayData data)
            {
                foreach (ArrowBuffer buffer in data.Buffers)
                {
                    AssertOwns(buffer);
                }
                foreach (ArrayData child in data.Children ?? System.Array.Empty<ArrayData>())
                {
                    AssertOwns(child);
                }
            }

            public void AssertOwns(ArrowBuffer buffer)
            {
                if (buffer.IsEmpty)
                {
                    return;
                }

                Assert.True(MemoryMarshal.TryGetArray(buffer.Memory, out ArraySegment<byte> segment),
                    "Buffer memory is not from this allocator (it is not a managed array).");
                lock (_handedOut)
                {
                    Assert.True(_handedOut.Contains(segment.Array), "Buffer memory is not from this allocator.");
                }
            }

            private sealed class Owner : IMemoryOwner<byte>
            {
                private readonly TrackingAllocator _allocator;
                private byte[] _buffer;

                public Owner(TrackingAllocator allocator, byte[] buffer)
                {
                    _allocator = allocator;
                    _buffer = buffer;
                }

                public Memory<byte> Memory =>
                    _buffer ?? throw new ObjectDisposedException(nameof(Owner));

                public void Dispose()
                {
                    if (Interlocked.Exchange(ref _buffer, null) != null)
                    {
                        Interlocked.Decrement(ref _allocator._live);
                    }
                }
            }
        }

        private sealed class ReferenceComparer : IEqualityComparer<byte[]>
        {
            public static readonly ReferenceComparer Instance = new ReferenceComparer();

            public bool Equals(byte[] x, byte[] y) => ReferenceEquals(x, y);

            public int GetHashCode(byte[] obj) => RuntimeHelpers.GetHashCode(obj);
        }

        /// <summary>
        /// A "compression" codec that copies its input unchanged, so that tests can exercise the
        /// compressed IPC code paths without a dependency on a real codec.
        /// </summary>
        private sealed class PassThroughCodecFactory : ICompressionCodecFactory
        {
            private int _codecsCreated;

            public int CodecsCreated => Volatile.Read(ref _codecsCreated);

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType)
            {
                Interlocked.Increment(ref _codecsCreated);
                return new PassThroughCodec();
            }

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);
        }

        /// <summary>
        /// A writable stream whose n-th write throws, once, and which counts the writes it
        /// receives. Writes before and after the failure succeed.
        /// </summary>
        private sealed class FailingStream : Stream
        {
            private readonly int _failOnWrite;
            private readonly MemoryStream _inner = new MemoryStream();

            public FailingStream(int failOnWrite) => _failOnWrite = failOnWrite;

            public int Writes { get; private set; }

            public override bool CanRead => false;
            public override bool CanSeek => false;
            public override bool CanWrite => true;
            public override long Length => _inner.Length;
            public override long Position { get => _inner.Position; set => throw new NotSupportedException(); }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (++Writes == _failOnWrite)
                {
                    throw new IOException("Simulated write failure.");
                }
                _inner.Write(buffer, offset, count);
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                Write(buffer, offset, count);
                return Task.CompletedTask;
            }

#if NET5_0_OR_GREATER
            public override void Write(ReadOnlySpan<byte> buffer) => Write(buffer.ToArray(), 0, buffer.Length);

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                Write(buffer.ToArray(), 0, buffer.Length);
                return default;
            }
#endif

            public override void Flush() { }
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
        }

        /// <summary>
        /// Creates codecs that pass data through, except that the n-th non-empty decompression
        /// (counted across all of the factory's codecs) fails as it would on corrupt input.
        /// </summary>
        private sealed class FailOnNthDecompressionFactory : ICompressionCodecFactory
        {
            private readonly int _failOn;
            private int _decompressions;

            public FailOnNthDecompressionFactory(int failOn) => _failOn = failOn;

            public int Decompressions => Volatile.Read(ref _decompressions);

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType) => new Codec(this);

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);

            private sealed class Codec : ICompressionCodec
            {
                private readonly FailOnNthDecompressionFactory _factory;

                public Codec(FailOnNthDecompressionFactory factory) => _factory = factory;

                public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
                {
                    if (!source.IsEmpty && Interlocked.Increment(ref _factory._decompressions) == _factory._failOn)
                    {
                        throw new InvalidDataException("Corrupt compressed data.");
                    }
                    source.CopyTo(destination);
                    return source.Length;
                }

                public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                    throw new NotSupportedException();

                public void Dispose()
                {
                }
            }
        }

        private sealed class FailingCodecFactory : ICompressionCodecFactory
        {
            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType) => new FailingCodec();

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);
        }

        /// <summary>
        /// A codec whose decompression fails on any data, as it would on corrupt input. Empty
        /// buffers still succeed, so that the failure happens after an output buffer is allocated.
        /// </summary>
        private sealed class FailingCodec : ICompressionCodec
        {
            public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination) =>
                source.IsEmpty ? 0 : throw new InvalidDataException("Corrupt compressed data.");

            public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                throw new NotSupportedException();

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// A codec that never manages to compress, so the writer stores every buffer uncompressed
        /// and the reader views those buffers in the message body.
        /// </summary>
        private sealed class IncompressibleCodecFactory : ICompressionCodecFactory
        {
            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType) => new IncompressibleCodec();

            public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int? compressionLevel) =>
                CreateCodec(compressionCodecType);

            private sealed class IncompressibleCodec : ITryCompressionCodec
            {
                // Only empty buffers, which the writer records as compressed, reach this.
                public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination) =>
                    source.IsEmpty ? 0 : throw new InvalidOperationException("Nothing should have been compressed.");

                public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                    throw new NotSupportedException();

                public bool TryCompress(ReadOnlyMemory<byte> source, Memory<byte> destination, out int bytesWritten)
                {
                    bytesWritten = 0;
                    return false;
                }

                public void Dispose()
                {
                }
            }
        }

        private sealed class PassThroughCodec : ITryCompressionCodec
        {
            public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
            {
                source.CopyTo(destination);
                return source.Length;
            }

            public void Compress(ReadOnlyMemory<byte> source, Stream destination) =>
                destination.Write(source.ToArray(), 0, source.Length);

            public bool TryCompress(ReadOnlyMemory<byte> source, Memory<byte> destination, out int bytesWritten)
            {
                if (source.Length > destination.Length)
                {
                    bytesWritten = 0;
                    return false;
                }

                source.CopyTo(destination);
                bytesWritten = source.Length;
                return true;
            }

            public void Dispose()
            {
            }
        }
    }
}
