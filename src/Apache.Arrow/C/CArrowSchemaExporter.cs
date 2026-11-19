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
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow.Types;

namespace Apache.Arrow.C
{
    public static class CArrowSchemaExporter
    {
        private const string ExtensionNameKey = "ARROW:extension:name";
        private const string ExtensionMetadataKey = "ARROW:extension:metadata";

#if NET5_0_OR_GREATER
        private static unsafe delegate* unmanaged<CArrowSchema*, void> ReleaseSchemaPtr => &ReleaseCArrowSchema;
#else
        internal unsafe delegate void ReleaseArrowSchema(CArrowSchema* cArray);
        private static unsafe readonly NativeDelegate<ReleaseArrowSchema> s_releaseSchema = new NativeDelegate<ReleaseArrowSchema>(ReleaseCArrowSchema);
        private static IntPtr ReleaseSchemaPtr => s_releaseSchema.Pointer;
#endif

        /// <summary>
        /// Export a type to a <see cref="CArrowSchema"/>.
        /// </summary>
        /// <param name="datatype">The datatype to export</param>
        /// <param name="schema">An allocated but uninitialized CArrowSchema pointer.</param>
        /// <example>
        /// <code>
        /// CArrowSchema* exportPtr = CArrowSchema.Create();
        /// CArrowSchemaExporter.ExportType(dataType, exportPtr);
        /// foreign_import_function(exportPtr);
        /// CArrowSchema.Free(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportType(IArrowType datatype, CArrowSchema* schema)
        {
            ExportType(datatype, schema, null);
        }

        private static unsafe void ExportType(IArrowType datatype, CArrowSchema* schema, IReadOnlyDictionary<string, string> metadata)
        {
            if (datatype == null)
            {
                throw new ArgumentNullException(nameof(datatype));
            }
            if (schema == null)
            {
                throw new ArgumentNullException(nameof(schema));
            }

            // The C Data Interface puts the extension metadata of a dictionary's value type on
            // the dictionary child; on the field itself, it means the extension's storage is the
            // whole dictionary. Earlier versions of this library's IPC reader left a value type's
            // extension metadata on the field, so move it only when it names the value type.
            // Unresolved metadata stays put.
            IReadOnlyDictionary<string, string> dictionaryMetadata = null;
            if (datatype is DictionaryType dictionaryType &&
                dictionaryType.ValueType is ExtensionType valueType &&
                metadata != null &&
                metadata.TryGetValue(ExtensionNameKey, out string extName) &&
                extName == valueType.Name)
            {
                metadata = SplitExtensionMetadata(metadata, out dictionaryMetadata);
            }

            schema->format = StringUtil.ToCStringUtf8(GetFormat(datatype));
            schema->name = null;
            schema->metadata = ConstructMetadata(WithExtensionMetadata(datatype, metadata));

            IArrowType physicalType = datatype is ExtensionType ext ? ext.StorageType : datatype;
            schema->flags = GetFlags(physicalType);

            schema->children = ConstructChildren(physicalType, out var numChildren);
            schema->n_children = numChildren;

            schema->dictionary = ConstructDictionary(physicalType, dictionaryMetadata);

            schema->release = ReleaseSchemaPtr;

            schema->private_data = null;
        }

        /// <summary>
        /// Export a field to a <see cref="CArrowSchema"/>.
        /// </summary>
        /// <param name="field">The field to export</param>
        /// <param name="schema">An allocated but uninitialized CArrowSchema pointer.</param>
        /// <example>
        /// <code>
        /// CArrowSchema* exportPtr = CArrowSchema.Create();
        /// CArrowSchemaExporter.ExportType(field, exportPtr);
        /// foreign_import_function(exportPtr);
        /// CArrowSchema.Free(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportField(Field field, CArrowSchema* schema)
        {
            ExportType(field.DataType, schema, field.Metadata);
            schema->name = StringUtil.ToCStringUtf8(field.Name);

            IArrowType physicalType = field.DataType is ExtensionType ext ? ext.StorageType : field.DataType;
            schema->flags = GetFlags(physicalType, field.IsNullable);
        }

        /// <summary>
        /// Export a schema to a <see cref="CArrowSchema"/>.
        /// </summary>
        /// <param name="schema">The schema to export</param>
        /// <param name="out_schema">An allocated but uninitialized CArrowSchema pointer.</param>
        /// <example>
        /// <code>
        /// CArrowSchema* exportPtr = CArrowSchema.Create();
        /// CArrowSchemaExporter.ExportType(schema, exportPtr);
        /// foreign_import_function(exportPtr);
        /// CArrowSchema.Free(exportPtr);
        /// </code>
        /// </example>
        public static unsafe void ExportSchema(Schema schema, CArrowSchema* out_schema)
        {
            var structType = new StructType(schema.FieldsList);
            ExportType(structType, out_schema, schema.Metadata);
        }

        private static char FormatTimeUnit(TimeUnit unit) => unit switch
        {
            TimeUnit.Second => 's',
            TimeUnit.Millisecond => 'm',
            TimeUnit.Microsecond => 'u',
            TimeUnit.Nanosecond => 'n',
            _ => throw new InvalidDataException($"Unsupported time unit for export: {unit}"),
        };

        private static string FormatUnion(UnionType unionType)
        {
            StringBuilder builder = new StringBuilder();
            builder.Append(unionType.Mode switch
            {
                UnionMode.Sparse => "+us:",
                UnionMode.Dense => "+ud:",
                _ => throw new InvalidDataException($"Unsupported union mode for export: {unionType.Mode}"),
            });
            for (int i = 0; i < unionType.TypeIds.Length; i++)
            {
                if (i > 0) { builder.Append(','); }
                builder.Append(unionType.TypeIds[i]);
            }
            return builder.ToString();
        }

        private static string GetFormat(IArrowType datatype)
        {
            switch (datatype)
            {
                case ExtensionType extensionType:
                    return GetFormat(extensionType.StorageType);
                case NullType _: return "n";
                case BooleanType _: return "b";
                // Integers
                case Int8Type _: return "c";
                case UInt8Type _: return "C";
                case Int16Type _: return "s";
                case UInt16Type _: return "S";
                case Int32Type _: return "i";
                case UInt32Type _: return "I";
                case Int64Type _: return "l";
                case UInt64Type _: return "L";
                // Floats
                case HalfFloatType _: return "e";
                case FloatType _: return "f";
                case DoubleType _: return "g";
                // Decimal
                case Decimal32Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale},32";
                case Decimal64Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale},64";
                case Decimal128Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale}";
                case Decimal256Type decimalType:
                    return $"d:{decimalType.Precision},{decimalType.Scale},256";
                // Binary
                case BinaryType _: return "z";
                case BinaryViewType _: return "vz";
                case LargeBinaryType _: return "Z";
                case StringType _: return "u";
                case StringViewType _: return "vu";
                case LargeStringType _: return "U";
                case FixedSizeBinaryType binaryType:
                    return $"w:{binaryType.ByteWidth}";
                // Date
                case Date32Type _: return "tdD";
                case Date64Type _: return "tdm";
                // Time
                case Time32Type timeType:
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                case Time64Type timeType:
                    // Same prefix as Time32, but allowed time units are different.
                    return String.Format("tt{0}", FormatTimeUnit(timeType.Unit));
                // Duration
                case DurationType durationType:
                    return String.Format("tD{0}", FormatTimeUnit(durationType.Unit));
                // Timestamp
                case TimestampType timestampType:
                    return String.Format("ts{0}:{1}", FormatTimeUnit(timestampType.Unit), timestampType.Timezone);
                // Interval
                case IntervalType intervalType:
                    return intervalType.Unit switch
                    {
                        IntervalUnit.YearMonth => "tiM",
                        IntervalUnit.DayTime => "tiD",
                        IntervalUnit.MonthDayNanosecond => "tin",
                        _ => throw new InvalidDataException($"Unsupported interval unit for export: {intervalType.Unit}"),
                    };
                // Nested
                case ListType _: return "+l";
                case ListViewType _: return "+vl";
                case LargeListType _: return "+L";
                case LargeListViewType _: return "+vL";
                case FixedSizeListType fixedListType:
                    return $"+w:{fixedListType.ListSize}";
                case StructType _: return "+s";
                case UnionType u: return FormatUnion(u);
                case MapType _: return "+m";
                case RunEndEncodedType _: return "+r";
                // Dictionary
                case DictionaryType dictionaryType:
                    return GetFormat(dictionaryType.IndexType);
                default: throw new NotImplementedException($"Exporting {datatype.Name} not implemented");
            }
        }

        private static long GetFlags(IArrowType datatype, bool nullable = true)
        {
            long flags = 0;

            if (nullable)
            {
                flags |= CArrowSchema.ArrowFlagNullable;
            }

            if (datatype is DictionaryType dictionaryType)
            {
                if (dictionaryType.Ordered)
                {
                    flags |= CArrowSchema.ArrowFlagDictionaryOrdered;
                }
            }

            if (datatype is MapType mapType && mapType.KeySorted)
            {
                flags |= CArrowSchema.ArrowFlagMapKeysSorted;
            }

            return flags;
        }

        private static unsafe CArrowSchema** ConstructChildren(IArrowType datatype, out long numChildren)
        {
            if (datatype is NestedType nestedType)
            {
                IReadOnlyList<Field> fields = nestedType.Fields;
                int numFields = fields.Count;
                numChildren = numFields;
                if (numFields == 0)
                {
                    throw new NotSupportedException("Exporting nested data types with zero children.");
                }

                var pointerList = (CArrowSchema**)Marshal.AllocHGlobal(numFields * IntPtr.Size);

                for (var i = 0; i < numChildren; i++)
                {
                    CArrowSchema* cSchema = CArrowSchema.Create();
                    ExportField(fields[i], cSchema);
                    pointerList[i] = cSchema;
                }

                return pointerList;

            }
            else
            {
                numChildren = 0;
                return null;
            }
        }

        private static unsafe CArrowSchema* ConstructDictionary(IArrowType datatype, IReadOnlyDictionary<string, string> metadata)
        {
            if (datatype is DictionaryType dictType)
            {
                CArrowSchema* cSchema = CArrowSchema.Create();
                ExportType(dictType.ValueType, cSchema, metadata);
                return cSchema;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Adds the extension name and metadata of <paramref name="datatype"/>, if it is an
        /// <see cref="ExtensionType"/>, to <paramref name="metadata"/>. Existing keys are kept.
        /// </summary>
        private static IReadOnlyDictionary<string, string> WithExtensionMetadata(IArrowType datatype, IReadOnlyDictionary<string, string> metadata)
        {
            if (!(datatype is ExtensionType extType))
            {
                return metadata;
            }

            var merged = metadata != null
                ? new Dictionary<string, string>((IDictionary<string, string>)metadata)
                : new Dictionary<string, string>();
            if (!merged.ContainsKey(ExtensionNameKey))
                merged[ExtensionNameKey] = extType.Name;
            if (!merged.ContainsKey(ExtensionMetadataKey))
                merged[ExtensionMetadataKey] = extType.ExtensionMetadata ?? "";
            return merged;
        }

        /// <summary>
        /// Splits the extension name and metadata keys out of <paramref name="metadata"/>.
        /// </summary>
        /// <returns>The remaining metadata, or null if none remains.</returns>
        private static IReadOnlyDictionary<string, string> SplitExtensionMetadata(
            IReadOnlyDictionary<string, string> metadata, out IReadOnlyDictionary<string, string> extensionMetadata)
        {
            extensionMetadata = null;
            if (metadata == null ||
                !(metadata.ContainsKey(ExtensionNameKey) || metadata.ContainsKey(ExtensionMetadataKey)))
            {
                return metadata;
            }

            var remaining = new Dictionary<string, string>();
            var extension = new Dictionary<string, string>();
            foreach (KeyValuePair<string, string> pair in metadata)
            {
                bool isExtensionKey = pair.Key == ExtensionNameKey || pair.Key == ExtensionMetadataKey;
                (isExtensionKey ? extension : remaining)[pair.Key] = pair.Value;
            }
            extensionMetadata = extension;
            return remaining.Count > 0 ? remaining : null;
        }

        private unsafe static byte* ConstructMetadata(IReadOnlyDictionary<string, string> metadata)
        {
            if (metadata == null || metadata.Count == 0)
            {
                return null;
            }

            int size = 4;
            int[] lengths = new int[metadata.Count * 2];
            int i = 0;
            foreach (KeyValuePair<string, string> pair in metadata)
            {
                size += 8;
                lengths[i] = Encoding.UTF8.GetByteCount(pair.Key);
                size += lengths[i++];
                lengths[i] = Encoding.UTF8.GetByteCount(pair.Value);
                size += lengths[i++];
            }

            IntPtr result = Marshal.AllocHGlobal(size);
            Marshal.WriteInt32(result, metadata.Count);
            byte* ptr = (byte*)result + 4;
            i = 0;
            foreach (KeyValuePair<string, string> pair in metadata)
            {
                WriteMetadataString(ref ptr, lengths[i++], pair.Key);
                WriteMetadataString(ref ptr, lengths[i++], pair.Value);
            }

            Debug.Assert((long)(IntPtr)ptr - (long)result == size);

            return (byte*)result;
        }

        private unsafe static void WriteMetadataString(ref byte* ptr, int length, string str)
        {
            Marshal.WriteInt32((IntPtr)ptr, length);
            ptr += 4;
            fixed (char* s = str)
            {
                Encoding.UTF8.GetBytes(s, str.Length, ptr, length);
            }
            ptr += length;
        }

#if NET5_0_OR_GREATER
        [UnmanagedCallersOnly]
#endif
        private static unsafe void ReleaseCArrowSchema(CArrowSchema* schema)
        {
            if (schema == null) return;
            if (schema->release == default) return;

            Marshal.FreeHGlobal((IntPtr)schema->format);
            Marshal.FreeHGlobal((IntPtr)schema->name);
            Marshal.FreeHGlobal((IntPtr)schema->metadata);
            schema->format = null;
            schema->name = null;
            schema->metadata = null;

            if (schema->n_children > 0)
            {
                for (int i = 0; i < schema->n_children; i++)
                {
                    CArrowSchema.Free(schema->GetChild(i));
                }
                Marshal.FreeHGlobal((IntPtr)schema->children);
            }

            if (schema->dictionary != null)
            {
                CArrowSchema.Free(schema->dictionary);
            }

            schema->flags = 0;
            schema->n_children = 0;
            schema->dictionary = null;
            schema->children = null;
            schema->release = default;
        }
    }
}
