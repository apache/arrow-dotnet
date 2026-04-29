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
using System.IO;
using System.Linq;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Conformance tests against the Iceberg-derived shredded variant test corpus
    /// from apache/parquet-testing. For each case, the sibling <c>.arrow</c> IPC
    /// file (produced by <c>test/shredded_variant_ipc/regen.py</c>) is loaded,
    /// the <c>var</c> column is projected as a <see cref="VariantArray"/>, and
    /// each row's materialization is compared against the expected
    /// <c>*.variant.bin</c> payload.
    /// </summary>
    public class ShreddedVariantConformanceTests
    {
        private static readonly string IpcDir = FindIpcDir();
        private static readonly string ShreddedVariantDir = FindShreddedVariantDir();

        private static string FindIpcDir()
        {
            string dir = AppContext.BaseDirectory;
            for (int i = 0; i < 10; i++)
            {
                string candidate = Path.Combine(dir, "test", "shredded_variant_ipc");
                if (Directory.Exists(candidate) && Directory.GetFiles(candidate, "*.arrow").Length > 0)
                    return candidate;
                string parent = Path.GetDirectoryName(dir);
                if (parent == null || parent == dir) break;
                dir = parent;
            }
            return null;
        }

        private static string FindShreddedVariantDir()
        {
            string dir = AppContext.BaseDirectory;
            for (int i = 0; i < 10; i++)
            {
                string candidate = Path.Combine(dir, "test", "parquet-testing", "shredded_variant");
                if (File.Exists(Path.Combine(candidate, "cases.json")))
                    return candidate;
                string parent = Path.GetDirectoryName(dir);
                if (parent == null || parent == dir) break;
                dir = parent;
            }
            return null;
        }

        public static IEnumerable<object[]> SingleRecordCases()
        {
            string shreddedDir = FindShreddedVariantDir();
            if (shreddedDir == null)
            {
                yield return new object[] { 0, null, null, null };
                yield break;
            }

            string casesPath = Path.Combine(shreddedDir, "cases.json");
            using JsonDocument doc = JsonDocument.Parse(File.ReadAllText(casesPath));
            foreach (JsonElement c in doc.RootElement.EnumerateArray())
            {
                if (!c.TryGetProperty("variant_file", out JsonElement vf)) continue;
                if (!c.TryGetProperty("parquet_file", out JsonElement pf)) continue;
                // Skip spec-INVALID cases here — the cases.json notes explicitly say
                // "implementations can choose to error, or read the shredded value".
                // They're covered by separate deliberate tests.
                if (pf.GetString().Contains("INVALID")) continue;
                int caseNumber = c.GetProperty("case_number").GetInt32();
                string testName = c.TryGetProperty("test", out JsonElement t) ? t.GetString() : "";
                yield return new object[] { caseNumber, testName, pf.GetString(), vf.GetString() };
            }
        }

        public static IEnumerable<object[]> MultiRecordCases()
        {
            string shreddedDir = FindShreddedVariantDir();
            if (shreddedDir == null)
            {
                yield return new object[] { 0, null, null };
                yield break;
            }

            string casesPath = Path.Combine(shreddedDir, "cases.json");
            using JsonDocument doc = JsonDocument.Parse(File.ReadAllText(casesPath));
            foreach (JsonElement c in doc.RootElement.EnumerateArray())
            {
                if (!c.TryGetProperty("variant_files", out _)) continue;
                if (!c.TryGetProperty("parquet_file", out JsonElement pf)) continue;
                int caseNumber = c.GetProperty("case_number").GetInt32();
                string testName = c.TryGetProperty("test", out JsonElement t) ? t.GetString() : "";
                yield return new object[] { caseNumber, testName, pf.GetString() };
            }
        }

        [SkippableTheory]
        [MemberData(nameof(SingleRecordCases))]
        public void SingleRecord(int caseNumber, string testName, string parquetFile, string variantFile)
        {
            Skip.If(ShreddedVariantDir == null, "parquet-testing submodule not checked out");
            Skip.If(IpcDir == null, "regen.py has not been run (test/shredded_variant_ipc/*.arrow missing)");

            string stem = Path.GetFileNameWithoutExtension(parquetFile);
            string ipcPath = Path.Combine(IpcDir, stem + ".arrow");
            string variantBinPath = Path.Combine(ShreddedVariantDir, variantFile);
            Skip.IfNot(File.Exists(ipcPath), $"Missing {ipcPath} (case {caseNumber}: {testName})");

            VariantArray variantArray = LoadVariantArray(ipcPath);
            Assert.True(variantArray.Length >= 1, $"Expected at least 1 row (case {caseNumber}: {testName})");

            VariantValue actual = variantArray.GetLogicalVariantValue(0);
            VariantValue expected = LoadExpectedVariant(variantBinPath);

            Assert.Equal(expected, actual);
        }

        [SkippableTheory]
        [MemberData(nameof(MultiRecordCases))]
        public void MultiRecord(int caseNumber, string testName, string parquetFile)
        {
            Skip.If(ShreddedVariantDir == null, "parquet-testing submodule not checked out");
            Skip.If(IpcDir == null, "regen.py has not been run");

            string stem = Path.GetFileNameWithoutExtension(parquetFile);
            string ipcPath = Path.Combine(IpcDir, stem + ".arrow");
            Skip.IfNot(File.Exists(ipcPath), $"Missing {ipcPath} (case {caseNumber}: {testName})");

            VariantArray variantArray = LoadVariantArray(ipcPath);

            // Load the list of expected files from cases.json.
            string casesPath = Path.Combine(ShreddedVariantDir, "cases.json");
            using JsonDocument doc = JsonDocument.Parse(File.ReadAllText(casesPath));
            JsonElement caseElement = doc.RootElement.EnumerateArray()
                .First(c => c.GetProperty("case_number").GetInt32() == caseNumber);
            JsonElement variantFiles = caseElement.GetProperty("variant_files");

            Assert.Equal(variantFiles.GetArrayLength(), variantArray.Length);

            for (int i = 0; i < variantArray.Length; i++)
            {
                JsonElement vf = variantFiles[i];
                if (vf.ValueKind == JsonValueKind.Null)
                {
                    Assert.True(variantArray.IsNull(i), $"Case {caseNumber} ({testName}) row {i} expected struct-level null");
                    continue;
                }

                Assert.False(variantArray.IsNull(i), $"Case {caseNumber} ({testName}) row {i} unexpectedly null");
                string binPath = Path.Combine(ShreddedVariantDir, vf.GetString());
                VariantValue expected = LoadExpectedVariant(binPath);
                VariantValue actual = variantArray.GetLogicalVariantValue(i);
                Assert.Equal(expected, actual);
            }
        }

        // ---------------------------------------------------------------
        // IPC loading helpers
        // ---------------------------------------------------------------

        private static VariantArray LoadVariantArray(string ipcPath)
        {
            using Stream stream = File.OpenRead(ipcPath);
            using ArrowFileReader reader = new ArrowFileReader(stream);
            RecordBatch batch = reader.ReadNextRecordBatch();
            if (batch == null)
            {
                throw new InvalidOperationException($"No record batches in {ipcPath}");
            }

            int varIdx = batch.Schema.GetFieldIndex("var");
            Assert.True(varIdx >= 0, "IPC schema missing 'var' column");

            IArrowArray varArray = batch.Column(varIdx);
            return new VariantArray(varArray);
        }

        // ---------------------------------------------------------------
        // Expected-variant loading: decode the .variant.bin format
        //   = concatenated metadata bytes | value bytes
        // ---------------------------------------------------------------

        private static VariantValue LoadExpectedVariant(string variantBinPath)
        {
            byte[] bytes = File.ReadAllBytes(variantBinPath);
            int metadataLength = ComputeMetadataLength(bytes);
            ReadOnlySpan<byte> metadata = new ReadOnlySpan<byte>(bytes, 0, metadataLength);
            ReadOnlySpan<byte> value = new ReadOnlySpan<byte>(bytes, metadataLength, bytes.Length - metadataLength);
            VariantReader reader = new VariantReader(metadata, value);
            return reader.ToVariantValue();
        }

        private static int ComputeMetadataLength(byte[] bytes)
        {
            byte header = bytes[0];
            int offsetSize = ((header >> 6) & 0x3) + 1;
            int dictSize = ReadLittleEndianInt(bytes, 1, offsetSize);
            int offsetsStart = 1 + offsetSize;
            int stringsStart = offsetsStart + (dictSize + 1) * offsetSize;
            int lastOffset = ReadLittleEndianInt(bytes, offsetsStart + dictSize * offsetSize, offsetSize);
            return stringsStart + lastOffset;
        }

        private static int ReadLittleEndianInt(byte[] buf, int pos, int size)
        {
            int result = 0;
            for (int i = 0; i < size; i++)
            {
                result |= buf[pos + i] << (8 * i);
            }
            return result;
        }
    }
}
