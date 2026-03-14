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
using Xunit;

namespace Apache.Arrow.Variant.Tests
{
    /// <summary>
    /// Cross-implementation conformance tests using binary test vectors from
    /// the apache/parquet-testing repository (test/parquet-testing/variant/).
    /// </summary>
    public class ParquetTestingVectorTests
    {
        private static readonly string VariantDir = FindVariantDir();
        private static readonly JsonSerializerOptions JsonOptions = new JsonSerializerOptions
        {
            AllowTrailingCommas = true,
        };

        private static string FindVariantDir()
        {
            // Walk up from the test assembly output directory to find the repo root,
            // then locate test/parquet-testing/variant/.
            string dir = AppContext.BaseDirectory;
            for (int i = 0; i < 10; i++)
            {
                string candidate = Path.Combine(dir, "test", "parquet-testing", "variant");
                if (Directory.Exists(candidate))
                    return candidate;
                string parent = Path.GetDirectoryName(dir);
                if (parent == null || parent == dir) break;
                dir = parent;
            }
            return null;
        }

        private static Dictionary<string, JsonElement> LoadDictionary()
        {
            string path = Path.Combine(VariantDir, "data_dictionary.json");
            string json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);
        }

        public static IEnumerable<object[]> TestCaseNames()
        {
            string variantDir = FindVariantDir();
            string dictPath = variantDir == null ? null : Path.Combine(variantDir, "data_dictionary.json");
            if (dictPath == null || !File.Exists(dictPath))
            {
                yield return new object[] { null };
                yield break;
            }

            string json = File.ReadAllText(dictPath);
            Dictionary<string, JsonElement> dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(json, JsonOptions);

            foreach (string name in dict.Keys.OrderBy(k => k))
            {
                string metadataPath = Path.Combine(variantDir, name + ".metadata");
                string valuePath = Path.Combine(variantDir, name + ".value");
                if (File.Exists(metadataPath) && File.Exists(valuePath))
                {
                    yield return new object[] { name };
                }
            }
        }

        [SkippableTheory]
        [MemberData(nameof(TestCaseNames))]
        public void DecodesTestVector(string testCaseName)
        {
            Skip.If(VariantDir == null || testCaseName == null, "parquet-testing submodule not checked out");

            Dictionary<string, JsonElement> dictionary = LoadDictionary();
            JsonElement expected = dictionary[testCaseName];

            byte[] metadata = File.ReadAllBytes(Path.Combine(VariantDir, testCaseName + ".metadata"));
            byte[] value = File.ReadAllBytes(Path.Combine(VariantDir, testCaseName + ".value"));

            VariantReader reader = new VariantReader(metadata, value);
            VariantValue actual = reader.ToVariantValue();

            AssertVariantMatchesJson(actual, expected, testCaseName);
        }

        // ---------------------------------------------------------------
        // Deep comparison: VariantValue vs JSON expected
        // ---------------------------------------------------------------

        private static void AssertVariantMatchesJson(VariantValue actual, JsonElement expected, string context)
        {
            switch (expected.ValueKind)
            {
                case JsonValueKind.Null:
                    Assert.True(actual.IsNull, $"{context}: expected null");
                    break;

                case JsonValueKind.True:
                    Assert.True(actual.IsBoolean, $"{context}: expected boolean");
                    Assert.True(actual.AsBoolean(), $"{context}: expected true");
                    break;

                case JsonValueKind.False:
                    Assert.True(actual.IsBoolean, $"{context}: expected boolean");
                    Assert.False(actual.AsBoolean(), $"{context}: expected false");
                    break;

                case JsonValueKind.String:
                    AssertStringOrTypedValue(actual, expected.GetString(), context);
                    break;

                case JsonValueKind.Number:
                    AssertNumericValue(actual, expected, context);
                    break;

                case JsonValueKind.Object:
                    Assert.True(actual.IsObject, $"{context}: expected object");
                    IReadOnlyDictionary<string, VariantValue> obj = actual.AsObject();
                    // Check that all expected fields are present
                    foreach (JsonProperty prop in expected.EnumerateObject())
                    {
                        Assert.True(obj.ContainsKey(prop.Name),
                            $"{context}: missing field '{prop.Name}'");
                        AssertVariantMatchesJson(obj[prop.Name], prop.Value,
                            $"{context}.{prop.Name}");
                    }
                    Assert.Equal(expected.EnumerateObject().Count(), obj.Count);
                    break;

                case JsonValueKind.Array:
                    Assert.True(actual.IsArray, $"{context}: expected array");
                    IReadOnlyList<VariantValue> arr = actual.AsArray();
                    List<JsonElement> expectedElements = expected.EnumerateArray().ToList();
                    Assert.Equal(expectedElements.Count, arr.Count);
                    for (int i = 0; i < expectedElements.Count; i++)
                    {
                        AssertVariantMatchesJson(arr[i], expectedElements[i],
                            $"{context}[{i}]");
                    }
                    break;

                default:
                    Assert.Fail($"{context}: unexpected JSON value kind {expected.ValueKind}");
                    break;
            }
        }

        /// <summary>
        /// JSON strings can represent actual strings or typed values like dates,
        /// timestamps, UUIDs, and binary (base64). Dispatch based on the actual
        /// variant primitive type.
        /// </summary>
        private static void AssertStringOrTypedValue(VariantValue actual, string expectedStr, string context)
        {
            VariantPrimitiveType pt = actual.PrimitiveType;

            switch (pt)
            {
                case VariantPrimitiveType.String:
                    Assert.Equal(expectedStr, actual.AsString());
                    break;

                case VariantPrimitiveType.Binary:
                    byte[] expectedBytes = Convert.FromBase64String(expectedStr);
                    Assert.Equal(expectedBytes, actual.AsBinary());
                    break;

                case VariantPrimitiveType.Uuid:
                    Guid expectedGuid = Guid.Parse(expectedStr);
                    Assert.Equal(expectedGuid, actual.AsUuid());
                    break;

                case VariantPrimitiveType.Date:
                    // "2025-04-16" → days since epoch
                    DateTime expectedDate = DateTime.Parse(expectedStr);
                    Assert.Equal(expectedDate.Date, actual.AsDate().Date);
                    break;

                case VariantPrimitiveType.Timestamp:
                    // "2025-04-16 12:34:56.78-04:00" → DateTimeOffset
                    DateTimeOffset expectedTs = DateTimeOffset.Parse(expectedStr);
                    Assert.Equal(expectedTs.UtcDateTime, actual.AsTimestamp().UtcDateTime);
                    break;

                case VariantPrimitiveType.TimestampNtz:
                    // "2025-04-16 12:34:56.78" → DateTime
                    DateTime expectedNtz = DateTime.Parse(expectedStr);
                    Assert.Equal(expectedNtz, actual.AsTimestampNtz());
                    break;

                case VariantPrimitiveType.TimeNtz:
                    // "12:33:54:123456" → microseconds since midnight
                    // The parquet-testing format uses colons: HH:MM:SS:ffffff
                    long expectedTimeMicros = ParseTimeMicros(expectedStr);
                    Assert.Equal(expectedTimeMicros, actual.AsTimeNtzMicros());
                    break;

                case VariantPrimitiveType.TimestampTzNanos:
                    // "2024-11-07T12:33:54.123456789+00:00" → nanoseconds since epoch
                    // DateTime.Parse truncates to tick precision (100ns), so we parse
                    // sub-tick nanoseconds manually from the fractional seconds.
                    Assert.Equal(ParseNanoTimestamp(expectedStr, withTz: true), actual.AsTimestampTzNanos());
                    break;

                case VariantPrimitiveType.TimestampNtzNanos:
                    // "2024-11-07T12:33:54.123456789" → nanoseconds since epoch
                    Assert.Equal(ParseNanoTimestamp(expectedStr, withTz: false), actual.AsTimestampNtzNanos());
                    break;

                default:
                    Assert.Fail($"{context}: unexpected string representation for type {pt}");
                    break;
            }
        }

        /// <summary>
        /// Parses time in the parquet-testing format "HH:MM:SS:ffffff" into microseconds since midnight.
        /// </summary>
        private static long ParseTimeMicros(string timeStr)
        {
            // Format: "12:33:54:123456" (HH:MM:SS:ffffff)
            string[] parts = timeStr.Split(':');
            long hours = long.Parse(parts[0]);
            long minutes = long.Parse(parts[1]);
            long seconds = long.Parse(parts[2]);
            long micros = long.Parse(parts[3]);
            return ((hours * 3600 + minutes * 60 + seconds) * 1_000_000) + micros;
        }

        /// <summary>
        /// Parses an ISO timestamp string with nanosecond precision into
        /// nanoseconds since Unix epoch. DateTime.Parse only preserves tick
        /// (100ns) precision, losing the final nanosecond digit.
        /// </summary>
        private static long ParseNanoTimestamp(string isoStr, bool withTz)
        {
            // Extract fractional seconds manually: find the '.' and read up to 9 digits
            int dotIndex = isoStr.IndexOf('.');
            long subSecondNanos = 0;
            string dateTimePart = isoStr;
            if (dotIndex >= 0)
            {
                // Find end of fractional part (before timezone or end of string)
                int fracEnd = dotIndex + 1;
                while (fracEnd < isoStr.Length && char.IsDigit(isoStr[fracEnd]))
                    fracEnd++;
                string fracStr = isoStr.Substring(dotIndex + 1, fracEnd - dotIndex - 1);
                // Pad or truncate to exactly 9 digits (nanoseconds)
                fracStr = fracStr.PadRight(9, '0').Substring(0, 9);
                subSecondNanos = long.Parse(fracStr);
                // Remove fractional part for DateTime.Parse to get whole seconds
                dateTimePart = isoStr.Substring(0, dotIndex) + isoStr.Substring(fracEnd);
            }

            if (withTz)
            {
                DateTimeOffset dto = DateTimeOffset.Parse(dateTimePart);
                long wholeSecondNanos = (dto.UtcDateTime.Ticks / TimeSpan.TicksPerSecond
                    - 62135596800L) * 1_000_000_000L; // .NET epoch to Unix epoch offset in seconds
                return wholeSecondNanos + subSecondNanos;
            }
            else
            {
                DateTime dt = DateTime.Parse(dateTimePart);
                long wholeSecondNanos = (dt.Ticks / TimeSpan.TicksPerSecond
                    - 62135596800L) * 1_000_000_000L;
                return wholeSecondNanos + subSecondNanos;
            }
        }

        /// <summary>
        /// Asserts a numeric variant value matches the JSON number.
        /// JSON numbers don't distinguish int sizes or decimal precision,
        /// so we dispatch on the actual variant primitive type.
        /// </summary>
        private static void AssertNumericValue(VariantValue actual, JsonElement expected, string context)
        {
            VariantPrimitiveType pt = actual.PrimitiveType;

            switch (pt)
            {
                case VariantPrimitiveType.Int8:
                    Assert.Equal((sbyte)expected.GetInt32(), actual.AsInt8());
                    break;

                case VariantPrimitiveType.Int16:
                    Assert.Equal((short)expected.GetInt32(), actual.AsInt16());
                    break;

                case VariantPrimitiveType.Int32:
                    Assert.Equal(expected.GetInt32(), actual.AsInt32());
                    break;

                case VariantPrimitiveType.Int64:
                    Assert.Equal(expected.GetInt64(), actual.AsInt64());
                    break;

                case VariantPrimitiveType.Float:
                    // Float has ~7 digits of precision; compare as floats
                    Assert.Equal((float)expected.GetDouble(), actual.AsFloat());
                    break;

                case VariantPrimitiveType.Double:
                    Assert.Equal(expected.GetDouble(), actual.AsDouble(), 4);
                    break;

                case VariantPrimitiveType.Decimal4:
                case VariantPrimitiveType.Decimal8:
                    Assert.Equal(expected.GetDecimal(), actual.AsDecimal());
                    break;

                case VariantPrimitiveType.Decimal16:
                    // JSON may lose precision for large decimals (e.g. 1.2345678912345678e+16
                    // loses the fractional part). Use relative tolerance comparison.
                    double expectedD16 = expected.GetDouble();
                    double actualD16 = (double)actual.AsDecimal();
                    double relError = Math.Abs(expectedD16 - actualD16) / Math.Max(1.0, Math.Abs(expectedD16));
                    Assert.True(relError < 1e-10,
                        $"{context}: decimal16 relative error {relError:E3} exceeds tolerance. " +
                        $"Expected ~{expectedD16}, actual ~{actualD16}");
                    break;

                default:
                    Assert.Fail($"{context}: unexpected numeric variant type {pt}");
                    break;
            }
        }
    }
}
