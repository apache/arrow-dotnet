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
using System.IO;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Scalars.Variant;
using Xunit;

namespace Apache.Arrow.Operations.Tests.Shredding
{
    /// <summary>
    /// Regression tests against the spec-invalid cases in the Iceberg corpus:
    /// (1) cases with <c>error_message</c> in <c>cases.json</c> — malformed schemas
    ///     (unsupported Arrow types) or malformed data (value/typed_value conflicts).
    /// (2) cases with <c>case-NNN-INVALID.parquet</c> — spec-invalid but whose
    ///     published "implementations may error or read" leaves behavior to the reader.
    /// </summary>
    public class ShreddedVariantErrorCaseTests
    {
        private static readonly string IpcDir = FindIpcDir();

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

        private static VariantArray LoadCase(string stem)
        {
            Skip.If(IpcDir == null, "regen.py has not been run");
            string path = Path.Combine(IpcDir, stem + ".arrow");
            Skip.IfNot(File.Exists(path), $"Missing {path}");

            using Stream stream = File.OpenRead(path);
            using ArrowFileReader reader = new ArrowFileReader(stream);
            RecordBatch batch = reader.ReadNextRecordBatch();
            return new VariantArray(batch.Column(batch.Schema.GetFieldIndex("var")));
        }

        // ===============================================================
        // Schema-level errors: unsupported Arrow types in typed_value.
        // These should fail as soon as the shredding schema is derived.
        // ===============================================================

        [SkippableFact]
        public void Case127_UnsignedInteger_RejectedAtSchemaDerivation()
        {
            // typed_value: uint32 — not a supported shredded type per spec.
            VariantArray array = LoadCase("case-127");
            ArgumentException ex = Assert.Throws<ArgumentException>(() => array.GetShredSchema());
            Assert.Contains("Unsupported shredded value type", ex.Message);
        }

        [SkippableFact]
        public void Case137_FixedLengthByteArray_NotUuid_RejectedAtSchemaDerivation()
        {
            // typed_value: fixed_size_binary[4] — only fsb(16) is valid (UUID).
            VariantArray array = LoadCase("case-137");
            ArgumentException ex = Assert.Throws<ArgumentException>(() => array.GetShredSchema());
            Assert.Contains("Unsupported shredded value type", ex.Message);
        }

        [SkippableFact]
        public void Case127_GetLogicalVariantValue_AlsoThrows()
        {
            // Any reader-facing entrypoint should surface the schema error.
            VariantArray array = LoadCase("case-127");
            Assert.Throws<ArgumentException>(() => array.GetLogicalVariantValue(0));
        }

        // ===============================================================
        // Data-level errors: both value and typed_value populated where
        // the spec forbids it (primitive and array-element slots).
        // ===============================================================

        [SkippableFact]
        public void Case42_PrimitiveSlot_ValueAndTypedValueConflict_Throws()
        {
            // Top-level row: value has residual bytes AND typed_value is an int32.
            VariantArray array = LoadCase("case-042");
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => array.GetLogicalVariantValue(0));
            Assert.Contains("both", ex.Message);
            Assert.Contains("value", ex.Message);
            Assert.Contains("typed_value", ex.Message);
        }

        [SkippableFact]
        public void Case40_ArrayElement_ValueAndTypedValueConflict_Throws()
        {
            // Array element 0 has both value and typed_value set.
            VariantArray array = LoadCase("case-040");
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => array.GetLogicalVariantValue(0));
            Assert.Contains("both", ex.Message);
            Assert.Contains("value", ex.Message);
            Assert.Contains("typed_value", ex.Message);
        }

        [SkippableFact]
        public void Case87_NonObjectResidualWithShreddedFields_Throws()
        {
            // Top-level typed_value is a shredded-object struct, but the residual
            // 'value' column holds a non-object variant (int32 = 34). Spec invalid.
            VariantArray array = LoadCase("case-087");
            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
                () => array.GetLogicalVariantValue(0));
            Assert.Contains("object", ex.Message);
        }

        [SkippableFact]
        public void Case128_NonObjectResidualWithEmptyShreddedFields_Throws()
        {
            // typed_value has all-null fields, value is a variant null (not an object).
            VariantArray array = LoadCase("case-128");
            Assert.Throws<InvalidOperationException>(() => array.GetLogicalVariantValue(0));
        }

        // ===============================================================
        // "INVALID" parquet files: spec-noncompliant but whose cases.json
        // notes say "implementations can choose to error, or read". We
        // document current behavior: we read (and the merged value may
        // differ from the Iceberg-published expected value).
        // ===============================================================

        [SkippableFact]
        public void Case043_INVALID_FieldConflict_ReadsWithoutThrowing()
        {
            // case-043-INVALID: a shredded field has typed_value=null but the
            // residual object re-declares it. We merge both, producing a result
            // that differs from Iceberg's published "typed wins" expectation.
            VariantArray array = LoadCase("case-043-INVALID");
            VariantValue v = array.GetLogicalVariantValue(0);
            Assert.True(v.IsObject,
                "Invalid-043 row is expected to materialize as an object under our permissive reader.");
        }

        [SkippableFact]
        public void Case125_INVALID_FieldConflict_ReadsWithoutThrowing()
        {
            VariantArray array = LoadCase("case-125-INVALID");
            VariantValue v = array.GetLogicalVariantValue(0);
            Assert.True(v.IsObject);
        }

        [SkippableFact]
        public void Case084_INVALID_OptionalFieldStructs_ReadsWithoutThrowing()
        {
            VariantArray array = LoadCase("case-084-INVALID");
            VariantValue v = array.GetLogicalVariantValue(0);
            Assert.True(v.IsObject);
        }
    }
}
