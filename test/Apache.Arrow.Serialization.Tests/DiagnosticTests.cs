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

using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Apache.Arrow.Serialization.Generator;
using Xunit;

namespace Apache.Arrow.Serialization.Tests;

public class DiagnosticTests
{
    private static ImmutableArray<Diagnostic> GetGeneratorDiagnostics(string source)
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(source);

        // Reference the runtime assembly for attributes
        var runtimeAssembly = typeof(ArrowSerializableAttribute).Assembly.Location;
        var references = new List<MetadataReference>
        {
            MetadataReference.CreateFromFile(typeof(object).Assembly.Location),
            MetadataReference.CreateFromFile(runtimeAssembly),
        };

        // Add System.Runtime for netcoreapp
        var runtimeDir = Path.GetDirectoryName(typeof(object).Assembly.Location)!;
        references.Add(MetadataReference.CreateFromFile(Path.Combine(runtimeDir, "System.Runtime.dll")));

        var compilation = CSharpCompilation.Create("TestAssembly",
            [syntaxTree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var generator = new ArrowSerializerGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);
        driver = driver.RunGeneratorsAndUpdateCompilation(compilation, out _, out var diagnostics);

        return diagnostics;
    }

    private static bool HasDiagnostic(ImmutableArray<Diagnostic> diagnostics, string id)
    {
        foreach (var d in diagnostics)
            if (d.Id == id) return true;
        return false;
    }

    [Fact]
    public void ARROW001_NonPartialType()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public struct NotPartial
            {
                public int X { get; set; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW001"),
            "Expected ARROW001 for non-partial type");
    }

    [Fact]
    public void ARROW002_ReadonlyFieldNoConstructor()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial struct BadStruct
            {
                public readonly int X;
                public readonly int Y;
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW002"),
            "Expected ARROW002 for readonly fields without matching constructor");
    }

    [Fact]
    public void ARROW002_NotFired_WhenConstructorExists()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial struct GoodStruct
            {
                public readonly int X;
                public readonly int Y;

                public GoodStruct(int x, int y) { X = x; Y = y; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.False(HasDiagnostic(diagnostics, "ARROW002"),
            "ARROW002 should not fire when a matching constructor exists");
    }

    [Fact]
    public void ARROW003_UnsupportedType()
    {
        var source = """
            using System.Collections.Generic;
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record WithQueue
            {
                public Queue<int> Items { get; init; } = new();
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW003"),
            "Expected ARROW003 for unsupported Queue<int> type");
    }

    [Fact]
    public void ARROW004_DuplicateFieldNames()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record DuplicateFields
            {
                [ArrowField("same")]
                public int A { get; init; }
                [ArrowField("same")]
                public int B { get; init; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW004"),
            "Expected ARROW004 for duplicate Arrow field names");
    }

    [Fact]
    public void ARROW005_GetOnlyProperty()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial class GetOnlyProp
            {
                public int X { get; }
                public string Name { get; init; } = "";
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW005"),
            "Expected ARROW005 for get-only property without init");
    }

    [Fact]
    public void NoDiagnostics_ForValidType()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record ValidType
            {
                public int X { get; init; }
                public string Name { get; init; } = "";
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        var arrowDiags = diagnostics.Where(d => d.Id.StartsWith("ARROW")).ToList();
        Assert.Empty(arrowDiags);
    }

    [Fact]
    public void ARROW006_ArrowAttributeOnPrivateProperty()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record WithPrivate
            {
                public int X { get; init; }
                [ArrowField("secret")]
                private int Y { get; init; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW006"),
            "Expected ARROW006 for Arrow attribute on private property");
    }

    [Fact]
    public void ARROW006_ArrowAttributeOnStaticProperty()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record WithStatic
            {
                public int X { get; init; }
                [ArrowField("shared")]
                public static int Y { get; set; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.True(HasDiagnostic(diagnostics, "ARROW006"),
            "Expected ARROW006 for Arrow attribute on static property");
    }

    [Fact]
    public void ARROW006_NotFired_ForPrivateWithoutAttributes()
    {
        var source = """
            using Apache.Arrow.Serialization;

            [ArrowSerializable]
            public partial record WithPrivateNoAttr
            {
                public int X { get; init; }
                private int Y { get; init; }
            }
            """;

        var diagnostics = GetGeneratorDiagnostics(source);
        Assert.False(HasDiagnostic(diagnostics, "ARROW006"),
            "ARROW006 should not fire for private member without Arrow attributes");
    }
}
