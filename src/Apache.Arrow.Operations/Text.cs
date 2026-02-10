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


using Apache.Arrow.Types;

namespace Apache.Arrow.Operations;

/// <summary>
/// Pretty printing utilities
/// </summary>
public static class Format
{
    /// <summary>
    /// Recursively pretty print format and write `array` into `stream`, indenting as nesting increases.
    /// </summary>
    /// <param name="array"></param>
    /// <param name="stream"></param>
    /// <param name="indent"></param>
    /// <param name="indenter"></param>
    /// <exception cref="NotImplementedException"></exception>
    public static void PrettyPrintFormat(IArrowArray array, StreamWriter stream, int indent = 0, string indenter = "    ")
    {

        List<string> indenting = Enumerable.Repeat(indenter, indent).ToList();
        string indentString = string.Concat(indenting);

        stream.WriteLine($"{indentString}[");
        var pad = indentString + indenter;
        switch (array.Data.DataType.TypeId)
        {
            case ArrowTypeId.Float:
                {
                    var valArray = (FloatArray)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Double:
                {
                    var valArray = (DoubleArray)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Int32:
                {
                    var valArray = (Int32Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Int64:
                {
                    var valArray = (Int64Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Int16:
                {
                    var valArray = (Int16Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Int8:
                {
                    var valArray = (Int8Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.UInt8:
                {
                    var valArray = (UInt8Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.UInt16:
                {
                    var valArray = (UInt16Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.UInt32:
                {
                    var valArray = (UInt32Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.UInt64:
                {
                    var valArray = (UInt64Array)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.Boolean:
                {
                    var valArray = (BooleanArray)array;

                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.HalfFloat:
                {
                    var valArray = (HalfFloatArray)array;
                    foreach (var v in valArray)
                    {
                        stream.WriteLine($"{pad}{v}");
                    }
                    break;
                }
            case ArrowTypeId.List:
                {
                    var valArray = (ListArray)array;
                    for (var i = 0; i < valArray.Length; i++)
                    {
                        if (valArray.IsNull(i))
                        {
                            stream.WriteLine($"{pad}{null}");
                        }
                        else
                        {
                            var slc = valArray.GetSlicedValues(i);
                            PrettyPrintFormat(slc, stream, indent + 1, indenter);
                        }
                    }
                    break;
                }
            case ArrowTypeId.String:
                {
                    var valArray = (StringArray)array;
                    for (var i = 0; i < valArray.Length; i++)
                    {
                        if (valArray.IsNull(i))
                        {
                            stream.WriteLine($"{pad}{null}");
                        }
                        else
                        {
                            var slc = valArray.GetString(i);
                            stream.WriteLine($"{pad}{slc}");
                        }
                    }
                    break;
                }
            case ArrowTypeId.Struct:
                {
                    var dtype = (StructType)array.Data.DataType;
                    var valArray = (StructArray)array;
                    foreach (var (f, col) in dtype.Fields.Zip(valArray.Fields))
                    {
                        stream.WriteLine($"{indentString}{f.Name}: {f.DataType.Name}");
                        PrettyPrintFormat(col, stream, indent + 1, indenter);
                    }
                    break;
                }
            default: throw new NotImplementedException($"{array.Data.DataType.Name}");
        }
        stream.WriteLine($"{indentString}]");
    }

    /// <summary>
    /// Recursively pretty print format and write `array` into a string, indenting as nesting increases.
    /// </summary>
    /// <param name="array"></param>
    /// <param name="indent"></param>
    /// <param name="indenter"></param>
    /// <returns></returns>
    public static string PrettyPrintFormat(IArrowArray array, int indent = 0, string indenter = "    ")
    {
        using (var bufferStream = new MemoryStream())
        {
            var writer = new StreamWriter(bufferStream);
            PrettyPrintFormat(array, writer, indent, indenter);
            writer.Flush();
            bufferStream.Seek(0, SeekOrigin.Begin);
            var reader = new StreamReader(bufferStream);
            var buff = reader.ReadToEnd();
            return buff;
        }
    }

    /// <summary>
    /// Pretty print `array` to `STDOUT` via `Console.WriteLine`. Prefer `PrettyPrintFormat` to control where the
    /// writing happens.
    /// </summary>
    /// <param name="array"></param>
    /// <param name="indent"></param>
    /// <param name="indenter"></param>
    public static void PrettyPrint(IArrowArray array, int indent = 0, string indenter = "    ")
    {
        var text = PrettyPrintFormat(array, indent, indenter);
        Console.WriteLine(text);
    }
}