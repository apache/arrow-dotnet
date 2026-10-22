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

[Apache Arrow](https://arrow.apache.org/) is a universal columnar format and
multi-language toolbox for fast data interchange and in-memory analytics.
This package is part of the .NET implementation of Apache Arrow.

## Packages

- `Apache.Arrow`: the core library (arrays, schemas, record batches and the
  Arrow IPC stream and file formats).
- `Apache.Arrow.Compression`: LZ4 frame and Zstandard codecs for compressed
  Arrow IPC data.
- `Apache.Arrow.Flight`: Arrow Flight client and server.
- `Apache.Arrow.Flight.AspNetCore`: ASP.NET Core integration for Arrow Flight servers.
- `Apache.Arrow.Flight.Sql`: Arrow Flight SQL client and server.
- `Apache.Arrow.Operations`: codecs for the Variant binary format, including a
  JSON reader and writer.
- `Apache.Arrow.Scalars`: scalar types, including readers, writers and an object
  model for the Parquet Variant binary format.

## Usage

Example of reading all record batches from an Arrow IPC file:

```csharp
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow;
using Apache.Arrow.Ipc;

public static async Task<List<RecordBatch>> ReadArrowFileAsync(string filename)
{
    var batches = new List<RecordBatch>();
    using (var stream = File.OpenRead(filename))
    using (var reader = new ArrowFileReader(stream))
    {
        RecordBatch batch;
        while ((batch = await reader.ReadNextRecordBatchAsync()) != null)
        {
            batches.Add(batch);
        }
    }
    return batches;
}
```

## Links

- Documentation: https://arrow.apache.org/dotnet/
- Source code and issue tracker: https://github.com/apache/arrow-dotnet
