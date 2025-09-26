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
using System.CommandLine;
using System.CommandLine.Parsing;
using System.IO;
using System.Threading.Tasks;

namespace Apache.Arrow.IntegrationTest
{
    public class Program
    {
        public static async Task<int> Main(string[] args)
        {
            var modeOption = new Option<string>("--mode")
            {
                Description = "Which command to run",
            };
            var jsonFileOption = new Option<FileInfo>("--json-file", "-j")
            {
                Description = "The JSON file to interact with",
            };
            var arrowFileOption = new Option<FileInfo>("--arrow-file", "-a")
            {
                Description = "The arrow file to interact with",
            };

            var integrationTestCommand = new RootCommand("Integration test app for Apache.Arrow .NET Library.")
            {
                modeOption, jsonFileOption, arrowFileOption
            };
            
            ParseResult parseResult = integrationTestCommand.Parse(args);
            if (parseResult.Errors.Count == 0)
            {
                var integrationCommand = new IntegrationCommand(
                    parseResult.GetValue(modeOption),
                    parseResult.GetValue(jsonFileOption),
                    parseResult.GetValue(arrowFileOption));
                return await integrationCommand.Execute();
            }

            foreach (ParseError parseError in parseResult.Errors)
            {
                Console.Error.WriteLine(parseError.Message);
            }
            return 1;
        }
    }
}
