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

namespace Apache.Arrow.Flight.IntegrationTest;

public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        var portOption = new Option<int>("--port", "-p")
        {
            Description = "Port the Flight server is listening on",
        };
        var scenarioOption = new Option<string>("--scenario", "-s")
        {
            Description = "The name of the scenario to run",
        };
        var pathOption = new Option<FileInfo>("--path", "-j")
        {
            Description = "Path to a JSON file of test data",
        };

        var clientCommand = new Command("client", "Run the Flight client")
        {
            portOption,
            scenarioOption,
            pathOption,
        };
        clientCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var command = new FlightClientCommand(
                parseResult.GetValue(portOption),
                parseResult.GetValue(scenarioOption),
                parseResult.GetValue(pathOption));
            await command.Execute().ConfigureAwait(false);
        });

        var serverCommand = new Command("server", "Run the Flight server")
        {
            scenarioOption,
        };
        serverCommand.SetAction(async (parseResult, cancellationToken) =>
        {
            var command = new FlightServerCommand(
                parseResult.GetValue(scenarioOption));
            await command.Execute().ConfigureAwait(false);
        });

        var rootCommand = new RootCommand("Integration test application for Apache.Arrow .NET Flight.")
        {
            clientCommand,
            serverCommand,
        };

        ParseResult parseResult = rootCommand.Parse(args);
        if (parseResult.Errors.Count == 0)
        {
            return await parseResult.InvokeAsync();
        }

        foreach (ParseError parseError in parseResult.Errors)
        {
            Console.Error.WriteLine(parseError.Message);
        }
        return 1;
    }
}
