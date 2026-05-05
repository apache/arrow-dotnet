# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

<#
.SYNOPSIS
    Verifies an Apache Arrow .NET release candidate.
.DESCRIPTION
    Downloads and verifies an Apache Arrow .NET release candidate,
    including GPG signature verification, checksum verification,
    and optionally building and testing source and binary distributions.
.PARAMETER Version
    The release version (e.g., 22.0.0).
.PARAMETER RC
    The release candidate number (e.g., 0).
.EXAMPLE
    .\verify_rc.ps1 22.0.0 0
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Version,

    [Parameter(Mandatory = $true, Position = 1)]
    [string]$RC
)

$ErrorActionPreference = "Stop"

# In PowerShell 7.3+, this makes native command failures respect
# $ErrorActionPreference. On older versions, we rely on Invoke-Block below.
if ($null -ne (Get-Variable PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)) {
    $PSNativeCommandUseErrorActionPreference = $true
}

function Invoke-Block {
    # Execute a script block containing native commands and throw on failure.
    param([scriptblock]$Block)
    & $Block
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $Block"
    }
}

$SourceDir = Split-Path -Parent $PSCommandPath
$TopSourceDir = Split-Path -Parent (Split-Path -Parent $SourceDir)

$ArrowDistBaseUrl = "https://dist.apache.org/repos/dist/release/arrow"
$DownloadRCBaseUrl = "https://github.com/apache/arrow-dotnet/releases/download/v${Version}-rc${RC}"
$ArchiveBaseName = "apache-arrow-dotnet-${Version}"

if (-not (Test-Path env:VERIFY_DEFAULT)) { $env:VERIFY_DEFAULT = "1" }
if (-not (Test-Path env:VERIFY_BINARY))  { $env:VERIFY_BINARY = $env:VERIFY_DEFAULT }
if (-not (Test-Path env:VERIFY_DOWNLOAD)){ $env:VERIFY_DOWNLOAD = $env:VERIFY_DEFAULT }
if (-not (Test-Path env:VERIFY_SIGN))    { $env:VERIFY_SIGN = $env:VERIFY_DEFAULT }
if (-not (Test-Path env:VERIFY_SOURCE))  { $env:VERIFY_SOURCE = $env:VERIFY_DEFAULT }

$VerifySuccess = $false

function GitHub-Actions-Group-Begin([string]$Name) {
    Write-Host "::group::$Name"
}

function GitHub-Actions-Group-End {
    Write-Host "::endgroup::"
}

function Download([string]$Url) {
    Invoke-Block { curl.exe --fail --location --remote-name --show-error --silent $Url }
}

function Download-RC-File([string]$FileName) {
    if ([int]$env:VERIFY_DOWNLOAD -gt 0) {
        Download "${DownloadRCBaseUrl}/${FileName}"
    } else {
        Copy-Item (Join-Path $TopSourceDir $FileName) -Destination $FileName
    }
}

function Import-GPG-Keys {
    if ([int]$env:VERIFY_SIGN -gt 0) {
        Download "${ArrowDistBaseUrl}/KEYS"
        Invoke-Block { gpg --import KEYS }
    }
}

function Verify-SHA([string]$Algorithm, [string]$ChecksumFile) {
    $ExpectedLine = (Get-Content $ChecksumFile -Raw).Trim()
    # Format is "hash  filename" or "hash *filename"
    $Parts = $ExpectedLine -split '\s+', 2
    $ExpectedHash = $Parts[0]
    $FileName = ($Parts[1] -replace '^\*', '').Trim()

    $ActualHash = (Get-FileHash -Algorithm $Algorithm -Path $FileName).Hash.ToLower()
    if ($ActualHash -ne $ExpectedHash.ToLower()) {
        throw "$Algorithm checksum mismatch for ${FileName}: expected ${ExpectedHash}, got ${ActualHash}"
    }
    Write-Host "${FileName}: OK (${Algorithm})"
}

function Fetch-Artifact([string]$Artifact) {
    Download-RC-File $Artifact
    if ([int]$env:VERIFY_SIGN -gt 0) {
        Download-RC-File "${Artifact}.asc"
        Invoke-Block { gpg --verify "${Artifact}.asc" $Artifact }
    }
    Download-RC-File "${Artifact}.sha256"
    Verify-SHA "SHA256" "${Artifact}.sha256"
    Download-RC-File "${Artifact}.sha512"
    Verify-SHA "SHA512" "${Artifact}.sha512"
}

function Fetch-Archive {
    Fetch-Artifact "${ArchiveBaseName}.tar.gz"
}

function Ensure-Source-Directory {
    Invoke-Block { tar xf "${ArchiveBaseName}.tar.gz" }
}

function Get-TestTarget {
    # Prefer the solution filter if available, fall back to the solution file.
    if (Test-Path "Apache.Arrow.Tests.slnf") {
        return "Apache.Arrow.Tests.slnf"
    }
    return "Apache.Arrow.sln"
}

function Test-Source-Distribution {
    if ([int]$env:VERIFY_SOURCE -le 0) {
        return
    }

    Invoke-Block { dotnet build }

    # Python 3 and PyArrow are required for C Data Interface tests.
    if (-not (Test-Path env:PYTHON)) {
        # On Windows, Python 3 installs as python.exe (no python3.exe).
        # On Linux/macOS, python3 is the standard name.
        if (Get-Command python3 -ErrorAction SilentlyContinue) {
            $env:PYTHON = "python3"
        } elseif (Get-Command python -ErrorAction SilentlyContinue) {
            $env:PYTHON = "python"
        } else {
            throw "Python is required but neither python3 nor python was found"
        }
    }
    $pyVersion = & $env:PYTHON --version 2>&1
    if ($pyVersion -notmatch 'Python 3\.') {
        throw "Python 3 is required but found: $pyVersion"
    }
    Invoke-Block { & $env:PYTHON -m pip install pyarrow find-libpython }
    $env:PYTHONNET_PYDLL = (& $env:PYTHON -m find_libpython).Trim()
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($env:PYTHONNET_PYDLL)) {
        throw "Failed to locate libpython via find_libpython"
    }

    $TestTarget = Get-TestTarget
    Invoke-Block { dotnet test $TestTarget }
}

function Reference-Package {
    param(
        [string]$PackageName,
        [string[]]$TestProjects
    )

    if (-not (Test-Path "src/${PackageName}")) {
        Write-Host "Skipping package ${PackageName} (not present in this release)"
        return
    }

    foreach ($TestProject in $TestProjects) {
        if (-not (Test-Path "test/${TestProject}")) {
            Write-Host "Skipping test project ${TestProject} (not present in this release)"
            continue
        }
        Invoke-Block { dotnet remove "test/${TestProject}" reference "src/${PackageName}/${PackageName}.csproj" }
        Invoke-Block { dotnet add "test/${TestProject}" package $PackageName --version $Version }
    }
}

function Test-Binary-Distribution {
    if ([int]$env:VERIFY_BINARY -le 0) {
        return
    }

    # Create NuGet local directory source
    New-Item -ItemType Directory -Path nuget -Force | Out-Null
    Invoke-Block { dotnet new nugetconfig }
    $NugetDir = Join-Path (Get-Location).Path "nuget"
    Invoke-Block { dotnet nuget add source -n local $NugetDir }

    Push-Location nuget
    $Packages = Get-ChildItem -Directory "../src" | Select-Object -ExpandProperty Name
    foreach ($Package in $Packages) {
        foreach ($PackageType in @("nupkg", "snupkg")) {
            Fetch-Artifact "${Package}.${Version}.${PackageType}"
        }
    }
    Pop-Location

    # Update test projects to reference NuGet packages for the release candidate
    Reference-Package "Apache.Arrow" @("Apache.Arrow.Tests", "Apache.Arrow.Compression.Tests")
    Reference-Package "Apache.Arrow.Compression" @("Apache.Arrow.Compression.Tests")
    Reference-Package "Apache.Arrow.Flight.Sql" @("Apache.Arrow.Flight.Sql.Tests", "Apache.Arrow.Flight.TestWeb")
    Reference-Package "Apache.Arrow.Flight.AspNetCore" @("Apache.Arrow.Flight.TestWeb")
    Reference-Package "Apache.Arrow.Operations" @("Apache.Arrow.Operations.Tests", "Apache.Arrow.Scalars.Tests")
    Reference-Package "Apache.Arrow.Scalars" @("Apache.Arrow.Scalars.Tests", "Apache.Arrow.Tests", "Apache.Arrow.Operations.Tests")

    # Move src directory to ensure we are only testing against built packages
    Rename-Item src src.backup

    $TestTarget = Get-TestTarget
    Invoke-Block { dotnet test $TestTarget }
}

# --- Main ---

GitHub-Actions-Group-Begin "Prepare"
GitHub-Actions-Group-End

GitHub-Actions-Group-Begin "Setup temporary directory"

if (-not (Test-Path env:VERIFY_TMPDIR)) {
    $VerifyTmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "arrow-dotnet-${Version}-${RC}-$(Get-Random)"
    $OwnsVerifyTmpDir = $true
} else {
    $VerifyTmpDir = $env:VERIFY_TMPDIR
    $OwnsVerifyTmpDir = $false
}

New-Item -ItemType Directory -Path $VerifyTmpDir -Force | Out-Null
Write-Host "Working in sandbox ${VerifyTmpDir}"
Push-Location $VerifyTmpDir

try {
    GitHub-Actions-Group-End

    GitHub-Actions-Group-Begin "Prepare source directory"
    Import-GPG-Keys
    Fetch-Archive
    Ensure-Source-Directory
    GitHub-Actions-Group-End

    Push-Location $ArchiveBaseName

    GitHub-Actions-Group-Begin "Test source distribution"
    Test-Source-Distribution
    GitHub-Actions-Group-End

    GitHub-Actions-Group-Begin "Test binary distribution"
    Test-Binary-Distribution
    GitHub-Actions-Group-End

    Pop-Location

    $VerifySuccess = $true
    Write-Host "RC looks good!"
} catch {
    Write-Host "::endgroup::"
    Write-Host "Failed to verify release candidate. See ${VerifyTmpDir} for details."
    throw
} finally {
    Pop-Location
    if ($VerifySuccess -and $OwnsVerifyTmpDir) {
        Remove-Item -Recurse -Force $VerifyTmpDir -ErrorAction SilentlyContinue
    }
}
