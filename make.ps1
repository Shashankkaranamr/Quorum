<#
.SYNOPSIS
    Quorum build tasks for Windows.

.DESCRIPTION
    A mirror of the Makefile, because make is not installed by default on
    Windows and this project is primarily developed there. Every target in the
    Makefile exists here under the same name, and test/tooling/targets_test.go
    fails the build if either file grows a target the other does not have.

    Everything here needs only the Go toolchain. Protobuf codegen uses buf, a
    pure-Go compiler installed by `.\make.ps1 tools`; there is no native protoc
    dependency.

.EXAMPLE
    .\make.ps1 build
    .\make.ps1 ci
    .\make.ps1 run -Config cluster-5.yaml
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [string]$Target = 'help',

    [string]$Config = 'cluster.yaml',
    [string]$Bin = 'bin'
)

$ErrorActionPreference = 'Stop'

# Target -> description. This table is the source of truth for `help`, and it is
# what targets_test.go compares against the Makefile.
$Targets = [ordered]@{
    'help'        = 'Show available targets'
    'tools'       = 'Install pinned dev tools (buf, protoc plugins, golangci-lint) into GOPATH/bin'
    'proto'       = 'Regenerate Go code from the .proto files'
    'proto-check' = 'Fail if checked-in generated code is stale'
    'build'       = 'Build every binary into bin/'
    'test'        = 'Run the test suite'
    'race'        = 'Run the test suite under the race detector'
    'cover'       = 'Run tests and report coverage per package'
    'vet'         = 'Run go vet'
    'lint'        = 'Run go vet and golangci-lint'
    'fmt'         = 'Format all Go source'
    'fmt-check'   = 'Fail if any Go source is unformatted'
    'ci'          = 'Everything CI would run'
    'run'         = "Show the cluster plan and one node's configuration"
    'up'          = 'Start every node in the config as a separate process'
    'down'        = 'Stop every node started by up'
    'clean'       = 'Remove build output, coverage data and runtime cluster state'
}

$BufVersion = 'latest'
$GolangciLintVersion = 'latest'
$ProtocGenGoVersion = 'latest'
$ProtocGenGrpcVersion = 'latest'

function Get-Version {
    try {
        $v = git describe --tags --always --dirty 2>$null
        if ($LASTEXITCODE -eq 0 -and $v) { return $v.Trim() }
    } catch { }
    return 'dev'
}

function Get-GoPathBin {
    return (Join-Path (& go env GOPATH) 'bin')
}

# Invoke a native command and fail the script if it returns non-zero.
# PowerShell does not do this on its own, so a broken step would otherwise be
# reported as success -- which is exactly the failure mode a CI target must not
# have.
function Invoke-Checked {
    param([Parameter(Mandatory)][scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "command failed with exit code ${LASTEXITCODE}: $Command"
    }
}

function Invoke-WithGoPathBin {
    param([Parameter(Mandatory)][scriptblock]$Command)
    $saved = $env:Path
    try {
        $env:Path = (Get-GoPathBin) + [IO.Path]::PathSeparator + $env:Path
        Invoke-Checked $Command
    } finally {
        $env:Path = $saved
    }
}

function Target-Help {
    Write-Host ("Quorum {0}`n" -f (Get-Version))
    foreach ($name in $Targets.Keys) {
        Write-Host ("  {0,-12} {1}" -f $name, $Targets[$name])
    }
}

function Target-Tools {
    Invoke-Checked { go install "github.com/bufbuild/buf/cmd/buf@$BufVersion" }
    Invoke-Checked { go install "google.golang.org/protobuf/cmd/protoc-gen-go@$ProtocGenGoVersion" }
    Invoke-Checked { go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$ProtocGenGrpcVersion" }
    Invoke-Checked { go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GolangciLintVersion" }
}

function Target-Proto {
    Invoke-WithGoPathBin { buf generate }
}

function Target-ProtoCheck {
    Target-Proto
    & git diff --exit-code -- gen
    if ($LASTEXITCODE -ne 0) {
        throw "gen/ is out of date: run '.\make.ps1 proto' and commit the result"
    }
}

function Target-Build {
    $ldflags = "-X main.version=$(Get-Version)"
    Invoke-Checked { go build -ldflags $ldflags -o "$Bin/" ./cmd/... }
}

function Target-Test { Invoke-Checked { go test ./... } }

# The race detector needs cgo, which needs a 64-bit C compiler.
#
# Windows machines frequently have an old 32-bit MinGW earlier on PATH, and Go
# then fails with "cc1.exe: sorry, unimplemented: 64-bit mode not compiled in",
# which reads like a Go problem and is not. Finding a usable compiler here is
# better than asking a developer to reorder their system PATH, and far better
# than quietly skipping the race detector -- this project's concurrency claims
# rest on it.
function Resolve-RaceCC {
    $candidates = @()
    if ($env:CC) { $candidates += $env:CC }
    $found = Get-Command gcc, x86_64-w64-mingw32-gcc, clang -All -ErrorAction SilentlyContinue
    if ($found) { $candidates += $found.Source }

    foreach ($cc in ($candidates | Where-Object { $_ } | Select-Object -Unique)) {
        try {
            $target = & $cc -dumpmachine 2>$null
            if ($LASTEXITCODE -eq 0 -and $target -match '^(x86_64|aarch64)') { return $cc }
        } catch { }
    }
    return $null
}

function Target-Race {
    if ($IsWindows -or $env:OS -eq 'Windows_NT') {
        $cc = Resolve-RaceCC
        if (-not $cc) {
            throw @'
go test -race needs cgo and a 64-bit C compiler, and none was found on PATH.

Install one:
    winget install BrechtSanders.WinLibs.POSIX.UCRT

A 32-bit MinGW will not do: it fails with
"sorry, unimplemented: 64-bit mode not compiled in".
'@
        }
        $env:CC = $cc
    }
    Invoke-Checked { go test -race ./... }
}

function Target-Cover {
    Invoke-Checked { go test -coverprofile=coverage.out ./... }
    Invoke-Checked { go tool cover -func=coverage.out }
}

function Target-Vet { Invoke-Checked { go vet ./... } }

function Target-Lint {
    Target-Vet
    Invoke-WithGoPathBin { golangci-lint run }
}

function Target-Fmt { Invoke-Checked { go fmt ./... } }

function Target-FmtCheck {
    $unformatted = & gofmt -l . | Where-Object { $_ -and ($_ -notlike 'gen*') }
    if ($unformatted) {
        Write-Host 'unformatted files:'
        $unformatted | ForEach-Object { Write-Host "  $_" }
        throw 'run .\make.ps1 fmt'
    }
}

function Target-Ci {
    Target-FmtCheck
    Target-Lint
    Target-Build
    Target-Test
    Target-Race
    Write-Host 'ci: ok'
}

function Target-Run {
    Target-Build
    Invoke-Checked { & "./$Bin/quorumctl" plan -config $Config }
    Write-Host ''
    Invoke-Checked { & "./$Bin/quorum-node" -id 1 -config $Config }
}

function Target-Up {
    Target-Build
    Invoke-Checked { & "./$Bin/quorumctl" up -config $Config }
}

function Target-Down {
    Target-Build
    Invoke-Checked { & "./$Bin/quorumctl" down }
}

function Target-Clean {
    foreach ($path in @($Bin, 'coverage.out', 'data')) {
        if (Test-Path $path) { Remove-Item -Recurse -Force $path }
    }
}

if (-not $Targets.Contains($Target)) {
    Write-Host "unknown target '$Target'`n"
    Target-Help
    exit 1
}

switch ($Target) {
    'help' { Target-Help }
    'tools' { Target-Tools }
    'proto' { Target-Proto }
    'proto-check' { Target-ProtoCheck }
    'build' { Target-Build }
    'test' { Target-Test }
    'race' { Target-Race }
    'cover' { Target-Cover }
    'vet' { Target-Vet }
    'lint' { Target-Lint }
    'fmt' { Target-Fmt }
    'fmt-check' { Target-FmtCheck }
    'ci' { Target-Ci }
    'run' { Target-Run }
    'up' { Target-Up }
    'down' { Target-Down }
    'clean' { Target-Clean }
}
