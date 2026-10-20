azure-kusto-orc-conv
=====================

Utility that converts ORC files to JSON format.

The source code is based on [apache-orc](https://github.com/apache/orc).

# Reader compatibility

Following [Apache ORC-2187](https://github.com/apache/orc/pull/2661), the
C++ reader explicitly limits each streamed protobuf metadata message to
1 GiB (1,073,741,824 decompressed parser bytes). This covers file and stripe
footers, metadata, row indexes, and bloom-filter indexes. Previously accepted
larger messages may now be rejected with a parse error. This is an intentional
compatibility-affecting cap, not a memory-allocation ceiling: protobuf objects,
buffers, and other reader allocations are not bounded by this value.

The postscript (already limited by its one-byte length) and the optional
caller-supplied serialized file-tail API retain their existing parsing behavior.
No schema or public API version changes are introduced.

# Building

    mkdir build
    cd build
    cmake -G "Visual Studio 17 2022" -A x64 -DBUILD_JAVA=Off -DANALYZE_JAVA=Off ..\src
    msbuild ORC.sln /p:Configuration=Release /p:Platform=x64

Windows builds use the Cygwin time-zone archive committed under
`third_party/tzdata` and verify its SHA-256 before extraction. The build does
not download tzdata, including in the isolated OneBranch release environment.
Hermetic callers can still override the default with another local copy:

    cmake -G "Visual Studio 17 2022" -A x64 -DBUILD_JAVA=Off -DANALYZE_JAVA=Off -DTZDATA_ARCHIVE=C:\artifacts\tzdata-2026e-1-noarch.tar.xz ..\src

`third_party/tzdata/tzdata.cmake` records the Cygwin package version, canonical
package path and metadata source, archive name, upstream SHA-512, and build
SHA-256. GitHub CI checks the current package in Cygwin's machine-readable
`setup.ini`; a network outage is reported separately from a stale archive.

To discover, verify, and vendor a newer package, create a dedicated branch,
commit and push it with tracking, and open a draft PR, run exactly this command
from a clean non-`master` checkout:

    python scripts/manage_tzdata.py update

The updater is operator-driven and refuses dirty worktrees, `master`, missing
GitHub authentication, conflicting branches, and unchanged packages. CI only
checks freshness and never pushes changes.

# Packaging

Copy all the artifacts into `bin` directory:

    mkdir bin
    xcopy /Q /E /I build\tzdata_ep-prefix\src\tzdata_ep\usr\share\zoneinfo bin\zoneinfo
    xcopy /Q build\tools\src\Release\*.exe bin\

Modify `Package.nuspec`, then run:

    nuget pack

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
