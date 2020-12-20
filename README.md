azure-kusto-orc-conv
=====================

Utility that converts ORC files to JSON format.

The source code is based on [apache-orc](https://github.com/apache/orc).

# Building

    mkdir build
    cd build
    cmake -G "Visual Studio 16 2019" -A x64 -DBUILD_JAVA=Off -DANALYZE_JAVA=Off ..\src
    msbuild ORC.sln /p:Configuration=Release /p:Platform=x64
    
# Packaging

Copy all the artifacts into `bin` directory:

    mkdir bin
    xcopy /Q /E /I build\tzdata_ep-prefix\src\tzdata_ep\share\zoneinfo bin\zoneinfo
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
