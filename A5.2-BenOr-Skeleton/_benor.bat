@set PROMPT=$G

dotnet build benor.csproj
@pause

@for /l %%X in (1, 1, 2) do @(
    dotnet .\bin\Debug\netcoreapp3.1\benor.dll < benor-inp-%%X.txt > benor-outp-%%X-test.txt
    rem type benor-outp-%%X-test.txt
    fc benor-outp-%%X.txt benor-outp-%%X-test.txt
    rem pause
)

pause
