param (
    [Parameter(Mandatory = $true)]
    [string]$Runner
)

$global:ProtoCompilerVersion = "27.2"

function Get-Link
{
    $baseUrl = "https://github.com/protocolbuffers/protobuf/releases/download/v$global:ProtoCompilerVersion/protoc"
    switch ($Runner)
    {
        "ubuntu-latest"  {
            return "$baseUrl-$global:ProtoCompilerVersion-linux-x86_64.zip"
        }

        "windows-latest" {
            return "$baseUrl-$global:ProtoCompilerVersion-win64.zip"
        }

        "macos-latest" {
            return "$baseUrl-$global:ProtoCompilerVersion-osx-universal_binary.zip"
        }

        default {
            throw "unsupported runner '{$Runner}'"
        }
    }
}

$link = Get-Link
$filename = "protobuf-compiler"
Invoke-WebRequest -Uri $link -OutFile "$filename.zip"
[System.IO.Compression.ZipFile]::ExtractToDirectory("$filename.zip", $filename)

$protobufBinDir = (Get-Item -Path "protobuf-compiler/bin").FullName

"protoc_bin=$protobufBinDir" | Out-File -FilePath $env:GITHUB_OUTPUT -Append