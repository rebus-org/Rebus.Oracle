@echo off

set scriptsdir=%~dp0
set root=%scriptsdir%\..
set deploydir=%root%\deploy
set version=%2

if "%version%"=="" (
	echo Please invoke the build script with a version as its second argument.
	echo.
	goto exit_fail
)

set Version=%version%

if exist "%deploydir%" (
	rd "%deploydir%" /s/q
)

pushd %root%

dotnet restore
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

dotnet pack "%root%/Rebus.Oracle" -c Release -o "%deploydir%" /p:PackageVersion=%version%
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

dotnet pack "%root%/Rebus.Oracle.Devart" -c Release -o "%deploydir%" /p:PackageVersion=%version%
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

call scripts\push.cmd "%version%"

popd






goto exit_success
:exit_fail
exit /b 1
:exit_success