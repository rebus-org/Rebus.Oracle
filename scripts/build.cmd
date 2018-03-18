@echo off

set scriptsdir=%~dp0
set root=%scriptsdir%\..
set version=%2

if "%version%"=="" (
	echo Please invoke the build script with a version as its second argument.
	echo.
	goto exit_fail
)

set Version=%version%

pushd %root%

dotnet restore
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

dotnet build "%root%\Rebus.Oracle" -c Release
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

dotnet build "%root%\Rebus.Oracle.Devart" -c Release
if %ERRORLEVEL% neq 0 (
	popd
 	goto exit_fail
)

popd



goto exit_success
:exit_fail
exit /b 1
:exit_success