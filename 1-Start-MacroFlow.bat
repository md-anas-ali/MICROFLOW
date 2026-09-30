@echo off
setlocal EnableExtensions
title MacroFlow
cd /d "%~dp0"

rem Optional argument /nobrowser is used by 2-Setup-Environment.bat
set "MF_OPEN=1"
if /I "%~1"=="/nobrowser" set "MF_OPEN=0"

where go >nul 2>&1
if errorlevel 1 goto :nogo

rem ---- Load the values saved by 2-Setup-Environment.bat (Windows user environment)
for %%V in (DATABASE_URL MICROFLOW_MASTER_KEY MICROFLOW_LOGIN_USER MICROFLOW_LOGIN_PASSWORD) do call :loadvar %%V

if not defined DATABASE_URL goto :nosetup
if not defined MICROFLOW_MASTER_KEY goto :nosetup

rem ---- Local PC defaults (only used when not already set)
if not defined MICROFLOW_ADDR set "MICROFLOW_ADDR=127.0.0.1:8080"
if not defined MICROFLOW_SCHEDULER_TIMEZONE set "MICROFLOW_SCHEDULER_TIMEZONE=Asia/Dhaka"
if not defined MICROFLOW_SCRATCH_DIR set "MICROFLOW_SCRATCH_DIR=%TEMP%\microflow"
if not exist "%~d0\tmp" mkdir "%~d0\tmp" >nul 2>&1
set "CGO_ENABLED=0"

rem ---- Tool paths (the server defaults are Linux paths)
if not defined MICROFLOW_FFMPEG_PATH for /f "delims=" %%P in ('where ffmpeg 2^>nul') do if not defined MICROFLOW_FFMPEG_PATH set "MICROFLOW_FFMPEG_PATH=%%P"
if not defined MICROFLOW_PYTHON_PATH for /f "delims=" %%P in ('where python 2^>nul') do if not defined MICROFLOW_PYTHON_PATH set "MICROFLOW_PYTHON_PATH=%%P"
if not defined MICROFLOW_EDGE_TTS_PATH for /f "delims=" %%P in ('where edge-tts 2^>nul') do if not defined MICROFLOW_EDGE_TTS_PATH set "MICROFLOW_EDGE_TTS_PATH=%%P"
if not defined MICROFLOW_FFMPEG_PATH echo WARNING: ffmpeg was not found on PATH - video nodes will fail until it is installed.
if not defined MICROFLOW_PYTHON_PATH echo WARNING: python was not found on PATH - command nodes will fail until it is installed.
if not defined MICROFLOW_EDGE_TTS_PATH echo WARNING: edge-tts was not found on PATH - run: pip install edge-tts

rem ---- Server address -> MF_URL
for /f "usebackq delims=" %%U in (`powershell -NoProfile -Command "$a=$env:MICROFLOW_ADDR; $h=($a -split ':')[0]; $p=($a -split ':')[-1]; if(-not $h -or $h -eq '0.0.0.0'){$h='127.0.0.1'}; 'http://'+$h+':'+$p"`) do set "MF_URL=%%U"

rem ---- Already running? Then do not start a duplicate.
powershell -NoProfile -Command "try{ if((Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 -Uri ($env:MF_URL+'/healthz')).Content -eq 'ok'){exit 0}else{exit 1} }catch{exit 1}"
if not errorlevel 1 goto :running

if "%MF_OPEN%"=="1" start "" /min powershell -NoProfile -WindowStyle Hidden -Command "for($i=0;$i -lt 180;$i++){ try{ if((Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri ($env:MF_URL+'/healthz')).Content -eq 'ok'){Start-Process $env:MF_URL; break} }catch{}; Start-Sleep 1 }"

echo Starting MacroFlow at %MF_URL%
echo The browser opens automatically once the server is ready.
echo Keep this window open while using MacroFlow. Close it to stop MacroFlow.
echo.
go run ./cmd/server
echo.
echo MacroFlow stopped.
pause
exit /b 0

:running
echo MacroFlow is already running at %MF_URL%
if "%MF_OPEN%"=="1" start "" "%MF_URL%"
timeout /t 3 >nul
exit /b 0

:nosetup
echo DATABASE_URL and MICROFLOW_MASTER_KEY are not saved yet.
echo Double-click 2-Setup-Environment.bat first, then run this file again.
pause
exit /b 1

:nogo
echo Go is not installed or not on PATH. Install Go 1.22 or newer from https://go.dev/dl/
echo and run this file again.
pause
exit /b 1

:loadvar
if defined %1 exit /b 0
for /f "usebackq delims=" %%A in (`powershell -NoProfile -Command "[Environment]::GetEnvironmentVariable('%1','User')"`) do set "%1=%%A"
exit /b 0
