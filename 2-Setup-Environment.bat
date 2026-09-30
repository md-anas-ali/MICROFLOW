@echo off
setlocal EnableExtensions
title MacroFlow - Setup Environment
cd /d "%~dp0"

echo ============================================================
echo  MacroFlow - one-time environment setup
echo ============================================================
echo.
echo Part 1 saves the two values the server needs before it can even
echo start: DATABASE_URL and MICROFLOW_MASTER_KEY. They are kept in your
echo Windows user environment, so they stay saved.
echo Part 2 saves everything else into MacroFlow's own Global Environment
echo (the same one you see in the MacroFlow UI). It is stored encrypted in
echo the database, so it stays saved there.
echo.

where go >nul 2>&1
if errorlevel 1 goto :nogo

for %%V in (DATABASE_URL MICROFLOW_MASTER_KEY MICROFLOW_LOGIN_USER MICROFLOW_LOGIN_PASSWORD) do call :loadvar %%V

rem ---------------- Part 1 : DATABASE_URL
echo ---- Part 1 of 2 : startup values ----
echo.
if defined DATABASE_URL echo DATABASE_URL is already saved. Press Enter to keep it, or paste a new one.
if not defined DATABASE_URL echo Paste your PostgreSQL DATABASE_URL, for example: postgres://user:pass@host/db?sslmode=require
set /p "DATABASE_URL=DATABASE_URL: "
if not defined DATABASE_URL goto :nodb

rem ---------------- Part 1 : MICROFLOW_MASTER_KEY
echo.
if defined MICROFLOW_MASTER_KEY echo MICROFLOW_MASTER_KEY is already saved. Press Enter to keep it, or paste a different one.
if not defined MICROFLOW_MASTER_KEY echo Paste the SAME MICROFLOW_MASTER_KEY your existing MacroFlow database uses.
if not defined MICROFLOW_MASTER_KEY echo Leave it empty and press Enter only if this is a brand new empty database.
set /p "MICROFLOW_MASTER_KEY=MICROFLOW_MASTER_KEY: "
if defined MICROFLOW_MASTER_KEY goto :savekeys

echo.
echo No key entered. A NEW key can never decrypt data stored with another key.
set "MF_GEN="
set /p "MF_GEN=Generate a NEW key for a fresh database? Type Y to confirm: "
if /I not "%MF_GEN%"=="Y" goto :nokey
for /f "delims=" %%K in ('go run ./cmd/server genkey 2^>^&1') do set "MICROFLOW_MASTER_KEY=%%K"
if not defined MICROFLOW_MASTER_KEY goto :nokey
echo.
echo New key generated. Keep a copy of it somewhere safe:
echo %MICROFLOW_MASTER_KEY%

:savekeys
powershell -NoProfile -Command "foreach($n in 'DATABASE_URL','MICROFLOW_MASTER_KEY'){ $v=[Environment]::GetEnvironmentVariable($n,'Process'); if($v){[Environment]::SetEnvironmentVariable($n,$v,'User')} }"
echo.
echo Startup values saved.

rem ---------------- Part 2 : Global Environment through the running server
echo.
echo ---- Part 2 of 2 : Global Environment ----
echo.

rem Same local defaults as 1-Start-MacroFlow.bat
if not defined MICROFLOW_ADDR set "MICROFLOW_ADDR=127.0.0.1:8080"
for /f "usebackq delims=" %%U in (`powershell -NoProfile -Command "$a=$env:MICROFLOW_ADDR; $h=($a -split ':')[0]; $p=($a -split ':')[-1]; if(-not $h -or $h -eq '0.0.0.0'){$h='127.0.0.1'}; 'http://'+$h+':'+$p"`) do set "MF_URL=%%U"

call :isup
if not errorlevel 1 goto :serverup
echo Starting MacroFlow server in a separate window, please wait...
start "MacroFlow Server" cmd /k ""%~dp01-Start-MacroFlow.bat" /nobrowser"
set /a MF_TRIES=0

:waitloop
call :isup
if not errorlevel 1 goto :serverup
set /a MF_TRIES+=1
if %MF_TRIES% GEQ 120 goto :noserver
timeout /t 2 /nobreak >nul
goto :waitloop

:serverup
echo Server is running at %MF_URL%
echo.
echo Already saved in Global Environment:
call :listenv
echo.
echo Add or update variables now. Examples of names used by MacroFlow:
echo   OPENROUTER_API_KEY  GEMINI_API_KEY  YOUTUBE_DATA_API_KEY  GOOGLE_SHEET_ID
echo   YOUTUBE_CATEGORY_ID  GOOGLE_OAUTH_CLIENT_ID  GOOGLE_OAUTH_CLIENT_SECRET
echo   GOOGLE_OAUTH_REDIRECT_URL  NOTIFY_WEBHOOK_URL  APP_REFERER_URL
echo Entering a name that already exists overwrites its value.
echo Press Enter on an empty name when you are finished.

:envloop
echo.
set "MF_K="
set "MF_V="
set /p "MF_K=Variable name: "
if not defined MF_K goto :done
set /p "MF_V=Value for %MF_K%: "
if not defined MF_V echo Empty value - skipped.
if not defined MF_V goto :envloop
call :putenv
goto :envloop

:done
echo.
echo ============================================================
echo  Setup finished. From now on just double-click
echo  1-Start-MacroFlow.bat to run MacroFlow.
echo ============================================================
pause
exit /b 0

:isup
powershell -NoProfile -Command "try{ if((Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 -Uri ($env:MF_URL+'/healthz')).Content -eq 'ok'){exit 0}else{exit 1} }catch{exit 1}"
exit /b %errorlevel%

:putenv
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; try{ $s=New-Object Microsoft.PowerShell.Commands.WebRequestSession; if($env:MICROFLOW_LOGIN_USER){ Invoke-WebRequest -UseBasicParsing -Method Post -Uri ($env:MF_URL+'/login') -Body @{username=$env:MICROFLOW_LOGIN_USER;password=$env:MICROFLOW_LOGIN_PASSWORD} -WebSession $s | Out-Null }; $b=@{key=$env:MF_K;value=$env:MF_V;isSecret=$true} | ConvertTo-Json; Invoke-RestMethod -Method Post -Uri ($env:MF_URL+'/api/global-env') -WebSession $s -ContentType 'application/json' -Body $b | Out-Null; Write-Host 'Saved.'; exit 0 }catch{ Write-Host ('Failed: '+$_.Exception.Message); exit 1 }"
exit /b 0

:listenv
powershell -NoProfile -Command "$ErrorActionPreference='Stop'; try{ $s=New-Object Microsoft.PowerShell.Commands.WebRequestSession; if($env:MICROFLOW_LOGIN_USER){ Invoke-WebRequest -UseBasicParsing -Method Post -Uri ($env:MF_URL+'/login') -Body @{username=$env:MICROFLOW_LOGIN_USER;password=$env:MICROFLOW_LOGIN_PASSWORD} -WebSession $s | Out-Null }; $r=@(Invoke-RestMethod -Uri ($env:MF_URL+'/api/global-env') -WebSession $s); if($r.Count -gt 0){ Write-Host ('  '+(($r | ForEach-Object { $_.key }) -join ', ')) } else { Write-Host '  (nothing yet)' } }catch{ Write-Host ('  Could not read the list: '+$_.Exception.Message) }"
exit /b 0

:loadvar
if defined %1 exit /b 0
for /f "usebackq delims=" %%A in (`powershell -NoProfile -Command "[Environment]::GetEnvironmentVariable('%1','User')"`) do set "%1=%%A"
exit /b 0

:nogo
echo Go is not installed or not on PATH. Install Go 1.22 or newer from https://go.dev/dl/
echo and run this file again.
pause
exit /b 1

:nodb
echo DATABASE_URL is required. Run this file again and paste it.
pause
exit /b 1

:nokey
echo MICROFLOW_MASTER_KEY is required. Run this file again and paste your existing key.
pause
exit /b 1

:noserver
echo The server did not become ready in time. Check the "MacroFlow Server" window for the error,
echo fix it, then run this file again. Your startup values from Part 1 are already saved.
pause
exit /b 1
