@echo off
setlocal enabledelayedexpansion
title "Steam Metadata & Depot Manifest Historical Tracker"

cd /d "%~dp0"

echo ================================================================
echo    STEAM METADATA ^& DEPOT MANIFEST HISTORICAL TRACKER
echo ================================================================
echo.
echo 1. Test Single/Popular Games (Among Us, CS2, Terraria, Dota 2, RE Requiem)
echo 2. Run Catalog Scan (Resume from Checkpoint, 10 Workers)
echo 3. Run Small Batch (First 50 Apps)
echo 4. Refresh Steam AppID Catalog (~165k-184k apps)
echo 5. View Dataset Statistics
echo 6. Reset Checkpoint
echo 0. Exit
echo.
set /p CHOICE="Choose an option [0-6]: "

if "%CHOICE%"=="1" (
    echo.
    echo Running test for popular games...
    python tracker.py --appids 945360,730,105600,570,3764200
    goto END
)

if "%CHOICE%"=="2" (
    echo.
    echo Starting/Resuming full catalog scan with 10 workers...
    echo Press Ctrl+C at any time to pause and save checkpoint safely.
    python tracker.py --run --concurrency 10
    goto END
)

if "%CHOICE%"=="3" (
    echo.
    echo Scanning 50 apps...
    python tracker.py --limit 50 --concurrency 10
    goto END
)

if "%CHOICE%"=="4" (
    echo.
    echo Refreshing Steam AppID catalog...
    python tracker.py --fetch-applist
    goto END
)

if "%CHOICE%"=="5" (
    echo.
    python tracker.py --stats
    goto END
)

if "%CHOICE%"=="6" (
    echo.
    set /p CONFIRM="Are you sure you want to reset the checkpoint? (y/N): "
    if /i "!CONFIRM!"=="y" (
        python tracker.py --reset-checkpoint
    ) else (
        echo Cancelled.
    )
    goto END
)

:END
echo.
pause
