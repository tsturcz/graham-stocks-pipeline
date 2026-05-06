@echo off
REM ====================================================================
REM Graham Stocks - Daily Scraper Wrapper
REM --------------------------------------------------------------------
REM Triggered by Windows Task Scheduler at 22:00 every day.
REM Runs scraper.py inside the project's virtual environment and
REM appends both stdout and stderr to scheduler.log.
REM ====================================================================

setlocal

REM --- Configuration --------------------------------------------------
set "PROJECT_DIR=D:\Tomi\Graham projekt"
set "PYTHON_EXE=%PROJECT_DIR%\venv\Scripts\python.exe"
set "SCRAPER=%PROJECT_DIR%\scraper.py"
set "LOG_FILE=%PROJECT_DIR%\scheduler.log"

REM --- Move to the project directory so .env is found ----------------
cd /d "%PROJECT_DIR%"

REM --- Header in the log ---------------------------------------------
echo. >> "%LOG_FILE%"
echo ==================================================================== >> "%LOG_FILE%"
echo  Scheduled run started: %date% %time% >> "%LOG_FILE%"
echo ==================================================================== >> "%LOG_FILE%"

REM --- Run the scraper ------------------------------------------------
"%PYTHON_EXE%" "%SCRAPER%" >> "%LOG_FILE%" 2>&1

set EXIT_CODE=%ERRORLEVEL%

REM --- Footer in the log ---------------------------------------------
echo. >> "%LOG_FILE%"
echo  Scheduled run finished: %date% %time%   exit code = %EXIT_CODE% >> "%LOG_FILE%"
echo ==================================================================== >> "%LOG_FILE%"

endlocal
exit /b %EXIT_CODE%
