@echo off

set PRESET=windows-x64

:: 生成
REM cmake --fresh --preset "${PRESET}"
cmake --preset "%PRESET%"
if %errorlevel% NEQ 0 goto error

:: 构建
cmake --build --preset "%PRESET%"
if %errorlevel% NEQ 0 goto error

:: 没有报错，直接结束脚本
goto :EOF

:error
pause
