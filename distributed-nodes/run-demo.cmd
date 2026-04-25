@echo off
setlocal
powershell.exe -NoExit -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-demo.ps1" %*