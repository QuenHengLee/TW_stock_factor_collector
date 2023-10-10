@echo OFF

rem Get Current Username
rem Define here the path to your conda installation
set CONDAPATH=C:\Users\%USERNAME%\anaconda3
rem Define here the name of the environment
rem set ENVNAME=factorAnalysis
set ENVNAME=ccthesis

rem The following command activates the base environment.
if %ENVNAME%==base (set ENVPATH=%CONDAPATH%) else (set ENVPATH=%CONDAPATH%\envs\%ENVNAME%)

rem Activate the conda environment
call %CONDAPATH%\Scripts\activate.bat %ENVPATH%

rem Change to script directory, because when call from VBA, current path is not where the script is
cd /D %~dp0
cd ..
rem Run a python script in that environment
python -m CollectorFactory.TWCollectorFactory.IndicatorFinal

rem Deactivate the environment
call conda deactivate