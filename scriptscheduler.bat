rem set a retry variable initially to 0  
set retry=0  

rem throw the first parameter away
set retries=%1
shift
set sleep=%1
:loop
shift
if [%1]==[] goto afterloop
set params=%params% %1
goto loop
:afterloop

@echo retries = %retries%
@echo sleep = %sleep%
@echo params = %params%

:step1  
set step=1  
@echo (%date%%time%) Step 1 : run script

%params%
  
   
@echo Errorlevel is %ERRORLEVEL%  
if %ERRORLEVEL% NEQ 0 goto retry  
   
rem re-set the retry number if success  
set retry=0  
   
@echo Errorlevel is %ERRORLEVEL%  
rem At the last step, direct the flow to the end of file otherwise it will execute retry block  
if %ERRORLEVEL% EQU 0 goto eof  
if %ERRORLEVEL% NEQ 0 goto retry  
   
:retry  
   
set /a retry=%retry%+1  
@echo (%date% %time%) There was an error at STEP%step%, retrying again!  
if %retry% LSS %retries% (
	TIMEOUT %sleep%
	goto :step%step%
)  
if %retry% EQU %retries% (goto :err)  
   
:err  
@echo (%date% %time%) sorry, this script stopped due to a %retries% unsuccessful retries.
EXIT  
   
:eof  
@echo (%date% %time%) Success! The script completed successfully!
EXIT  